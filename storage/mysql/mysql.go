package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	_ "github.com/go-sql-driver/mysql"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/storage"
	"github.com/pkg/errors"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultMaxTuplesPerWrite             = 100
	defaultMaxTypesPerAuthorizationModel = 100
)

type MySQL struct {
	db                            *sql.DB
	tracer                        trace.Tracer
	logger                        logger.Logger
	maxTuplesPerWrite             int
	maxTypesPerAuthorizationModel int

	maxOpenConns    int
	maxIdleConns    int
	connMaxIdleTime time.Duration
	connMaxLifetime time.Duration
}

type MySQLOption func(*MySQL)

func WithTracer(t trace.Tracer) MySQLOption {
	return func(m *MySQL) {
		m.tracer = t
	}
}

func WithLogger(l logger.Logger) MySQLOption {
	return func(m *MySQL) {
		m.logger = l
	}
}

func WithMaxTuplesPerWrite(maxTuples int) MySQLOption {
	return func(m *MySQL) {
		m.maxTuplesPerWrite = maxTuples
	}
}

func WithMaxTypesPerAuthorizationModel(maxTypes int) MySQLOption {
	return func(m *MySQL) {
		m.maxTypesPerAuthorizationModel = maxTypes
	}
}

func WithMaxOpenConns(c int) MySQLOption {
	return func(m *MySQL) {
		m.maxOpenConns = c
	}
}

func WithMaxIdleConns(c int) MySQLOption {
	return func(m *MySQL) {
		m.maxIdleConns = c
	}
}

func WithConnMaxIdleTime(d time.Duration) MySQLOption {
	return func(m *MySQL) {
		m.connMaxIdleTime = d
	}
}

func WithConnMaxLifetime(d time.Duration) MySQLOption {
	return func(m *MySQL) {
		m.connMaxLifetime = d
	}
}

func NewMySQLDatastore(uri string, opts ...MySQLOption) (*MySQL, error) {
	m := &MySQL{}

	for _, opt := range opts {
		opt(m)
	}

	if m.logger == nil {
		m.logger = logger.NewNoopLogger()
	}

	if m.tracer == nil {
		m.tracer = telemetry.NewNoopTracer()
	}

	if m.maxTuplesPerWrite == 0 {
		m.maxTuplesPerWrite = defaultMaxTuplesPerWrite
	}

	if m.maxTypesPerAuthorizationModel == 0 {
		m.maxTypesPerAuthorizationModel = defaultMaxTypesPerAuthorizationModel
	}

	db, err := sql.Open("mysql", uri)
	if err != nil {
		return nil, errors.Errorf("failed to open MySQL connection: %v", err)
	}

	if m.maxOpenConns != 0 {
		db.SetMaxOpenConns(m.maxOpenConns)
	}

	if m.maxIdleConns != 0 {
		db.SetMaxIdleConns(m.maxIdleConns)
	}

	if m.connMaxIdleTime != 0 {
		db.SetConnMaxIdleTime(m.connMaxIdleTime)
	}

	if m.connMaxLifetime != 0 {
		db.SetConnMaxLifetime(m.connMaxLifetime)
	}

	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = 1 * time.Minute
	attempt := 1
	err = backoff.Retry(func() error {
		err := db.Ping()
		if err != nil {
			m.logger.Info("waiting for MySQL", zap.Int("attempt", attempt))
			attempt++
			return err
		}
		return nil
	}, policy)
	if err != nil {
		return nil, errors.Errorf("failed to initialize MySQL connection: %v", err)
	}

	m.db = db

	return m, nil
}

// Close closes the datastore and cleans up any residual resources.
func (m *MySQL) Close(ctx context.Context) {
	m.db.Close()
}

func (m *MySQL) ListObjectsByType(ctx context.Context, store string, objectType string) (storage.ObjectIterator, error) {
	ctx, span := m.tracer.Start(ctx, "mysql.ListObjectsByType")
	defer span.End()

	stmt := "SELECT DISTINCT object_type, object_id FROM tuple WHERE store = ? AND object_type = ?"
	rows, err := m.db.QueryContext(ctx, stmt, store, objectType)
	if err != nil {
		return nil, err
	}

	return NewMysqlObjectIterator(rows), nil
}

func (m *MySQL) Read(ctx context.Context, store string, tupleKey *openfgapb.TupleKey) (storage.TupleIterator, error) {
	ctx, span := m.tracer.Start(ctx, "mysql.Read")
	defer span.End()

	return m.read(ctx, store, tupleKey, storage.PaginationOptions{})
}

func (m *MySQL) ReadPage(ctx context.Context, store string, tupleKey *openfgapb.TupleKey, opts storage.PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	ctx, span := m.tracer.Start(ctx, "mysql.ReadPage")
	defer span.End()

	iter, err := m.read(ctx, store, tupleKey, opts)
	if err != nil {
		return nil, nil, err
	}
	defer iter.Stop()

	return iter.toArray(ctx, opts)
}

func (m *MySQL) read(ctx context.Context, store string, tupleKey *openfgapb.TupleKey, opts storage.PaginationOptions) (*tupleIterator, error) {
	ctx, span := m.tracer.Start(ctx, "mysql.read")
	defer span.End()

	stmt, args, err := buildReadQuery(store, tupleKey, opts)
	if err != nil {
		return nil, err
	}

	rows, err := m.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, handleMySQLError(err)
	}

	return NewMysqlTupleIterator(rows), nil
}

func (m *MySQL) Write(ctx context.Context, store string, deletes storage.Deletes, writes storage.Writes) error {
	ctx, span := m.tracer.Start(ctx, "mysql.Write")
	defer span.End()

	if len(deletes)+len(writes) > m.MaxTuplesPerWrite() {
		return storage.ErrExceededWriteBatchLimit
	}

	now := time.Now().UTC()
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return handleMySQLError(err)
	}
	defer rollbackTx(ctx, tx, m.logger)

	for _, tk := range deletes {
		id := ulid.MustNew(ulid.Timestamp(now), ulid.DefaultEntropy()).String()
		objectType, objectID := tupleUtils.SplitObject(tk.GetObject())
		r, err := tx.ExecContext(ctx, `DELETE FROM tuple WHERE store = ? AND object_type = ? AND object_id = ? AND relation = ? AND _user = ? AND user_type = ?`, store, objectType, objectID, tk.GetRelation(), tk.GetUser(), tupleUtils.GetUserTypeFromUser(tk.GetUser()))
		if err != nil {
			return handleMySQLError(err)
		}

		affectedRows, err := r.RowsAffected()
		if err != nil {
			return handleMySQLError(err)
		}

		if affectedRows != 1 {
			return storage.InvalidWriteInputError(tk, openfgapb.TupleOperation_TUPLE_OPERATION_DELETE)
		}

		_, err = tx.ExecContext(ctx, `INSERT INTO changelog (store, object_type, object_id, relation, _user, operation, ulid, inserted_at) VALUES (?, ?, ?, ?, ?, ?, ?, NOW())`, store, objectType, objectID, tk.GetRelation(), tk.GetUser(), openfgapb.TupleOperation_TUPLE_OPERATION_DELETE, id)
		if err != nil {
			return handleMySQLError(err)
		}
	}

	for _, tk := range writes {
		id := ulid.MustNew(ulid.Timestamp(now), ulid.DefaultEntropy()).String()
		objectType, objectID := tupleUtils.SplitObject(tk.GetObject())
		relation := tk.GetRelation()
		user := tk.GetUser()
		userType := tupleUtils.GetUserTypeFromUser(user)

		_, err = tx.ExecContext(
			ctx,
			`INSERT INTO tuple (store, object_type, object_id, relation, _user, user_type, ulid, inserted_at) VALUES (?, ?, ?, ?, ?, ?, ?, NOW())`,
			store, objectType, objectID, relation, user, userType, id,
		)

		if err != nil {
			return handleMySQLError(err, tk)
		}

		_, err = tx.ExecContext(ctx, `INSERT INTO changelog (store, object_type, object_id, relation, _user, operation, ulid, inserted_at) VALUES (?, ?, ?, ?, ?, ?, ?, NOW())`, store, objectType, objectID, tk.GetRelation(), tk.GetUser(), openfgapb.TupleOperation_TUPLE_OPERATION_WRITE, id)
		if err != nil {
			return handleMySQLError(err, tk)
		}
	}

	if err := tx.Commit(); err != nil {
		return handleMySQLError(err)
	}

	return nil
}

func (m *MySQL) ReadUserTuple(ctx context.Context, store string, tupleKey *openfgapb.TupleKey) (*openfgapb.Tuple, error) {
	ctx, span := m.tracer.Start(ctx, "mysql.ReadUserTuple")
	defer span.End()

	objectType, objectID := tupleUtils.SplitObject(tupleKey.GetObject())
	userType := tupleUtils.GetUserTypeFromUser(tupleKey.GetUser())

	row := m.db.QueryRowContext(ctx, `SELECT object_type, object_id, relation, _user FROM tuple WHERE store = ? AND object_type = ? AND object_id = ? AND relation = ? AND _user = ? AND user_type = ?`,
		store, objectType, objectID, tupleKey.GetRelation(), tupleKey.GetUser(), userType)

	var record tupleRecord
	if err := row.Scan(&record.objectType, &record.objectID, &record.relation, &record.user); err != nil {
		return nil, handleMySQLError(err)
	}

	return record.asTuple(), nil
}

func (m *MySQL) ReadUsersetTuples(ctx context.Context, store string, tupleKey *openfgapb.TupleKey) (storage.TupleIterator, error) {
	ctx, span := m.tracer.Start(ctx, "mysql.ReadUsersetTuples")
	defer span.End()

	stmt, args, err := buildReadUsersetTuplesQuery(store, tupleKey)
	if err != nil {
		return nil, err
	}

	rows, err := m.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, handleMySQLError(err)
	}

	return NewMysqlTupleIterator(rows), nil
}

func (m *MySQL) ReadByStore(ctx context.Context, store string, opts storage.PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	ctx, span := m.tracer.Start(ctx, "mysql.ReadByStore")
	defer span.End()

	iter, err := m.read(ctx, store, nil, opts)
	if err != nil {
		return nil, nil, err
	}
	defer iter.Stop()
	return iter.toArray(ctx, opts)
}

func (m *MySQL) MaxTuplesPerWrite() int {
	return m.maxTuplesPerWrite
}

func (m *MySQL) ReadAuthorizationModel(ctx context.Context, store string, modelID string) (*openfgapb.AuthorizationModel, error) {
	ctx, span := m.tracer.Start(ctx, "mysql.ReadAuthorizationModel")
	defer span.End()

	stmt := "SELECT schema_version, type, type_definition FROM authorization_model WHERE store = ? AND authorization_model_id = ?"
	rows, err := m.db.QueryContext(ctx, stmt, store, modelID)
	if err != nil {
		return nil, handleMySQLError(err)
	}
	defer rows.Close()

	var schemaVersion string
	var typeDefs []*openfgapb.TypeDefinition
	for rows.Next() {
		var typeName string
		var marshalledTypeDef []byte
		err = rows.Scan(&schemaVersion, &typeName, &marshalledTypeDef)
		if err != nil {
			return nil, handleMySQLError(err)
		}

		var typeDef openfgapb.TypeDefinition
		if err := proto.Unmarshal(marshalledTypeDef, &typeDef); err != nil {
			return nil, err
		}

		typeDefs = append(typeDefs, &typeDef)
	}

	if err := rows.Err(); err != nil {
		return nil, handleMySQLError(err)
	}

	if len(typeDefs) == 0 {
		return nil, storage.ErrNotFound
	}

	// Update the schema version lazily if it is not a valid typesystem.SchemaVersion.
	if schemaVersion != typesystem.SchemaVersion1_0 && schemaVersion != typesystem.SchemaVersion1_1 {
		schemaVersion = typesystem.SchemaVersion1_0
		_, err = m.db.ExecContext(ctx, "UPDATE authorization_model SET schema_version = ? WHERE store = ? AND authorization_model_id = ?", schemaVersion, store, modelID)
		if err != nil {
			// Don't worry if we error, we'll update it lazily next time, but let's log:
			m.logger.Warn("failed to lazily update schema version", zap.String("store", store), zap.String("authorization_model_id", modelID))
		}
	}

	return &openfgapb.AuthorizationModel{
		SchemaVersion:   schemaVersion,
		Id:              modelID,
		TypeDefinitions: typeDefs,
	}, nil
}

func (m *MySQL) ReadAuthorizationModels(ctx context.Context, store string, opts storage.PaginationOptions) ([]*openfgapb.AuthorizationModel, []byte, error) {
	ctx, span := m.tracer.Start(ctx, "mysql.ReadAuthorizationModels")
	defer span.End()

	stmt, args, err := buildReadAuthorizationModelsQuery(store, opts)
	if err != nil {
		return nil, nil, err
	}

	rows, err := m.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, nil, handleMySQLError(err)
	}
	defer rows.Close()

	var modelIDs []string
	var modelID string

	for rows.Next() {
		err := rows.Scan(&modelID)
		if err != nil {
			return nil, nil, handleMySQLError(err)
		}

		modelIDs = append(modelIDs, modelID)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, handleMySQLError(err)
	}

	var token []byte
	numModelIDs := len(modelIDs)
	if len(modelIDs) > opts.PageSize {
		numModelIDs = opts.PageSize
		token, err = json.Marshal(newContToken(modelID, ""))
		if err != nil {
			return nil, nil, err
		}
	}

	// TODO: make this concurrent with a maximum of 5 goroutines. This may be helpful:
	// https://stackoverflow.com/questions/25306073/always-have-x-number-of-goroutines-running-at-any-time
	models := make([]*openfgapb.AuthorizationModel, 0, numModelIDs)
	// We use numModelIDs here to avoid retrieving possibly one extra model.
	for i := 0; i < numModelIDs; i++ {
		model, err := m.ReadAuthorizationModel(ctx, store, modelIDs[i])
		if err != nil {
			return nil, nil, err
		}
		models = append(models, model)
	}

	return models, token, nil
}

func (m *MySQL) FindLatestAuthorizationModelID(ctx context.Context, store string) (string, error) {
	ctx, span := m.tracer.Start(ctx, "mysql.FindLatestAuthorizationModelID")
	defer span.End()

	var modelID string
	stmt := "SELECT authorization_model_id FROM authorization_model WHERE store = ? ORDER BY authorization_model_id DESC LIMIT 1"
	err := m.db.QueryRowContext(ctx, stmt, store).Scan(&modelID)
	if err != nil {
		return "", handleMySQLError(err)
	}

	return modelID, nil
}

func (m *MySQL) ReadTypeDefinition(
	ctx context.Context,
	store, modelID, objectType string,
) (*openfgapb.TypeDefinition, error) {
	ctx, span := m.tracer.Start(ctx, "mysql.ReadTypeDefinition")
	defer span.End()

	var marshalledTypeDef []byte
	stmt := "SELECT type_definition FROM authorization_model WHERE store = ? AND authorization_model_id = ? AND type = ?"
	err := m.db.QueryRowContext(ctx, stmt, store, modelID, objectType).Scan(&marshalledTypeDef)
	if err != nil {
		return nil, handleMySQLError(err)
	}

	var typeDef openfgapb.TypeDefinition
	if err := proto.Unmarshal(marshalledTypeDef, &typeDef); err != nil {
		return nil, err
	}

	return &typeDef, nil
}

func (m *MySQL) MaxTypesPerAuthorizationModel() int {
	return m.maxTypesPerAuthorizationModel
}

func (m *MySQL) WriteAuthorizationModel(ctx context.Context, store string, model *openfgapb.AuthorizationModel) error {
	ctx, span := m.tracer.Start(ctx, "mysql.WriteAuthorizationModel")
	defer span.End()

	schemaVersion := model.GetSchemaVersion()
	typeDefinitions := model.GetTypeDefinitions()

	if len(typeDefinitions) > m.MaxTypesPerAuthorizationModel() {
		return storage.ExceededMaxTypeDefinitionsLimitError(m.maxTypesPerAuthorizationModel)
	}

	stmt := "INSERT INTO authorization_model (store, authorization_model_id, schema_version, type, type_definition) VALUES (?, ?, ?, ?, ?)"

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return handleMySQLError(err)
	}
	defer rollbackTx(ctx, tx, m.logger)

	for _, typeDef := range typeDefinitions {
		marshalledTypeDef, err := proto.Marshal(typeDef)
		if err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, stmt, store, model.Id, schemaVersion, typeDef.GetType(), marshalledTypeDef)
		if err != nil {
			return handleMySQLError(err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return handleMySQLError(err)
	}

	return nil
}

func (m *MySQL) CreateStore(ctx context.Context, store *openfgapb.Store) (*openfgapb.Store, error) {
	ctx, span := m.tracer.Start(ctx, "mysql.CreateStore")
	defer span.End()

	tx, err := m.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, handleMySQLError(err)
	}

	defer func() {
		_ = tx.Rollback()
	}()

	stmt := "INSERT INTO store (id, name, created_at, updated_at) VALUES (?, ?, NOW(), NOW())"
	_, err = tx.ExecContext(ctx, stmt, store.Id, store.Name)
	if err != nil {
		return nil, handleMySQLError(err)
	}

	var createdAt time.Time
	var id, name string
	stmt = "SELECT id, name, created_at FROM store WHERE id = ?"
	err = tx.QueryRowContext(ctx, stmt, store.Id).Scan(&id, &name, &createdAt)
	if err != nil {
		return nil, handleMySQLError(err)
	}

	err = tx.Commit()
	if err != nil {
		return nil, handleMySQLError(err)
	}

	return &openfgapb.Store{
		Id:        id,
		Name:      name,
		CreatedAt: timestamppb.New(createdAt),
		UpdatedAt: timestamppb.New(createdAt),
	}, nil
}

func (m *MySQL) GetStore(ctx context.Context, id string) (*openfgapb.Store, error) {
	ctx, span := m.tracer.Start(ctx, "mysql.GetStore")
	defer span.End()

	row := m.db.QueryRowContext(ctx, "SELECT id, name, created_at, updated_at FROM store WHERE id = ? AND deleted_at IS NULL", id)

	var storeID, name string
	var createdAt, updatedAt time.Time
	err := row.Scan(&storeID, &name, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, storage.ErrNotFound
		}
		return nil, handleMySQLError(err)
	}

	return &openfgapb.Store{
		Id:        storeID,
		Name:      name,
		CreatedAt: timestamppb.New(createdAt),
		UpdatedAt: timestamppb.New(updatedAt),
	}, nil
}

func (m *MySQL) ListStores(ctx context.Context, opts storage.PaginationOptions) ([]*openfgapb.Store, []byte, error) {
	ctx, span := m.tracer.Start(ctx, "mysql.ListStores")
	defer span.End()

	stmt, args, err := buildListStoresQuery(opts)
	if err != nil {
		return nil, nil, err
	}

	rows, err := m.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, nil, handleMySQLError(err)
	}
	defer rows.Close()

	var stores []*openfgapb.Store
	var id string
	for rows.Next() {
		var name string
		var createdAt, updatedAt time.Time
		err := rows.Scan(&id, &name, &createdAt, &updatedAt)
		if err != nil {
			return nil, nil, handleMySQLError(err)
		}

		stores = append(stores, &openfgapb.Store{
			Id:        id,
			Name:      name,
			CreatedAt: timestamppb.New(createdAt),
			UpdatedAt: timestamppb.New(updatedAt),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, nil, handleMySQLError(err)
	}

	if len(stores) > opts.PageSize {
		contToken, err := json.Marshal(newContToken(id, ""))
		if err != nil {
			return nil, nil, err
		}
		return stores[:opts.PageSize], contToken, nil
	}

	return stores, nil, nil
}

func (m *MySQL) DeleteStore(ctx context.Context, id string) error {
	ctx, span := m.tracer.Start(ctx, "mysql.DeleteStore")
	defer span.End()

	_, err := m.db.ExecContext(ctx, "UPDATE store SET deleted_at = NOW() WHERE id = ?", id)
	if err != nil {
		return handleMySQLError(err)
	}

	return nil
}

func (m *MySQL) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgapb.Assertion) error {
	ctx, span := m.tracer.Start(ctx, "mysql.WriteAssertions")
	defer span.End()

	marshalledAssertions, err := proto.Marshal(&openfgapb.Assertions{Assertions: assertions})
	if err != nil {
		return err
	}

	stmt := "INSERT INTO assertion (store, authorization_model_id, assertions) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE assertions = ?"
	_, err = m.db.ExecContext(ctx, stmt, store, modelID, marshalledAssertions, marshalledAssertions)
	if err != nil {
		return handleMySQLError(err)
	}

	return nil
}

func (m *MySQL) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgapb.Assertion, error) {
	ctx, span := m.tracer.Start(ctx, "mysql.ReadAssertions")
	defer span.End()

	var marshalledAssertions []byte
	err := m.db.QueryRowContext(ctx, `SELECT assertions FROM assertion WHERE store = ? AND authorization_model_id = ?`, store, modelID).Scan(&marshalledAssertions)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []*openfgapb.Assertion{}, nil
		}
		return nil, handleMySQLError(err)
	}

	var assertions openfgapb.Assertions
	err = proto.Unmarshal(marshalledAssertions, &assertions)
	if err != nil {
		return nil, err
	}

	return assertions.Assertions, nil
}

func (m *MySQL) ReadChanges(
	ctx context.Context,
	store, objectTypeFilter string,
	opts storage.PaginationOptions,
	horizonOffset time.Duration,
) ([]*openfgapb.TupleChange, []byte, error) {
	ctx, span := m.tracer.Start(ctx, "mysql.ReadChanges")
	defer span.End()

	stmt, args, err := buildReadChangesQuery(store, objectTypeFilter, opts, horizonOffset)
	if err != nil {
		return nil, nil, err
	}

	rows, err := m.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, nil, handleMySQLError(err)
	}
	defer rows.Close()

	var changes []*openfgapb.TupleChange
	var ulid string
	for rows.Next() {
		var objectType, objectID, relation, user string
		var operation int
		var insertedAt time.Time

		err = rows.Scan(&ulid, &objectType, &objectID, &relation, &user, &operation, &insertedAt)
		if err != nil {
			return nil, nil, handleMySQLError(err)
		}

		changes = append(changes, &openfgapb.TupleChange{
			TupleKey: &openfgapb.TupleKey{
				Object:   tupleUtils.BuildObject(objectType, objectID),
				Relation: relation,
				User:     user,
			},
			Operation: openfgapb.TupleOperation(operation),
			Timestamp: timestamppb.New(insertedAt.UTC()),
		})
	}

	if len(changes) == 0 {
		return nil, nil, storage.ErrNotFound
	}

	contToken, err := json.Marshal(newContToken(ulid, objectTypeFilter))
	if err != nil {
		return nil, nil, err
	}

	return changes, contToken, nil
}

func (m *MySQL) ReadStartingWithUser(ctx context.Context, store string, opts storage.ReadStartingWithUserFilter) (storage.TupleIterator, error) {
	ctx, span := m.tracer.Start(ctx, "mysql.ReadStartingWithUser")
	defer span.End()

	questionMarksArray := make([]string, 0)
	for range opts.UserFilter {
		questionMarksArray = append(questionMarksArray, "?")
	}

	stmt := "SELECT store, object_type, object_id, relation, _user, ulid, inserted_at FROM tuple WHERE store = ? AND object_type = ? AND relation = ? AND _user IN (" + strings.Join(questionMarksArray, ", ") + ")"

	queryArgs := []any{
		store,
		opts.ObjectType,
		opts.Relation,
	}

	for _, u := range opts.UserFilter {
		targetUser := u.GetObject()
		if u.GetRelation() != "" {
			targetUser = strings.Join([]string{u.GetObject(), u.GetRelation()}, "#")
		}
		queryArgs = append(queryArgs, targetUser)
	}

	rows, err := m.db.QueryContext(ctx, stmt, queryArgs...)
	if err != nil {
		return nil, handleMySQLError(err)
	}

	return NewMysqlTupleIterator(rows), nil
}

// IsReady reports whether this MySQL datastore instance is ready
// to accept connections.
func (m *MySQL) IsReady(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := m.db.PingContext(ctx); err != nil {
		return false, err
	}

	return true, nil
}
