package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"

	sq "github.com/Masterminds/squirrel"
	"github.com/cenkalti/backoff/v4"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var tracer = otel.Tracer("openfga/pkg/storage/mysql")

type MySQL struct {
	stbl                   sq.StatementBuilderType
	db                     *sql.DB
	logger                 logger.Logger
	maxTuplesPerWriteField int
	maxTypesPerModelField  int
}

var _ storage.OpenFGADatastore = (*MySQL)(nil)

func New(uri string, cfg *sqlcommon.Config) (*MySQL, error) {

	if cfg.Username != "" || cfg.Password != "" {
		dsnCfg, err := mysql.ParseDSN(uri)
		if err != nil {
			return nil, fmt.Errorf("failed to parse mysql connection dsn: %w", err)
		}

		if cfg.Username != "" {
			dsnCfg.User = cfg.Username
		}
		if cfg.Password != "" {
			dsnCfg.Passwd = cfg.Password
		}

		uri = dsnCfg.FormatDSN()
	}

	db, err := sql.Open("mysql", uri)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize mysql connection: %w", err)
	}

	if cfg.MaxOpenConns != 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}

	if cfg.MaxIdleConns != 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}

	if cfg.ConnMaxIdleTime != 0 {
		db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	}

	if cfg.ConnMaxLifetime != 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}

	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = 1 * time.Minute
	attempt := 1
	err = backoff.Retry(func() error {
		err = db.PingContext(context.Background())
		if err != nil {
			cfg.Logger.Info("waiting for mysql", zap.Int("attempt", attempt))
			attempt++
			return err
		}
		return nil
	}, policy)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize mysql connection: %w", err)
	}

	return &MySQL{
		stbl:                   sq.StatementBuilder.RunWith(db),
		db:                     db,
		logger:                 cfg.Logger,
		maxTuplesPerWriteField: cfg.MaxTuplesPerWriteField,
		maxTypesPerModelField:  cfg.MaxTypesPerModelField,
	}, nil
}

// Close closes the datastore and cleans up any residual resources.
func (m *MySQL) Close() {
	m.db.Close()
}

func (m *MySQL) ListObjectsByType(ctx context.Context, store string, objectType string) (storage.ObjectIterator, error) {
	ctx, span := tracer.Start(ctx, "mysql.ListObjectsByType")
	defer span.End()

	rows, err := m.stbl.
		Select("object_type", "object_id").
		Distinct().
		From("tuple").
		Where(sq.Eq{
			"store":       store,
			"object_type": objectType,
		}).
		QueryContext(ctx)
	if err != nil {
		return nil, sqlcommon.HandleSQLError(err)
	}

	return sqlcommon.NewSQLObjectIterator(rows), nil
}

func (m *MySQL) Read(ctx context.Context, store string, tupleKey *openfgapb.TupleKey) (storage.TupleIterator, error) {
	ctx, span := tracer.Start(ctx, "mysql.Read")
	defer span.End()

	return m.read(ctx, store, tupleKey, nil)
}

func (m *MySQL) ReadPage(ctx context.Context, store string, tupleKey *openfgapb.TupleKey, opts storage.PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	ctx, span := tracer.Start(ctx, "mysql.ReadPage")
	defer span.End()

	iter, err := m.read(ctx, store, tupleKey, &opts)
	if err != nil {
		return nil, nil, err
	}
	defer iter.Stop()

	return iter.ToArray(opts)
}

func (m *MySQL) read(ctx context.Context, store string, tupleKey *openfgapb.TupleKey, opts *storage.PaginationOptions) (*sqlcommon.SQLTupleIterator, error) {
	ctx, span := tracer.Start(ctx, "mysql.read")
	defer span.End()

	sb := m.stbl.
		Select("store", "object_type", "object_id", "relation", "_user", "user_object_type", "user_object_id", "user_relation", "ulid", "inserted_at").
		From("tuple").
		Where(sq.Eq{"store": store})
	if opts != nil {
		sb = sb.OrderBy("ulid")
	}
	objectType, objectID := tupleUtils.SplitObject(tupleKey.GetObject())
	if objectType != "" {
		sb = sb.Where(sq.Eq{"object_type": objectType})
	}
	if objectID != "" {
		sb = sb.Where(sq.Eq{"object_id": objectID})
	}
	if tupleKey.GetRelation() != "" {
		sb = sb.Where(sq.Eq{"relation": tupleKey.GetRelation()})
	}
	if tupleKey.GetUser() != "" {
		userObjectType, userObjectID, userRelation := tupleUtils.ToUserParts(tupleKey.GetUser())
		sb = sb.Where(sq.Eq{"_user": tupleKey.GetUser()})
		sb = sb.Where(sq.Eq{"user_object_type": userObjectType})
		sb = sb.Where(sq.Eq{"user_object_id": userObjectID})
		sb = sb.Where(sq.Eq{"user_relation": userRelation})
	}
	if opts != nil && opts.From != "" {
		token, err := sqlcommon.UnmarshallContToken(opts.From)
		if err != nil {
			return nil, err
		}
		sb = sb.Where(sq.GtOrEq{"ulid": token.Ulid})
	}
	if opts != nil && opts.PageSize != 0 {
		sb = sb.Limit(uint64(opts.PageSize + 1)) // + 1 is used to determine whether to return a continuation token.
	}

	rows, err := sb.QueryContext(ctx)
	if err != nil {
		return nil, sqlcommon.HandleSQLError(err)
	}

	return sqlcommon.NewSQLTupleIterator(rows), nil
}

func (m *MySQL) Write(ctx context.Context, store string, deletes storage.Deletes, writes storage.Writes) error {
	ctx, span := tracer.Start(ctx, "mysql.Write")
	defer span.End()

	if len(deletes)+len(writes) > m.MaxTuplesPerWrite() {
		return storage.ErrExceededWriteBatchLimit
	}

	now := time.Now().UTC()

	return sqlcommon.Write(ctx, sqlcommon.NewDBInfo(m.db, m.stbl, sq.Expr("NOW()")), store, deletes, writes, now)
}

func (m *MySQL) ReadUserTuple(ctx context.Context, store string, tupleKey *openfgapb.TupleKey) (*openfgapb.Tuple, error) {
	ctx, span := tracer.Start(ctx, "mysql.ReadUserTuple")
	defer span.End()

	objectType, objectID := tupleUtils.SplitObject(tupleKey.GetObject())
	userType := tupleUtils.GetUserTypeFromUser(tupleKey.GetUser())
	userObjectType, userObjectID, userRelation := tupleUtils.ToUserParts(tupleKey.GetUser())

	var record sqlcommon.TupleRecord
	err := m.stbl.
		Select("object_type", "object_id", "relation", "_user", "user_object_type", "user_object_id", "user_relation").
		From("tuple").
		Where(sq.Eq{
			"store":            store,
			"object_type":      objectType,
			"object_id":        objectID,
			"relation":         tupleKey.GetRelation(),
			"_user":            tupleKey.GetUser(),
			"user_type":        userType,
			"user_object_type": userObjectType,
			"user_object_id":   userObjectID,
			"user_relation":    userRelation,
		}).
		QueryRowContext(ctx).
		Scan(&record.ObjectType, &record.ObjectID, &record.Relation, &record.User, &record.UserObjectType, &record.UserObjectID, &record.UserRelation)
	if err != nil {
		return nil, sqlcommon.HandleSQLError(err)
	}

	return record.AsTuple(), nil
}

func (m *MySQL) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter) (storage.TupleIterator, error) {
	ctx, span := tracer.Start(ctx, "mysql.ReadUsersetTuples")
	defer span.End()

	sb := m.stbl.Select("store", "object_type", "object_id", "relation", "_user", "user_object_type", "user_object_id", "user_relation", "ulid", "inserted_at").
		From("tuple").
		Where(sq.Eq{"store": store}).
		Where(sq.Eq{"user_type": tupleUtils.UserSet})

	objectType, objectID := tupleUtils.SplitObject(filter.Object)
	if objectType != "" {
		sb = sb.Where(sq.Eq{"object_type": objectType})
	}
	if objectID != "" {
		sb = sb.Where(sq.Eq{"object_id": objectID})
	}
	if filter.Relation != "" {
		sb = sb.Where(sq.Eq{"relation": filter.Relation})
	}
	if len(filter.AllowedUserTypeRestrictions) > 0 {
		orConditions := sq.Or{}
		for _, userset := range filter.AllowedUserTypeRestrictions {
			if _, ok := userset.RelationOrWildcard.(*openfgapb.RelationReference_Relation); ok {
				orConditions = append(orConditions, sq.And{sq.Eq{"user_object_type": userset.GetType()}, sq.Eq{"user_relation": userset.GetRelation()}})
			}
			if _, ok := userset.RelationOrWildcard.(*openfgapb.RelationReference_Wildcard); ok {
				orConditions = append(orConditions, sq.And{sq.Eq{"user_object_type": userset.GetType()}, sq.Eq{"user_object_id": "*"}})
			}
		}
		sb = sb.Where(orConditions)
	}
	rows, err := sb.QueryContext(ctx)
	if err != nil {
		return nil, sqlcommon.HandleSQLError(err)
	}

	return sqlcommon.NewSQLTupleIterator(rows), nil
}

func (m *MySQL) ReadStartingWithUser(ctx context.Context, store string, opts storage.ReadStartingWithUserFilter) (storage.TupleIterator, error) {
	ctx, span := tracer.Start(ctx, "mysql.ReadStartingWithUser")
	defer span.End()

	iterators := make([]storage.TupleIterator, 0, len(opts.UserFilter))
	for _, u := range opts.UserFilter {
		userObjectType, userObjectID, userRelation := tupleUtils.ToUserPartsFromObjectRelation(u)

		rows, err := m.stbl.
			Select("store", "object_type", "object_id", "relation", "_user", "user_object_type", "user_object_id", "user_relation", "ulid", "inserted_at").
			From("tuple").
			Where(sq.Eq{
				"store":            store,
				"object_type":      opts.ObjectType,
				"relation":         opts.Relation,
				"user_object_type": userObjectType,
				"user_object_id":   userObjectID,
				"user_relation":    userRelation,
			}).QueryContext(ctx)
		if err != nil {
			return nil, sqlcommon.HandleSQLError(err)
		}

		iterators = append(iterators, sqlcommon.NewSQLTupleIterator(rows))
	}

	return storage.NewCombinedIterator(iterators...), nil
}

func (m *MySQL) MaxTuplesPerWrite() int {
	return m.maxTuplesPerWriteField
}

func (m *MySQL) ReadAuthorizationModel(ctx context.Context, store string, modelID string) (*openfgapb.AuthorizationModel, error) {
	ctx, span := tracer.Start(ctx, "mysql.ReadAuthorizationModel")
	defer span.End()

	rows, err := m.stbl.
		Select("schema_version", "type", "type_definition").
		From("authorization_model").
		Where(sq.Eq{
			"store":                  store,
			"authorization_model_id": modelID,
		}).QueryContext(ctx)
	if err != nil {
		return nil, sqlcommon.HandleSQLError(err)
	}
	defer rows.Close()

	var schemaVersion string
	var typeDefs []*openfgapb.TypeDefinition
	for rows.Next() {
		var typeName string
		var marshalledTypeDef []byte
		err = rows.Scan(&schemaVersion, &typeName, &marshalledTypeDef)
		if err != nil {
			return nil, sqlcommon.HandleSQLError(err)
		}

		var typeDef openfgapb.TypeDefinition
		if err := proto.Unmarshal(marshalledTypeDef, &typeDef); err != nil {
			return nil, err
		}

		typeDefs = append(typeDefs, &typeDef)
	}

	if err := rows.Err(); err != nil {
		return nil, sqlcommon.HandleSQLError(err)
	}

	if len(typeDefs) == 0 {
		return nil, storage.ErrNotFound
	}

	// Update the schema version lazily if it is not a valid typesystem.SchemaVersion.
	if schemaVersion != typesystem.SchemaVersion1_0 && schemaVersion != typesystem.SchemaVersion1_1 {
		schemaVersion = typesystem.SchemaVersion1_0
		_, err = m.stbl.
			Update("authorization_model").
			Set("schema_version", schemaVersion).
			Where(sq.Eq{"store": store, "authorization_model_id": modelID}).
			ExecContext(ctx)
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
	ctx, span := tracer.Start(ctx, "mysql.ReadAuthorizationModels")
	defer span.End()

	sb := m.stbl.Select("authorization_model_id").
		Distinct().
		From("authorization_model").
		Where(sq.Eq{"store": store}).
		OrderBy("authorization_model_id desc")

	if opts.From != "" {
		token, err := sqlcommon.UnmarshallContToken(opts.From)
		if err != nil {
			return nil, nil, err
		}
		sb = sb.Where(sq.LtOrEq{"authorization_model_id": token.Ulid})
	}
	if opts.PageSize > 0 {
		sb = sb.Limit(uint64(opts.PageSize + 1)) // + 1 is used to determine whether to return a continuation token.
	}

	rows, err := sb.QueryContext(ctx)
	if err != nil {
		return nil, nil, sqlcommon.HandleSQLError(err)
	}
	defer rows.Close()

	var modelIDs []string
	var modelID string

	for rows.Next() {
		err = rows.Scan(&modelID)
		if err != nil {
			return nil, nil, sqlcommon.HandleSQLError(err)
		}

		modelIDs = append(modelIDs, modelID)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, sqlcommon.HandleSQLError(err)
	}

	var token []byte
	numModelIDs := len(modelIDs)
	if len(modelIDs) > opts.PageSize {
		numModelIDs = opts.PageSize
		token, err = json.Marshal(sqlcommon.NewContToken(modelID, ""))
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
	ctx, span := tracer.Start(ctx, "mysql.FindLatestAuthorizationModelID")
	defer span.End()

	var modelID string
	err := m.stbl.
		Select("authorization_model_id").
		From("authorization_model").
		Where(sq.Eq{"store": store}).
		OrderBy("authorization_model_id desc").
		Limit(1).
		QueryRowContext(ctx).
		Scan(&modelID)
	if err != nil {
		return "", sqlcommon.HandleSQLError(err)
	}

	return modelID, nil
}

func (m *MySQL) MaxTypesPerAuthorizationModel() int {
	return m.maxTypesPerModelField
}

func (m *MySQL) WriteAuthorizationModel(ctx context.Context, store string, model *openfgapb.AuthorizationModel) error {
	ctx, span := tracer.Start(ctx, "mysql.WriteAuthorizationModel")
	defer span.End()

	schemaVersion := model.GetSchemaVersion()
	typeDefinitions := model.GetTypeDefinitions()

	if len(typeDefinitions) > m.MaxTypesPerAuthorizationModel() {
		return storage.ExceededMaxTypeDefinitionsLimitError(m.maxTypesPerModelField)
	}

	if len(typeDefinitions) < 1 {
		return nil
	}

	sb := m.stbl.
		Insert("authorization_model").
		Columns("store", "authorization_model_id", "schema_version", "type", "type_definition")

	for _, td := range typeDefinitions {
		marshalledTypeDef, err := proto.Marshal(td)
		if err != nil {
			return err
		}

		sb = sb.Values(store, model.Id, schemaVersion, td.GetType(), marshalledTypeDef)
	}

	_, err := sb.ExecContext(ctx)
	if err != nil {
		return sqlcommon.HandleSQLError(err)
	}

	return nil
}

// CreateStore is slightly different between Postgres and MySQL
func (m *MySQL) CreateStore(ctx context.Context, store *openfgapb.Store) (*openfgapb.Store, error) {
	ctx, span := tracer.Start(ctx, "mysql.CreateStore")
	defer span.End()

	txn, err := m.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, sqlcommon.HandleSQLError(err)
	}
	defer func() {
		_ = txn.Rollback()
	}()

	_, err = m.stbl.
		Insert("store").
		Columns("id", "name", "created_at", "updated_at").
		Values(store.Id, store.Name, sq.Expr("NOW()"), sq.Expr("NOW()")).
		RunWith(txn).
		ExecContext(ctx)
	if err != nil {
		return nil, sqlcommon.HandleSQLError(err)
	}

	var createdAt time.Time
	var id, name string
	err = m.stbl.
		Select("id", "name", "created_at").
		From("store").
		Where(sq.Eq{"id": store.Id}).
		RunWith(txn).
		QueryRowContext(ctx).
		Scan(&id, &name, &createdAt)
	if err != nil {
		return nil, sqlcommon.HandleSQLError(err)
	}

	err = txn.Commit()
	if err != nil {
		return nil, sqlcommon.HandleSQLError(err)
	}

	return &openfgapb.Store{
		Id:        id,
		Name:      name,
		CreatedAt: timestamppb.New(createdAt),
		UpdatedAt: timestamppb.New(createdAt),
	}, nil
}

func (m *MySQL) GetStore(ctx context.Context, id string) (*openfgapb.Store, error) {
	ctx, span := tracer.Start(ctx, "mysql.GetStore")
	defer span.End()

	row := m.stbl.
		Select("id", "name", "created_at", "updated_at").
		From("store").
		Where(sq.Eq{
			"id":         id,
			"deleted_at": nil,
		}).
		QueryRowContext(ctx)

	var storeID, name string
	var createdAt, updatedAt time.Time
	err := row.Scan(&storeID, &name, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, storage.ErrNotFound
		}
		return nil, sqlcommon.HandleSQLError(err)
	}

	return &openfgapb.Store{
		Id:        storeID,
		Name:      name,
		CreatedAt: timestamppb.New(createdAt),
		UpdatedAt: timestamppb.New(updatedAt),
	}, nil
}

func (m *MySQL) ListStores(ctx context.Context, opts storage.PaginationOptions) ([]*openfgapb.Store, []byte, error) {
	ctx, span := tracer.Start(ctx, "mysql.ListStores")
	defer span.End()

	sb := m.stbl.Select("id", "name", "created_at", "updated_at").
		From("store").
		Where(sq.Eq{"deleted_at": nil}).
		OrderBy("id")

	if opts.From != "" {
		token, err := sqlcommon.UnmarshallContToken(opts.From)
		if err != nil {
			return nil, nil, err
		}
		sb = sb.Where(sq.GtOrEq{"id": token.Ulid})
	}
	if opts.PageSize > 0 {
		sb = sb.Limit(uint64(opts.PageSize + 1)) // + 1 is used to determine whether to return a continuation token.
	}

	rows, err := sb.QueryContext(ctx)
	if err != nil {
		return nil, nil, sqlcommon.HandleSQLError(err)
	}
	defer rows.Close()

	var stores []*openfgapb.Store
	var id string
	for rows.Next() {
		var name string
		var createdAt, updatedAt time.Time
		err := rows.Scan(&id, &name, &createdAt, &updatedAt)
		if err != nil {
			return nil, nil, sqlcommon.HandleSQLError(err)
		}

		stores = append(stores, &openfgapb.Store{
			Id:        id,
			Name:      name,
			CreatedAt: timestamppb.New(createdAt),
			UpdatedAt: timestamppb.New(updatedAt),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, nil, sqlcommon.HandleSQLError(err)
	}

	if len(stores) > opts.PageSize {
		contToken, err := json.Marshal(sqlcommon.NewContToken(id, ""))
		if err != nil {
			return nil, nil, err
		}
		return stores[:opts.PageSize], contToken, nil
	}

	return stores, nil, nil
}

func (m *MySQL) DeleteStore(ctx context.Context, id string) error {
	ctx, span := tracer.Start(ctx, "mysql.DeleteStore")
	defer span.End()

	_, err := m.stbl.
		Update("store").
		Set("deleted_at", sq.Expr("NOW()")).
		Where(sq.Eq{"id": id}).
		ExecContext(ctx)
	if err != nil {
		return sqlcommon.HandleSQLError(err)
	}

	return nil
}

// WriteAssertions is slightly different between Postgres and MySQL
func (m *MySQL) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgapb.Assertion) error {
	ctx, span := tracer.Start(ctx, "mysql.WriteAssertions")
	defer span.End()

	marshalledAssertions, err := proto.Marshal(&openfgapb.Assertions{Assertions: assertions})
	if err != nil {
		return err
	}

	_, err = m.stbl.
		Insert("assertion").
		Columns("store", "authorization_model_id", "assertions").
		Values(store, modelID, marshalledAssertions).
		Suffix("ON DUPLICATE KEY UPDATE assertions = ?", marshalledAssertions).
		ExecContext(ctx)
	if err != nil {
		return sqlcommon.HandleSQLError(err)
	}

	return nil
}

func (m *MySQL) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgapb.Assertion, error) {
	ctx, span := tracer.Start(ctx, "mysql.ReadAssertions")
	defer span.End()

	var marshalledAssertions []byte
	err := m.stbl.
		Select("assertions").
		From("assertion").
		Where(sq.Eq{
			"store":                  store,
			"authorization_model_id": modelID,
		}).
		QueryRowContext(ctx).
		Scan(&marshalledAssertions)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []*openfgapb.Assertion{}, nil
		}
		return nil, sqlcommon.HandleSQLError(err)
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
	ctx, span := tracer.Start(ctx, "mysql.ReadChanges")
	defer span.End()

	sb := m.stbl.Select("ulid", "object_type", "object_id", "relation", "user_object_type", "user_object_id", "user_relation", "operation", "inserted_at").
		From("changelog").
		Where(sq.Eq{"store": store}).
		Where(fmt.Sprintf("inserted_at <= NOW() - INTERVAL %d MICROSECOND", horizonOffset.Microseconds())).
		OrderBy("inserted_at asc")

	if objectTypeFilter != "" {
		sb = sb.Where(sq.Eq{"object_type": objectTypeFilter})
	}
	if opts.From != "" {
		token, err := sqlcommon.UnmarshallContToken(opts.From)
		if err != nil {
			return nil, nil, err
		}
		if token.ObjectType != objectTypeFilter {
			return nil, nil, storage.ErrMismatchObjectType
		}

		sb = sb.Where(sq.Gt{"ulid": token.Ulid}) // > as we always return a continuation token
	}
	if opts.PageSize > 0 {
		sb = sb.Limit(uint64(opts.PageSize)) // + 1 is NOT used here as we always return a continuation token
	}

	rows, err := sb.QueryContext(ctx)
	if err != nil {
		return nil, nil, sqlcommon.HandleSQLError(err)
	}
	defer rows.Close()

	var changes []*openfgapb.TupleChange
	var ulid string
	for rows.Next() {
		var objectType, objectID, relation, userObjectType, userObjectID, userRelation string
		var operation int
		var insertedAt time.Time

		err = rows.Scan(&ulid, &objectType, &objectID, &relation, &userObjectType, &userObjectID, &userRelation, &operation, &insertedAt)
		if err != nil {
			return nil, nil, sqlcommon.HandleSQLError(err)
		}

		changes = append(changes, &openfgapb.TupleChange{
			TupleKey: &openfgapb.TupleKey{
				Object:   tupleUtils.BuildObject(objectType, objectID),
				Relation: relation,
				User:     tupleUtils.FromUserParts(userObjectType, userObjectID, userRelation),
			},
			Operation: openfgapb.TupleOperation(operation),
			Timestamp: timestamppb.New(insertedAt.UTC()),
		})
	}

	if len(changes) == 0 {
		return nil, nil, storage.ErrNotFound
	}

	contToken, err := json.Marshal(sqlcommon.NewContToken(ulid, objectTypeFilter))
	if err != nil {
		return nil, nil, err
	}

	return changes, contToken, nil
}

// IsReady reports whether this MySQL datastore instance is ready
// to accept connections.
func (m *MySQL) IsReady(ctx context.Context) (bool, error) {
	return sqlcommon.IsReady(ctx, m.db)
}
