package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/cenkalti/backoff/v4"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/logger"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/storage"
	sql2 "github.com/openfga/openfga/storage/sql"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Postgres struct {
	db                     *sql.DB
	tracer                 trace.Tracer
	logger                 logger.Logger
	maxTuplesPerWriteField int
	maxTypesPerModelField  int
}

func New(uri string, cfg *sql2.Config) (*Postgres, error) {
	db, err := sql.Open("pgx", uri)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize postgres connection: %w", err)
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
			cfg.Logger.Info("waiting for postgres", zap.Int("attempt", attempt))
			attempt++
			return err
		}
		return nil
	}, policy)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize postgres connection: %w", err)
	}

	return &Postgres{
		db:                     db,
		tracer:                 cfg.Tracer,
		logger:                 cfg.Logger,
		maxTuplesPerWriteField: cfg.MaxTuplesPerWriteField,
		maxTypesPerModelField:  cfg.MaxTypesPerModelField,
	}, nil
}

// Close closes any open connections and cleans up residual resources
// used by this storage adapter instance.
func (p *Postgres) Close() {
	p.db.Close()
}

func (p *Postgres) ListObjectsByType(ctx context.Context, store string, objectType string) (storage.ObjectIterator, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ListObjectsByType")
	defer span.End()

	stmt, args, err := squirrel.
		StatementBuilder.PlaceholderFormat(squirrel.Dollar).
		Select("object_type", "object_id").
		Distinct().
		From("tuple").
		Where(squirrel.Eq{
			"store":       store,
			"object_type": objectType,
		}).ToSql()
	if err != nil {
		return nil, err
	}

	rows, err := p.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, sql2.HandleSQLError(err)
	}

	return sql2.NewSQLObjectIterator(rows), nil
}

func (p *Postgres) Read(ctx context.Context, store string, tupleKey *openfgapb.TupleKey) (storage.TupleIterator, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.Read")
	defer span.End()

	return p.read(ctx, store, tupleKey, storage.PaginationOptions{})
}

func (p *Postgres) ReadPage(ctx context.Context, store string, tupleKey *openfgapb.TupleKey, opts storage.PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ReadPage")
	defer span.End()

	iter, err := p.read(ctx, store, tupleKey, opts)
	if err != nil {
		return nil, nil, err
	}
	defer iter.Stop()

	return iter.ToArray(ctx, opts)
}

func (p *Postgres) read(ctx context.Context, store string, tupleKey *openfgapb.TupleKey, opts storage.PaginationOptions) (*sql2.SQLTupleIterator, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.read")
	defer span.End()

	stmt, args, err := buildReadQuery(store, tupleKey, opts)
	if err != nil {
		return nil, err
	}

	rows, err := p.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, sql2.HandleSQLError(err)
	}

	return sql2.NewSQLTupleIterator(rows), nil
}

func (p *Postgres) Write(ctx context.Context, store string, deletes storage.Deletes, writes storage.Writes) error {
	ctx, span := p.tracer.Start(ctx, "postgres.Write")
	defer span.End()

	if len(deletes)+len(writes) > p.MaxTuplesPerWrite() {
		return storage.ErrExceededWriteBatchLimit
	}

	now := time.Now().UTC()
	txn, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return sql2.HandleSQLError(err)
	}
	defer rollbackTx(ctx, txn, p.logger)

	changelogBuilder := squirrel.
		StatementBuilder.PlaceholderFormat(squirrel.Dollar).
		Insert("changelog").
		Columns("store", "object_type", "object_id", "relation", "_user", "operation", "ulid", "inserted_at")

	deletebuilder := squirrel.
		StatementBuilder.PlaceholderFormat(squirrel.Dollar).
		Delete("tuple")

	for _, tk := range deletes {
		id := ulid.MustNew(ulid.Timestamp(now), ulid.DefaultEntropy()).String()
		objectType, objectID := tupleUtils.SplitObject(tk.GetObject())

		stmt, args, err := deletebuilder.Where(squirrel.Eq{
			"store":       store,
			"object_type": objectType,
			"object_id":   objectID,
			"relation":    tk.GetRelation(),
			"_user":       tk.GetUser(),
			"user_type":   tupleUtils.GetUserTypeFromUser(tk.GetUser()),
		}).ToSql()
		if err != nil {
			return sql2.HandleSQLError(err)
		}

		res, err := txn.ExecContext(ctx, stmt, args...)
		if err != nil {
			return sql2.HandleSQLError(err, tk)
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return sql2.HandleSQLError(err)
		}

		if rowsAffected != 1 {
			return storage.InvalidWriteInputError(tk, openfgapb.TupleOperation_TUPLE_OPERATION_DELETE)
		}

		changelogBuilder = changelogBuilder.Values(store, objectType, objectID, tk.GetRelation(), tk.GetUser(), openfgapb.TupleOperation_TUPLE_OPERATION_DELETE, id, "NOW()")
	}

	tupleInsertBuilder := squirrel.
		StatementBuilder.PlaceholderFormat(squirrel.Dollar).
		Insert("tuple").
		Columns("store", "object_type", "object_id", "relation", "_user", "user_type", "ulid", "inserted_at")

	for _, tk := range writes {
		id := ulid.MustNew(ulid.Timestamp(now), ulid.DefaultEntropy()).String()
		objectType, objectID := tupleUtils.SplitObject(tk.GetObject())

		stmt, args, err := tupleInsertBuilder.Values(store, objectType, objectID, tk.GetRelation(), tk.GetUser(), tupleUtils.GetUserTypeFromUser(tk.GetUser()), id, "NOW()").ToSql()
		if err != nil {
			return sql2.HandleSQLError(err)
		}

		_, err = txn.ExecContext(ctx, stmt, args...)
		if err != nil {
			return sql2.HandleSQLError(err, tk)
		}

		changelogBuilder = changelogBuilder.Values(store, objectType, objectID, tk.GetRelation(), tk.GetUser(), openfgapb.TupleOperation_TUPLE_OPERATION_WRITE, id, "NOW()")
	}

	if len(writes) > 0 || len(deletes) > 0 {
		stmt, args, err := changelogBuilder.ToSql()
		if err != nil {
			return sql2.HandleSQLError(err)
		}

		_, err = txn.ExecContext(ctx, stmt, args...)
		if err != nil {
			return sql2.HandleSQLError(err)
		}
	}

	if err := txn.Commit(); err != nil {
		return sql2.HandleSQLError(err)
	}

	return nil
}

func (p *Postgres) ReadUserTuple(ctx context.Context, store string, tupleKey *openfgapb.TupleKey) (*openfgapb.Tuple, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ReadUserTuple")
	defer span.End()

	objectType, objectID := tupleUtils.SplitObject(tupleKey.GetObject())
	userType := tupleUtils.GetUserTypeFromUser(tupleKey.GetUser())

	row := p.db.QueryRowContext(ctx, `SELECT object_type, object_id, relation, _user FROM tuple WHERE store = $1 AND object_type = $2 AND object_id = $3 AND relation = $4 AND _user = $5 AND user_type = $6`,
		store, objectType, objectID, tupleKey.GetRelation(), tupleKey.GetUser(), userType)

	var record sql2.TupleRecord
	if err := row.Scan(&record.ObjectType, &record.ObjectID, &record.Relation, &record.User); err != nil {
		return nil, sql2.HandleSQLError(err)
	}

	return record.AsTuple(), nil
}

func (p *Postgres) ReadUsersetTuples(ctx context.Context, store string, tupleKey *openfgapb.TupleKey) (storage.TupleIterator, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ReadUsersetTuples")
	defer span.End()

	stmt, args, err := buildReadUsersetTuplesQuery(store, tupleKey)
	if err != nil {
		return nil, err
	}

	rows, err := p.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, sql2.HandleSQLError(err)
	}

	return sql2.NewSQLTupleIterator(rows), nil
}

func (p *Postgres) ReadStartingWithUser(ctx context.Context, store string, opts storage.ReadStartingWithUserFilter) (storage.TupleIterator, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ReadStartingWithUser")
	defer span.End()

	stmt := "SELECT store, object_type, object_id, relation, _user, ulid, inserted_at FROM tuple WHERE store = $1 AND object_type = $2 AND relation = $3 AND _user = any($4)"
	var targetUsersArg []string
	for _, u := range opts.UserFilter {
		targetUser := u.GetObject()
		if u.GetRelation() != "" {
			targetUser = strings.Join([]string{u.GetObject(), u.GetRelation()}, "#")
		}
		targetUsersArg = append(targetUsersArg, targetUser)
	}

	rows, err := p.db.QueryContext(ctx, stmt, store, opts.ObjectType, opts.Relation, targetUsersArg)
	if err != nil {
		return nil, sql2.HandleSQLError(err)
	}

	return sql2.NewSQLTupleIterator(rows), nil
}

func (p *Postgres) MaxTuplesPerWrite() int {
	return p.maxTuplesPerWriteField
}

func (p *Postgres) ReadAuthorizationModel(ctx context.Context, store string, modelID string) (*openfgapb.AuthorizationModel, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ReadAuthorizationModel")
	defer span.End()

	stmt := "SELECT schema_version, type, type_definition FROM authorization_model WHERE store = $1 AND authorization_model_id = $2"
	rows, err := p.db.QueryContext(ctx, stmt, store, modelID)
	if err != nil {
		return nil, sql2.HandleSQLError(err)
	}
	defer rows.Close()

	var schemaVersion string
	var typeDefs []*openfgapb.TypeDefinition
	for rows.Next() {
		var typeName string
		var marshalledTypeDef []byte
		err = rows.Scan(&schemaVersion, &typeName, &marshalledTypeDef)
		if err != nil {
			return nil, sql2.HandleSQLError(err)
		}

		var typeDef openfgapb.TypeDefinition
		if err := proto.Unmarshal(marshalledTypeDef, &typeDef); err != nil {
			return nil, err
		}

		typeDefs = append(typeDefs, &typeDef)
	}

	if err := rows.Err(); err != nil {
		return nil, sql2.HandleSQLError(err)
	}

	if len(typeDefs) == 0 {
		return nil, storage.ErrNotFound
	}

	// Update the schema version lazily if it is not a valid typesystem.SchemaVersion.
	if schemaVersion != typesystem.SchemaVersion1_0 && schemaVersion != typesystem.SchemaVersion1_1 {
		schemaVersion = typesystem.SchemaVersion1_0
		_, err = p.db.ExecContext(ctx, "UPDATE authorization_model SET schema_version = $1 WHERE store = $2 AND authorization_model_id = $3", schemaVersion, store, modelID)
		if err != nil {
			// Don't worry if we error, we'll update it lazily next time, but let's log:
			p.logger.Warn("failed to lazily update schema version", zap.String("store", store), zap.String("authorization_model_id", modelID))
		}
	}

	return &openfgapb.AuthorizationModel{
		SchemaVersion:   schemaVersion,
		Id:              modelID,
		TypeDefinitions: typeDefs,
	}, nil
}

func (p *Postgres) ReadAuthorizationModels(ctx context.Context, store string, opts storage.PaginationOptions) ([]*openfgapb.AuthorizationModel, []byte, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ReadAuthorizationModels")
	defer span.End()

	stmt, args, err := buildReadAuthorizationModelsQuery(store, opts)
	if err != nil {
		return nil, nil, err
	}

	rows, err := p.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, nil, sql2.HandleSQLError(err)
	}
	defer rows.Close()

	var modelIDs []string
	var modelID string

	for rows.Next() {
		err := rows.Scan(&modelID)
		if err != nil {
			return nil, nil, sql2.HandleSQLError(err)
		}

		modelIDs = append(modelIDs, modelID)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, sql2.HandleSQLError(err)
	}

	var token []byte
	numModelIDs := len(modelIDs)
	if len(modelIDs) > opts.PageSize {
		numModelIDs = opts.PageSize
		token, err = json.Marshal(sql2.NewContToken(modelID, ""))
		if err != nil {
			return nil, nil, err
		}
	}

	// TODO: make this concurrent with a maximum of 5 goroutines. This may be helpful:
	// https://stackoverflow.com/questions/25306073/always-have-x-number-of-goroutines-running-at-any-time
	models := make([]*openfgapb.AuthorizationModel, 0, numModelIDs)
	// We use numModelIDs here to avoid retrieving possibly one extra model.
	for i := 0; i < numModelIDs; i++ {
		model, err := p.ReadAuthorizationModel(ctx, store, modelIDs[i])
		if err != nil {
			return nil, nil, err
		}
		models = append(models, model)
	}

	return models, token, nil
}

func (p *Postgres) FindLatestAuthorizationModelID(ctx context.Context, store string) (string, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.FindLatestAuthorizationModelID")
	defer span.End()

	var modelID string
	stmt := "SELECT authorization_model_id FROM authorization_model WHERE store = $1 ORDER BY authorization_model_id DESC LIMIT 1"
	err := p.db.QueryRowContext(ctx, stmt, store).Scan(&modelID)
	if err != nil {
		return "", sql2.HandleSQLError(err)
	}

	return modelID, nil
}

func (p *Postgres) ReadTypeDefinition(
	ctx context.Context,
	store, modelID, objectType string,
) (*openfgapb.TypeDefinition, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ReadTypeDefinition")
	defer span.End()

	var marshalledTypeDef []byte
	stmt := "SELECT type_definition FROM authorization_model WHERE store = $1 AND authorization_model_id = $2 AND type = $3"
	err := p.db.QueryRowContext(ctx, stmt, store, modelID, objectType).Scan(&marshalledTypeDef)
	if err != nil {
		return nil, sql2.HandleSQLError(err)
	}

	var typeDef openfgapb.TypeDefinition
	if err := proto.Unmarshal(marshalledTypeDef, &typeDef); err != nil {
		return nil, err
	}

	return &typeDef, nil
}

func (p *Postgres) MaxTypesPerAuthorizationModel() int {
	return p.maxTypesPerModelField
}

func (p *Postgres) WriteAuthorizationModel(ctx context.Context, store string, model *openfgapb.AuthorizationModel) error {
	ctx, span := p.tracer.Start(ctx, "postgres.WriteAuthorizationModel")
	defer span.End()

	schemaVersion := model.GetSchemaVersion()
	typeDefinitions := model.GetTypeDefinitions()

	if len(typeDefinitions) > p.MaxTypesPerAuthorizationModel() {
		return storage.ExceededMaxTypeDefinitionsLimitError(p.maxTypesPerModelField)
	}

	if len(typeDefinitions) < 1 {
		return nil
	}

	sqlbuilder := squirrel.
		StatementBuilder.PlaceholderFormat(squirrel.Dollar).
		Insert("authorization_model").Columns("store", "authorization_model_id", "schema_version", "type", "type_definition")

	for _, td := range typeDefinitions {
		marshalledTypeDef, err := proto.Marshal(td)
		if err != nil {
			return err
		}

		sqlbuilder = sqlbuilder.Values(store, model.Id, schemaVersion, td.GetType(), marshalledTypeDef)
	}

	stmt, args, err := sqlbuilder.ToSql()
	if err != nil {
		return sql2.HandleSQLError(err)
	}

	_, err = p.db.ExecContext(ctx, stmt, args...)
	if err != nil {
		return sql2.HandleSQLError(err)
	}

	return nil
}

func (p *Postgres) CreateStore(ctx context.Context, store *openfgapb.Store) (*openfgapb.Store, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.CreateStore")
	defer span.End()

	var id, name string
	var createdAt time.Time
	stmt := "INSERT INTO store (id, name, created_at, updated_at) VALUES ($1, $2, NOW(), NOW()) RETURNING id, name, created_at"
	err := p.db.QueryRowContext(ctx, stmt, store.Id, store.Name).Scan(&id, &name, &createdAt)
	if err != nil {
		return nil, sql2.HandleSQLError(err)
	}

	return &openfgapb.Store{
		Id:        id,
		Name:      name,
		CreatedAt: timestamppb.New(createdAt),
		UpdatedAt: timestamppb.New(createdAt),
	}, nil
}

func (p *Postgres) GetStore(ctx context.Context, id string) (*openfgapb.Store, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.GetStore")
	defer span.End()

	row := p.db.QueryRowContext(ctx, "SELECT id, name, created_at, updated_at FROM store WHERE id = $1 AND deleted_at IS NULL", id)

	var storeID, name string
	var createdAt, updatedAt time.Time
	err := row.Scan(&storeID, &name, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, storage.ErrNotFound
		}
		return nil, sql2.HandleSQLError(err)
	}

	return &openfgapb.Store{
		Id:        storeID,
		Name:      name,
		CreatedAt: timestamppb.New(createdAt),
		UpdatedAt: timestamppb.New(updatedAt),
	}, nil
}

func (p *Postgres) ListStores(ctx context.Context, opts storage.PaginationOptions) ([]*openfgapb.Store, []byte, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ListStores")
	defer span.End()

	stmt, args, err := buildListStoresQuery(opts)
	if err != nil {
		return nil, nil, err
	}

	rows, err := p.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, nil, sql2.HandleSQLError(err)
	}
	defer rows.Close()

	var stores []*openfgapb.Store
	var id string
	for rows.Next() {
		var name string
		var createdAt, updatedAt time.Time
		err := rows.Scan(&id, &name, &createdAt, &updatedAt)
		if err != nil {
			return nil, nil, sql2.HandleSQLError(err)
		}

		stores = append(stores, &openfgapb.Store{
			Id:        id,
			Name:      name,
			CreatedAt: timestamppb.New(createdAt),
			UpdatedAt: timestamppb.New(updatedAt),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, nil, sql2.HandleSQLError(err)
	}

	if len(stores) > opts.PageSize {
		contToken, err := json.Marshal(sql2.NewContToken(id, ""))
		if err != nil {
			return nil, nil, err
		}
		return stores[:opts.PageSize], contToken, nil
	}

	return stores, nil, nil
}

func (p *Postgres) DeleteStore(ctx context.Context, id string) error {
	ctx, span := p.tracer.Start(ctx, "postgres.DeleteStore")
	defer span.End()

	_, err := p.db.ExecContext(ctx, "UPDATE store SET deleted_at = NOW() WHERE id = $1", id)
	if err != nil {
		return sql2.HandleSQLError(err)
	}

	return nil
}

func (p *Postgres) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgapb.Assertion) error {
	ctx, span := p.tracer.Start(ctx, "postgres.WriteAssertions")
	defer span.End()

	marshalledAssertions, err := proto.Marshal(&openfgapb.Assertions{Assertions: assertions})
	if err != nil {
		return err
	}

	stmt := "INSERT INTO assertion (store, authorization_model_id, assertions) VALUES ($1, $2, $3) ON CONFLICT (store, authorization_model_id) DO UPDATE SET assertions = $3"
	_, err = p.db.ExecContext(ctx, stmt, store, modelID, marshalledAssertions)
	if err != nil {
		return sql2.HandleSQLError(err)
	}

	return nil
}

func (p *Postgres) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgapb.Assertion, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ReadAssertions")
	defer span.End()

	var marshalledAssertions []byte
	err := p.db.QueryRowContext(ctx, `SELECT assertions FROM assertion WHERE store = $1 AND authorization_model_id = $2`, store, modelID).Scan(&marshalledAssertions)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []*openfgapb.Assertion{}, nil
		}
		return nil, sql2.HandleSQLError(err)
	}

	var assertions openfgapb.Assertions
	err = proto.Unmarshal(marshalledAssertions, &assertions)
	if err != nil {
		return nil, err
	}

	return assertions.Assertions, nil
}

func (p *Postgres) ReadChanges(
	ctx context.Context,
	store, objectTypeFilter string,
	opts storage.PaginationOptions,
	horizonOffset time.Duration,
) ([]*openfgapb.TupleChange, []byte, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ReadChanges")
	defer span.End()

	stmt, args, err := buildReadChangesQuery(store, objectTypeFilter, opts, horizonOffset)
	if err != nil {
		return nil, nil, err
	}

	rows, err := p.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, nil, sql2.HandleSQLError(err)
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
			return nil, nil, sql2.HandleSQLError(err)
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

	contToken, err := json.Marshal(sql2.NewContToken(ulid, objectTypeFilter))
	if err != nil {
		return nil, nil, err
	}

	return changes, contToken, nil
}

// IsReady reports whether this Postgres datastore instance is ready
// to accept connections.
func (p *Postgres) IsReady(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := p.db.PingContext(ctx); err != nil {
		return false, err
	}

	return true, nil
}
