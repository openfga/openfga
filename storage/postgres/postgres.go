package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultMaxTuplesInWrite     = 100
	defaultMaxTypesInDefinition = 100
)

type Postgres struct {
	pool                     *pgxpool.Pool
	tracer                   trace.Tracer
	logger                   logger.Logger
	maxTuplesInWrite         int
	maxTypesInTypeDefinition int
}

var _ storage.OpenFGADatastore = &Postgres{}

type PostgresOption func(*Postgres)

func WithMaxTuplesInWrite(maxTuples int) PostgresOption {
	return func(p *Postgres) {
		p.maxTuplesInWrite = maxTuples
	}
}

func WithMaxTypesInTypeDefinition(maxTypes int) PostgresOption {
	return func(p *Postgres) {
		p.maxTypesInTypeDefinition = maxTypes
	}
}

func WithLogger(l logger.Logger) PostgresOption {
	return func(p *Postgres) {
		p.logger = l
	}
}

func WithTracer(t trace.Tracer) PostgresOption {
	return func(p *Postgres) {
		p.tracer = t
	}
}

func NewPostgresDatastore(uri string, opts ...PostgresOption) (*Postgres, error) {
	p := &Postgres{}

	for _, opt := range opts {
		opt(p)
	}

	if p.logger == nil {
		p.logger = logger.NewNoopLogger()
	}

	if p.tracer == nil {
		p.tracer = telemetry.NewNoopTracer()
	}

	if p.maxTuplesInWrite == 0 {
		p.maxTuplesInWrite = defaultMaxTuplesInWrite
	}

	if p.maxTypesInTypeDefinition == 0 {
		p.maxTypesInTypeDefinition = defaultMaxTypesInDefinition
	}

	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = 1 * time.Minute
	var pool *pgxpool.Pool
	attempt := 1
	err := backoff.Retry(func() error {
		var err error
		pool, err = pgxpool.Connect(context.Background(), uri)
		if err != nil {
			p.logger.Info("waiting for Postgres", zap.Int("attempt", attempt))
			attempt++
			return err
		}
		return nil
	}, policy)
	if err != nil {
		return nil, errors.Errorf("failed to initialize Postgres connection: %v", err)
	}

	p.pool = pool

	return p, nil
}

// Close closes any open connections and cleans up residual resources
// used by this storage adapter instance.
func (p *Postgres) Close(ctx context.Context) error {
	p.pool.Close()

	return nil
}

func (p *Postgres) ListObjectsByType(ctx context.Context, store string, objectType string) (storage.ObjectIterator, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ListObjectsByType")
	defer span.End()

	stmt := "SELECT DISTINCT object_type, object_id FROM tuple WHERE store = $1 AND object_type = $2"
	rows, err := p.pool.Query(ctx, stmt, store, objectType)
	if err != nil {
		return nil, err
	}

	return &objectIterator{rows: rows}, nil
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

	return iter.toArray(opts)
}

func (p *Postgres) read(ctx context.Context, store string, tupleKey *openfgapb.TupleKey, opts storage.PaginationOptions) (*tupleIterator, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.read")
	defer span.End()

	stmt, args, err := buildReadQuery(store, tupleKey, opts)
	if err != nil {
		return nil, err
	}

	rows, err := p.pool.Query(ctx, stmt, args...)
	if err != nil {
		return nil, handlePostgresError(err)
	}

	return &tupleIterator{rows: rows}, nil
}

func (p *Postgres) Write(ctx context.Context, store string, deletes storage.Deletes, writes storage.Writes) error {
	ctx, span := p.tracer.Start(ctx, "postgres.Write")
	defer span.End()

	if len(deletes)+len(writes) > p.MaxTuplesInWriteOperation() {
		return storage.ErrExceededWriteBatchLimit
	}

	now := time.Now().UTC()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return handlePostgresError(err)
	}
	defer rollbackTx(ctx, tx, p.logger)

	deleteBatch := &pgx.Batch{}
	writeBatch := &pgx.Batch{}
	changelogBatch := &pgx.Batch{}

	for _, tk := range deletes {
		ulid, err := id.NewStringFromTime(now)
		if err != nil {
			return err
		}
		objectType, objectID := tupleUtils.SplitObject(tk.GetObject())
		deleteBatch.Queue(`DELETE FROM tuple WHERE store = $1 AND object_type = $2 AND object_id = $3 AND relation = $4 AND _user = $5 AND user_type = $6`, store, objectType, objectID, tk.GetRelation(), tk.GetUser(), tupleUtils.GetUserTypeFromUser(tk.GetUser()))
		changelogBatch.Queue(`INSERT INTO changelog (store, object_type, object_id, relation, _user, operation, ulid, inserted_at) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())`, store, objectType, objectID, tk.GetRelation(), tk.GetUser(), openfgapb.TupleOperation_TUPLE_OPERATION_DELETE, ulid)
	}

	deleteResults := tx.SendBatch(ctx, deleteBatch)
	for i := 0; i < deleteBatch.Len(); i++ {
		tag, err := deleteResults.Exec()
		if err != nil {
			return handlePostgresError(err)
		}
		if tag.RowsAffected() != 1 {
			return storage.InvalidWriteInputError(deletes[i], openfgapb.TupleOperation_TUPLE_OPERATION_DELETE)
		}
	}
	if err := deleteResults.Close(); err != nil {
		return err
	}

	for _, tk := range writes {
		ulid, err := id.NewStringFromTime(now)
		if err != nil {
			return err
		}
		objectType, objectID := tupleUtils.SplitObject(tk.GetObject())
		writeBatch.Queue(`INSERT INTO tuple (store, object_type, object_id, relation, _user, user_type, ulid, inserted_at) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())`, store, objectType, objectID, tk.GetRelation(), tk.GetUser(), tupleUtils.GetUserTypeFromUser(tk.GetUser()), ulid)
		changelogBatch.Queue(`INSERT INTO changelog (store, object_type, object_id, relation, _user, operation, ulid, inserted_at) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())`, store, objectType, objectID, tk.GetRelation(), tk.GetUser(), openfgapb.TupleOperation_TUPLE_OPERATION_WRITE, ulid)
	}

	writeResults := tx.SendBatch(ctx, writeBatch)
	for i := 0; i < writeBatch.Len(); i++ {
		if _, err := writeResults.Exec(); err != nil {
			return handlePostgresError(err, writes[i])
		}
	}
	if err := writeResults.Close(); err != nil {
		return err
	}

	if err := tx.SendBatch(ctx, changelogBatch).Close(); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return handlePostgresError(err)
	}

	return nil
}

func (p *Postgres) ReadUserTuple(ctx context.Context, store string, tupleKey *openfgapb.TupleKey) (*openfgapb.Tuple, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ReadUserTuple")
	defer span.End()

	objectType, objectID := tupleUtils.SplitObject(tupleKey.GetObject())
	userType := tupleUtils.GetUserTypeFromUser(tupleKey.GetUser())

	row := p.pool.QueryRow(ctx, `SELECT object_type, object_id, relation, _user FROM tuple WHERE store = $1 AND object_type = $2 AND object_id = $3 AND relation = $4 AND _user = $5 AND user_type = $6`,
		store, objectType, objectID, tupleKey.GetRelation(), tupleKey.GetUser(), userType)

	var record tupleRecord
	if err := row.Scan(&record.objectType, &record.objectID, &record.relation, &record.user); err != nil {
		return nil, handlePostgresError(err)
	}

	return record.asTuple(), nil
}

func (p *Postgres) ReadUsersetTuples(ctx context.Context, store string, tupleKey *openfgapb.TupleKey) (storage.TupleIterator, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ReadUsersetTuples")
	defer span.End()

	stmt, args, err := buildReadUsersetTuplesQuery(store, tupleKey)
	if err != nil {
		return nil, err
	}

	rows, err := p.pool.Query(ctx, stmt, args...)
	if err != nil {
		return nil, handlePostgresError(err)
	}

	return &tupleIterator{rows: rows}, nil
}

func (p *Postgres) ReadByStore(ctx context.Context, store string, opts storage.PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ReadByStore")
	defer span.End()

	iter, err := p.read(ctx, store, nil, opts)
	if err != nil {
		return nil, nil, err
	}
	return iter.toArray(opts)
}

func (p *Postgres) MaxTuplesInWriteOperation() int {
	return p.maxTuplesInWrite
}

func (p *Postgres) ReadAuthorizationModel(ctx context.Context, store string, modelID string) (*openfgapb.AuthorizationModel, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ReadAuthorizationModel")
	defer span.End()

	stmt := "SELECT type, type_definition FROM authorization_model WHERE store = $1 AND authorization_model_id = $2"
	rows, err := p.pool.Query(ctx, stmt, store, modelID)
	if err != nil {
		return nil, handlePostgresError(err)
	}

	var typeDefs []*openfgapb.TypeDefinition

	for rows.Next() {
		var typeName string
		var marshalledTypeDef []byte
		err = rows.Scan(&typeName, &marshalledTypeDef)
		if err != nil {
			return nil, handlePostgresError(err)
		}

		var typeDef openfgapb.TypeDefinition
		if err := proto.Unmarshal(marshalledTypeDef, &typeDef); err != nil {
			return nil, err
		}

		typeDefs = append(typeDefs, &typeDef)
	}

	if err := rows.Err(); err != nil {
		return nil, handlePostgresError(err)
	}

	if len(typeDefs) == 0 {
		return nil, storage.ErrNotFound
	}

	return &openfgapb.AuthorizationModel{
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

	rows, err := p.pool.Query(ctx, stmt, args...)
	if err != nil {
		return nil, nil, handlePostgresError(err)
	}

	var modelIDs []string
	var modelID string

	for rows.Next() {
		err := rows.Scan(&modelID)
		if err != nil {
			return nil, nil, handlePostgresError(err)
		}

		modelIDs = append(modelIDs, modelID)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, handlePostgresError(err)
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
	err := p.pool.QueryRow(ctx, stmt, store).Scan(&modelID)
	if err != nil {
		return "", handlePostgresError(err)
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
	err := p.pool.QueryRow(ctx, stmt, store, modelID, objectType).Scan(&marshalledTypeDef)
	if err != nil {
		return nil, handlePostgresError(err)
	}

	var typeDef openfgapb.TypeDefinition
	if err := proto.Unmarshal(marshalledTypeDef, &typeDef); err != nil {
		return nil, err
	}

	return &typeDef, nil
}

func (p *Postgres) MaxTypesInTypeDefinition() int {
	return p.maxTypesInTypeDefinition
}

func (p *Postgres) WriteAuthorizationModel(
	ctx context.Context,
	store, modelID string,
	tds []*openfgapb.TypeDefinition,
) error {
	ctx, span := p.tracer.Start(ctx, "postgres.WriteAuthorizationModel")
	defer span.End()

	if len(tds) > p.MaxTypesInTypeDefinition() {
		return storage.ExceededMaxTypeDefinitionsLimitError(p.maxTypesInTypeDefinition)
	}

	stmt := "INSERT INTO authorization_model (store, authorization_model_id, type, type_definition) VALUES ($1, $2, $3, $4)"

	inserts := &pgx.Batch{}
	for _, td := range tds {
		marshalledTypeDef, err := proto.Marshal(td)
		if err != nil {
			return err
		}

		inserts.Queue(stmt, store, modelID, td.GetType(), marshalledTypeDef)
	}

	err := p.pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		return tx.SendBatch(ctx, inserts).Close()
	})
	if err != nil {
		return handlePostgresError(err)
	}

	return nil
}

func (p *Postgres) CreateStore(ctx context.Context, store *openfgapb.Store) (*openfgapb.Store, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.CreateStore")
	defer span.End()

	var id, name string
	var createdAt time.Time
	stmt := "INSERT INTO store (id, name, created_at, updated_at) VALUES ($1, $2, NOW(), NOW()) RETURNING id, name, created_at"
	err := p.pool.QueryRow(ctx, stmt, store.Id, store.Name).Scan(&id, &name, &createdAt)
	if err != nil {
		return nil, handlePostgresError(err)
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

	row := p.pool.QueryRow(ctx, "SELECT id, name, created_at, updated_at FROM store WHERE id = $1 AND deleted_at IS NULL", id)

	var storeID, name string
	var createdAt, updatedAt time.Time
	err := row.Scan(&storeID, &name, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, storage.ErrNotFound
		}
		return nil, handlePostgresError(err)
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

	rows, err := p.pool.Query(ctx, stmt, args...)
	if err != nil {
		return nil, nil, handlePostgresError(err)
	}

	var stores []*openfgapb.Store
	var id string
	for rows.Next() {
		var name string
		var createdAt, updatedAt time.Time
		err := rows.Scan(&id, &name, &createdAt, &updatedAt)
		if err != nil {
			return nil, nil, handlePostgresError(err)
		}

		stores = append(stores, &openfgapb.Store{
			Id:        id,
			Name:      name,
			CreatedAt: timestamppb.New(createdAt),
			UpdatedAt: timestamppb.New(updatedAt),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, nil, handlePostgresError(err)
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

func (p *Postgres) DeleteStore(ctx context.Context, id string) error {
	ctx, span := p.tracer.Start(ctx, "postgres.DeleteStore")
	defer span.End()

	_, err := p.pool.Exec(ctx, "UPDATE store SET deleted_at = NOW() WHERE id = $1", id)
	if err != nil {
		return handlePostgresError(err)
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
	_, err = p.pool.Exec(ctx, stmt, store, modelID, marshalledAssertions)
	if err != nil {
		return handlePostgresError(err)
	}

	return nil
}

func (p *Postgres) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgapb.Assertion, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ReadAssertions")
	defer span.End()

	var marshalledAssertions []byte
	err := p.pool.QueryRow(ctx, `SELECT assertions FROM assertion WHERE store = $1 AND authorization_model_id = $2`, store, modelID).Scan(&marshalledAssertions)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return []*openfgapb.Assertion{}, nil
		}
		return nil, handlePostgresError(err)
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

	rows, err := p.pool.Query(ctx, stmt, args...)
	if err != nil {
		return nil, nil, handlePostgresError(err)
	}

	var changes []*openfgapb.TupleChange
	var ulid string
	for rows.Next() {
		var objectType, objectID, relation, user string
		var operation int
		var insertedAt time.Time

		err = rows.Scan(&ulid, &objectType, &objectID, &relation, &user, &operation, &insertedAt)
		if err != nil {
			return nil, nil, handlePostgresError(err)
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

// IsReady reports whether this Postgres datastore instance is ready
// to accept connections.
func (p *Postgres) IsReady(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := p.pool.Ping(ctx); err != nil {
		return false, err
	}

	return true, nil
}
