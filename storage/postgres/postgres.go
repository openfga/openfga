package postgres

import (
	"context"
	"time"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/tuple"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/storage"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.buf.build/openfga/go/openfga/api/openfga"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/encoding/protojson"
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

var _ storage.TupleBackend = &Postgres{}
var _ storage.AuthorizationModelBackend = &Postgres{}
var _ storage.AssertionsBackend = &Postgres{}
var _ storage.ChangelogBackend = &Postgres{}

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

func New(
	pool *pgxpool.Pool,
	tracer trace.Tracer,
	logger logger.Logger,
	opts ...PostgresOption,
) *Postgres {

	p := &Postgres{
		pool:   pool,
		tracer: tracer,
		logger: logger,
	}

	for _, opt := range opts {
		opt(p)
	}

	if p.maxTuplesInWrite == 0 {
		p.maxTuplesInWrite = defaultMaxTuplesInWrite
	}

	if p.maxTypesInTypeDefinition == 0 {
		p.maxTypesInTypeDefinition = defaultMaxTypesInDefinition
	}

	return p
}

func (p *Postgres) Read(ctx context.Context, store string, tupleKey *openfga.TupleKey) (storage.TupleIterator, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.Read")
	defer span.End()

	return p.read(ctx, store, tupleKey, storage.PaginationOptions{})
}

func (p *Postgres) ReadPage(ctx context.Context, store string, tupleKey *openfga.TupleKey, opts storage.PaginationOptions) ([]*openfga.Tuple, []byte, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ReadPage")
	defer span.End()

	iter, err := p.read(ctx, store, tupleKey, opts)
	if err != nil {
		return nil, nil, err
	}

	return iter.toArray(opts)
}

func (p *Postgres) read(ctx context.Context, store string, tupleKey *openfga.TupleKey, opts storage.PaginationOptions) (*tupleIterator, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.read")
	defer span.End()

	readQuery := buildReadQuery(store, tupleKey, opts)
	rows, err := p.pool.Query(ctx, readQuery)
	if err != nil {
		return nil, handlePostgresError(err)
	}

	return &tupleIterator{rows: rows}, nil
}

func (p *Postgres) Write(ctx context.Context, store string, deletes storage.Deletes, writes storage.Writes) error {
	ctx, span := p.tracer.Start(ctx, "postgres.Write")
	defer span.End()

	if len(deletes)+len(writes) > p.MaxTuplesInWriteOperation() {
		return storage.ExceededWriteBatchLimit
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
		changelogBatch.Queue(`INSERT INTO changelog (store, object_type, object_id, relation, _user, operation, ulid, inserted_at) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())`, store, objectType, objectID, tk.GetRelation(), tk.GetUser(), openfga.TupleOperation_DELETE, ulid)
	}

	deleteResults := tx.SendBatch(ctx, deleteBatch)
	for i := 0; i < len(deletes); i++ {
		tag, err := deleteResults.Exec()
		if err != nil {
			return handlePostgresError(err)
		}
		if tag.RowsAffected() != 1 {
			return storage.InvalidWriteInputError(deletes[i], openfga.TupleOperation_DELETE)
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
		changelogBatch.Queue(`INSERT INTO changelog (store, object_type, object_id, relation, _user, operation, ulid, inserted_at) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())`, store, objectType, objectID, tk.GetRelation(), tk.GetUser(), openfga.TupleOperation_WRITE, ulid)
	}

	writeResults := tx.SendBatch(ctx, writeBatch)
	for i := 0; i < len(writes); i++ {
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

func (p *Postgres) ReadUserTuple(ctx context.Context, store string, tupleKey *openfga.TupleKey) (*openfga.Tuple, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ReadUserTuple")
	defer span.End()

	objectType, objectID := tupleUtils.SplitObject(tupleKey.GetObject())
	row := p.pool.QueryRow(ctx, `SELECT object_type, object_id, relation, _user FROM tuple WHERE store = $1 AND object_type = $2 AND object_id = $3 AND relation = $4 AND _user = $5 AND user_type = $6`,
		store, objectType, objectID, tupleKey.GetRelation(), tupleKey.GetUser(), tupleUtils.User)
	var record tupleRecord
	if err := row.Scan(&record.objectType, &record.objectId, &record.relation, &record.user); err != nil {
		return nil, handlePostgresError(err)
	}

	return record.asTuple(), nil
}

func (p *Postgres) ReadUsersetTuples(ctx context.Context, store string, tupleKey *openfga.TupleKey) (storage.TupleIterator, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ReadUsersetTuples")
	defer span.End()

	stmt := buildReadUsersetTuplesQuery(store, tupleKey)
	rows, err := p.pool.Query(ctx, stmt)
	if err != nil {
		return nil, handlePostgresError(err)
	}

	return &tupleIterator{rows: rows}, nil
}

func (p *Postgres) ReadByStore(ctx context.Context, store string, opts storage.PaginationOptions) ([]*openfga.Tuple, []byte, error) {
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

func (p *Postgres) ReadAuthorizationModel(ctx context.Context, store string, id string) (*openfgav1pb.AuthorizationModel, error) {

	stmt := `SELECT type, relation, relation_definition FROM type_definition WHERE store = $1 AND authorization_model_id = $2`

	rows, err := p.pool.Query(ctx, stmt, store, id)
	if err != nil {
		return nil, handlePostgresError(err)
	}

	typeDefinitions := map[string]map[string]*openfgav1pb.Userset{}

	for rows.Next() {
		var typeName, relation, jsonRelationDefinition string
		err := rows.Scan(&typeName, &relation, &jsonRelationDefinition)
		if err != nil {
			return nil, handlePostgresError(err)
		}

		if _, ok := typeDefinitions[typeName]; !ok {
			typeDefinitions[typeName] = map[string]*openfgav1pb.Userset{}
		}

		var userset openfgav1pb.Userset
		if err := protojson.Unmarshal([]byte(jsonRelationDefinition), &userset); err != nil {
			return nil, err
		}

		typeDefinitions[typeName][relation] = &userset
	}

	if err := rows.Err(); err != nil {
		return nil, handlePostgresError(err)
	}

	if len(typeDefinitions) == 0 {
		return nil, storage.NotFound
	}

	model := &openfgav1pb.AuthorizationModel{
		Id:              id,
		TypeDefinitions: mapOfTypeDefinitionsToSlice(typeDefinitions),
	}

	return model, nil
}

func (p *Postgres) ReadAuthorizationModels(ctx context.Context, store string, opts storage.PaginationOptions) ([]string, []byte, error) {

	stmt := buildReadAuthorizationModelsQuery(store, opts)
	rows, err := p.pool.Query(ctx, stmt)
	if err != nil {
		return nil, nil, handlePostgresError(err)
	}

	var modelIDs []string
	var insertedAt time.Time

	for rows.Next() {
		var modelID string

		err := rows.Scan(&modelID, &insertedAt)
		if err != nil {
			return nil, nil, handlePostgresError(err)
		}

		modelIDs = append(modelIDs, modelID)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, handlePostgresError(err)
	}

	if len(modelIDs) > opts.PageSize {
		return modelIDs[:opts.PageSize], []byte(insertedAt.UTC().Format(time.RFC3339Nano)), nil
	}

	return modelIDs, nil, nil
}

func (p *Postgres) FindLatestAuthorizationModelID(ctx context.Context, store string) (string, error) {

	stmt := `SELECT authorization_model_id FROM type_definition WHERE store = $1 ORDER BY inserted_at DESC LIMIT 1`
	row := p.pool.QueryRow(ctx, stmt, store)

	var modelID string
	err := row.Scan(&modelID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", storage.NotFound
		}

		return "", err
	}

	return modelID, nil
}

func (p *Postgres) ReadTypeDefinition(
	ctx context.Context,
	store, modelID, objectType string,
) (*openfgav1pb.TypeDefinition, error) {

	stmt := `SELECT relation, relation_definition FROM type_definition WHERE store = $1 AND authorization_model_id = $2 AND type = $3`

	rows, err := p.pool.Query(ctx, stmt, store, modelID, objectType)
	if err != nil {
		return nil, handlePostgresError(err)
	}

	definition := &openfgav1pb.TypeDefinition{
		Type:      objectType,
		Relations: map[string]*openfgav1pb.Userset{},
	}

	for rows.Next() {

		var relation, jsonRelationDefinition string
		err := rows.Scan(&relation, &jsonRelationDefinition)
		if err != nil {
			return nil, handlePostgresError(err)
		}

		var userset openfgav1pb.Userset
		if err := protojson.Unmarshal([]byte(jsonRelationDefinition), &userset); err != nil {
			return nil, err
		}

		definition.Relations[relation] = &userset
	}

	if rows.Err() != nil {
		return nil, handlePostgresError(rows.Err())
	}

	return definition, nil
}

func (p *Postgres) MaxTypesInTypeDefinition() int {
	return p.maxTypesInTypeDefinition
}

func (p *Postgres) WriteAuthorizationModel(
	ctx context.Context,
	store, id string,
	tds *openfgav1pb.TypeDefinitions,
) error {

	if len(tds.TypeDefinitions) > p.MaxTypesInTypeDefinition() {
		return storage.ExceededMaxTypeDefinitionsLimitError(p.maxTypesInTypeDefinition)
	}

	stmt := `INSERT INTO type_definition (store, authorization_model_id, type, relation, relation_definition, inserted_at) VALUES ($1, $2, $3, $4, $5, NOW())`

	inserts := &pgx.Batch{}
	for _, typeDef := range tds.TypeDefinitions {

		for relation, userset := range typeDef.Relations {
			jsonUserset, err := protojson.Marshal(userset)
			if err != nil {
				return err
			}

			inserts.Queue(stmt, store, id, typeDef.Type, relation, jsonUserset)
		}
	}

	err := p.pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		if err := tx.SendBatch(ctx, inserts).Close(); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return handlePostgresError(err)
	}

	return nil
}

func (p *Postgres) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfga.Assertion) error {
	ctx, span := p.tracer.Start(ctx, "postgres.WriteAssertions")
	defer span.End()

	batch := &pgx.Batch{}
	for _, assertion := range assertions {
		tk := assertion.GetTupleKey()
		batch.Queue(`INSERT INTO assertion (store, authorization_model_id, object, relation, _user, expectation, inserted_at) VALUES ($1, $2, $3, $4, $5, $6, NOW())`, store, modelID, tk.GetObject(), tk.GetRelation(), tk.GetUser(), assertion.GetExpectation())
	}

	if err := p.pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		return tx.SendBatch(ctx, batch).Close()
	}); err != nil {
		return handlePostgresError(err)
	}

	return nil
}

func (p *Postgres) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfga.Assertion, error) {
	ctx, span := p.tracer.Start(ctx, "postgres.ReadAssertions")
	defer span.End()

	rows, err := p.pool.Query(ctx, `SELECT object, relation, _user, expectation FROM assertion WHERE store = $1 AND authorization_model_id = $2`, store, modelID)
	if err != nil {
		return nil, handlePostgresError(err)
	}

	var assertions []*openfga.Assertion
	for rows.Next() {
		var tk openfga.TupleKey
		var expectation bool
		if err := rows.Scan(&tk.Object, &tk.Relation, &tk.User, &expectation); err != nil {
			return nil, handlePostgresError(err)
		}
		assertions = append(assertions, &openfga.Assertion{
			TupleKey:    &tk,
			Expectation: expectation,
		})
	}

	if rows.Err() != nil {
		return nil, handlePostgresError(rows.Err())
	}

	return assertions, nil
}

func (p *Postgres) ReadChanges(
	ctx context.Context,
	store, objectType string,
	opts storage.PaginationOptions,
	horizonOffset time.Duration,
) ([]*openfga.TupleChange, []byte, error) {

	stmt := buildReadChangesQuery(store, objectType, opts, horizonOffset)
	rows, err := p.pool.Query(ctx, stmt)
	if err != nil {
		return nil, nil, handlePostgresError(err)
	}

	var changes []*openfga.TupleChange
	var ulid string

	for rows.Next() {
		var objectID, relation, user string
		var operation int
		var insertedAt time.Time

		err = rows.Scan(&ulid, &objectID, &relation, &user, &operation, &insertedAt)
		if err != nil {
			return nil, nil, handlePostgresError(err)
		}

		changes = append(changes, &openfga.TupleChange{
			TupleKey: &openfga.TupleKey{
				Object:   tuple.BuildObject(objectType, objectID),
				Relation: relation,
				User:     user,
			},
			Operation: openfga.TupleOperation(operation),
			Timestamp: timestamppb.New(insertedAt.UTC()),
		})
	}

	if len(changes) == 0 {
		return nil, nil, storage.NotFound
	}

	return changes, []byte(ulid), nil
}

func mapOfTypeDefinitionsToSlice(typeDefinitions map[string]map[string]*openfgav1pb.Userset) []*openfgav1pb.TypeDefinition {

	t := make([]*openfgav1pb.TypeDefinition, 0, len(typeDefinitions))

	for typeName, relations := range typeDefinitions {
		t = append(t, &openfgav1pb.TypeDefinition{
			Type:      typeName,
			Relations: relations,
		})
	}

	return t
}
