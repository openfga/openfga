package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/cenkalti/backoff/v4"
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver.
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

var tracer = otel.Tracer("openfga/pkg/storage/postgres")

func startTrace(ctx context.Context, name string) (context.Context, trace.Span) {
	return tracer.Start(ctx, "postgres."+name)
}

// Datastore provides a PostgreSQL based implementation of [storage.OpenFGADatastore].
type Datastore struct {
	writeStbl              sq.StatementBuilderType
	readStbl               sq.StatementBuilderType
	writeDb                *sql.DB
	readDb                 *sql.DB
	writeDbInfo            *sqlcommon.DBInfo
	readDbInfo             *sqlcommon.DBInfo
	logger                 logger.Logger
	writeDbStatsCollector  prometheus.Collector
	readDbStatsCollector   prometheus.Collector
	maxTuplesPerWriteField int
	maxTypesPerModelField  int
}

type Option func(*Datastore)

func WithReadDB(uri string, cfg *sqlcommon.Config) Option {
	return func(d *Datastore) {
		db, err := initDB(uri, cfg.ReadUsername, cfg.ReadPassword, cfg)
		if err != nil {
			d.logger.Error("failed to initialize read db", zap.Error(err))
			return
		}
		d.readDb = db
	}
}

// Ensures that Datastore implements the OpenFGADatastore interface.
var _ storage.OpenFGADatastore = (*Datastore)(nil)

// initDB initializes a new postgres database connection.
func initDB(uri string, username string, password string, cfg *sqlcommon.Config) (*sql.DB, error) {
	if username != "" || password != "" {
		parsed, err := url.Parse(uri)
		if err != nil {
			return nil, fmt.Errorf("parse postgres connection uri: %w", err)
		}

		username := ""
		if cfg.Username != "" {
			username = cfg.Username
		} else if parsed.User != nil {
			username = parsed.User.Username()
		}

		switch {
		case cfg.Password != "":
			parsed.User = url.UserPassword(username, cfg.Password)
		case parsed.User != nil:
			if password, ok := parsed.User.Password(); ok {
				parsed.User = url.UserPassword(username, password)
			} else {
				parsed.User = url.User(username)
			}
		default:
			parsed.User = url.User(username)
		}

		uri = parsed.String()
	}

	db, err := sql.Open("pgx", uri)
	if err != nil {
		return nil, fmt.Errorf("initialize postgres connection: %w", err)
	}

	if cfg.MaxIdleConns != 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns) // default is 2, not retaining connections(0) would be detrimental for performance
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	return db, nil
}

// New creates a new [Datastore] storage.
func New(uri string, cfg *sqlcommon.Config, opts ...Option) (*Datastore, error) {

	writeDb, err := initDB(uri, cfg.Username, cfg.Password, cfg)
	if err != nil {
		return nil, fmt.Errorf("initialize postgres connection: %w", err)
	}

	return NewWithDB(writeDb, cfg)
}

// configureDB configures a postgres database connection.
func configureDB(db *sql.DB, cfg *sqlcommon.Config) (prometheus.Collector, error) {
	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = 1 * time.Minute
	attempt := 1
	err := backoff.Retry(func() error {
		err := db.PingContext(context.Background())
		if err != nil {
			cfg.Logger.Info("waiting for database", zap.Int("attempt", attempt))
			attempt++
			return err
		}
		return nil
	}, policy)
	if err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}

	var collector prometheus.Collector
	if cfg.ExportMetrics {
		collector = collectors.NewDBStatsCollector(db, "openfga")
		if err := prometheus.Register(collector); err != nil {
			return nil, fmt.Errorf("initialize metrics: %w", err)
		}
	}

	return collector, nil
}

// NewWithDB creates a new [Datastore] storage with the provided database connection.
func NewWithDB(writeDb *sql.DB, cfg *sqlcommon.Config, opts ...Option) (*Datastore, error) {

	datastore := &Datastore{
		writeDb:                writeDb,
		logger:                 cfg.Logger,
		maxTuplesPerWriteField: cfg.MaxTuplesPerWriteField,
		maxTypesPerModelField:  cfg.MaxTypesPerModelField,
	}

	for _, opt := range opts {
		opt(datastore)
	}

	collector, err := configureDB(writeDb, cfg)
	if err != nil {
		return nil, fmt.Errorf("configure db: %w", err)
	}
	writeStbl := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).RunWith(writeDb)
	datastore.writeDbInfo = sqlcommon.NewDBInfo(writeDb, writeStbl, HandleSQLError, "postgres")
	datastore.writeStbl = writeStbl
	datastore.writeDbStatsCollector = collector

	// If a readDB is provided, configure it. Otherwise, use the writeDB as the readDB.
	switch {
	case datastore.readDb != nil:
		readCollector, err := configureDB(datastore.readDb, cfg)
		if err != nil {
			return nil, fmt.Errorf("configure db: %w", err)
		}
		readStbl := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).RunWith(datastore.readDb)
		datastore.readDbInfo = sqlcommon.NewDBInfo(datastore.readDb, readStbl, HandleSQLError, "postgres")
		datastore.readStbl = readStbl
		datastore.readDbStatsCollector = readCollector
	default:
		datastore.readDb = writeDb
		datastore.readDbInfo = datastore.writeDbInfo
		datastore.readStbl = writeStbl
		datastore.readDbStatsCollector = collector
	}

	return datastore, nil
}

// Close see [storage.OpenFGADatastore].Close.
func (s *Datastore) Close() {
	if s.writeDbStatsCollector != nil {
		prometheus.Unregister(s.writeDbStatsCollector)
	}
	if s.readDbStatsCollector != nil {
		prometheus.Unregister(s.readDbStatsCollector)
	}
	s.writeDb.Close()
	s.readDb.Close()
}

// getReadStbl returns the appropriate statement builder based on consistency options
func (s *Datastore) getReadStbl(consistency openfgav1.ConsistencyPreference) sq.StatementBuilderType {
	if consistency == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		// If we are using higher consistency, we need to use the write database.
		return s.writeStbl
	}
	// If we are using lower consistency, we can use the read database.
	return s.readStbl
}

// Read see [storage.RelationshipTupleReader].Read.
func (s *Datastore) Read(
	ctx context.Context,
	store string,
	tupleKey *openfgav1.TupleKey,
	options storage.ReadOptions,
) (storage.TupleIterator, error) {
	ctx, span := startTrace(ctx, "Read")
	defer span.End()

	readStbl := s.getReadStbl(options.Consistency.Preference)
	return s.read(ctx, store, tupleKey, nil, readStbl)
}

// ReadPage see [storage.RelationshipTupleReader].ReadPage.
func (s *Datastore) ReadPage(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options storage.ReadPageOptions) ([]*openfgav1.Tuple, string, error) {
	ctx, span := startTrace(ctx, "ReadPage")
	defer span.End()

	readStbl := s.getReadStbl(options.Consistency.Preference)
	iter, err := s.read(ctx, store, tupleKey, &options, readStbl)
	if err != nil {
		return nil, "", err
	}
	defer iter.Stop()

	return iter.ToArray(ctx, options.Pagination)
}

func (s *Datastore) read(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options *storage.ReadPageOptions, readStbl sq.StatementBuilderType) (*sqlcommon.SQLTupleIterator, error) {
	_, span := startTrace(ctx, "read")
	defer span.End()

	sb := readStbl.
		Select(
			"store", "object_type", "object_id", "relation",
			"_user",
			"condition_name", "condition_context", "ulid", "inserted_at",
		).
		From("tuple").
		Where(sq.Eq{"store": store})
	if options != nil {
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
		sb = sb.Where(sq.Eq{"_user": tupleKey.GetUser()})
	}

	if options != nil && options.Pagination.From != "" {
		sb = sb.Where(sq.GtOrEq{"ulid": options.Pagination.From})
	}
	if options != nil && options.Pagination.PageSize != 0 {
		sb = sb.Limit(uint64(options.Pagination.PageSize + 1)) // + 1 is used to determine whether to return a continuation token.
	}

	return sqlcommon.NewSQLTupleIterator(sb, HandleSQLError), nil
}

// Write see [storage.RelationshipTupleWriter].Write.
func (s *Datastore) Write(
	ctx context.Context,
	store string,
	deletes storage.Deletes,
	writes storage.Writes,
) error {
	ctx, span := startTrace(ctx, "Write")
	defer span.End()

	return sqlcommon.Write(ctx, s.writeDbInfo, store, deletes, writes, time.Now().UTC())
}

// ReadUserTuple see [storage.RelationshipTupleReader].ReadUserTuple.
func (s *Datastore) ReadUserTuple(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
	ctx, span := startTrace(ctx, "ReadUserTuple")
	defer span.End()

	readStbl := s.getReadStbl(options.Consistency.Preference)
	objectType, objectID := tupleUtils.SplitObject(tupleKey.GetObject())
	userType := tupleUtils.GetUserTypeFromUser(tupleKey.GetUser())

	var conditionName sql.NullString
	var conditionContext []byte
	var record storage.TupleRecord

	err := readStbl.
		Select(
			"object_type", "object_id", "relation",
			"_user",
			"condition_name", "condition_context",
		).
		From("tuple").
		Where(sq.Eq{
			"store":       store,
			"object_type": objectType,
			"object_id":   objectID,
			"relation":    tupleKey.GetRelation(),
			"_user":       tupleKey.GetUser(),
			"user_type":   userType,
		}).
		QueryRowContext(ctx).
		Scan(
			&record.ObjectType,
			&record.ObjectID,
			&record.Relation,
			&record.User,
			&conditionName,
			&conditionContext,
		)
	if err != nil {
		return nil, HandleSQLError(err)
	}

	if conditionName.String != "" {
		record.ConditionName = conditionName.String

		if conditionContext != nil {
			var conditionContextStruct structpb.Struct
			if err := proto.Unmarshal(conditionContext, &conditionContextStruct); err != nil {
				return nil, err
			}
			record.ConditionContext = &conditionContextStruct
		}
	}

	return record.AsTuple(), nil
}

// ReadUsersetTuples see [storage.RelationshipTupleReader].ReadUsersetTuples.
func (s *Datastore) ReadUsersetTuples(
	ctx context.Context,
	store string,
	filter storage.ReadUsersetTuplesFilter,
	options storage.ReadUsersetTuplesOptions,
) (storage.TupleIterator, error) {
	_, span := startTrace(ctx, "ReadUsersetTuples")
	defer span.End()

	readStbl := s.getReadStbl(options.Consistency.Preference)
	sb := readStbl.
		Select(
			"store", "object_type", "object_id", "relation",
			"_user",
			"condition_name", "condition_context", "ulid", "inserted_at",
		).
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
			if _, ok := userset.GetRelationOrWildcard().(*openfgav1.RelationReference_Relation); ok {
				orConditions = append(orConditions, sq.Like{
					"_user": userset.GetType() + ":%#" + userset.GetRelation(),
				})
			}
			if _, ok := userset.GetRelationOrWildcard().(*openfgav1.RelationReference_Wildcard); ok {
				orConditions = append(orConditions, sq.Eq{
					"_user": userset.GetType() + ":*",
				})
			}
		}
		sb = sb.Where(orConditions)
	}

	return sqlcommon.NewSQLTupleIterator(sb, HandleSQLError), nil
}

// ReadStartingWithUser see [storage.RelationshipTupleReader].ReadStartingWithUser.
func (s *Datastore) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter storage.ReadStartingWithUserFilter,
	options storage.ReadStartingWithUserOptions,
) (storage.TupleIterator, error) {
	_, span := startTrace(ctx, "ReadStartingWithUser")
	defer span.End()

	readStbl := s.getReadStbl(options.Consistency.Preference)
	var targetUsersArg []string
	for _, u := range filter.UserFilter {
		targetUser := u.GetObject()
		if u.GetRelation() != "" {
			targetUser = strings.Join([]string{u.GetObject(), u.GetRelation()}, "#")
		}
		targetUsersArg = append(targetUsersArg, targetUser)
	}

	builder := readStbl.
		Select(
			"store", "object_type", "object_id", "relation",
			"_user",
			"condition_name", "condition_context", "ulid", "inserted_at",
		).
		From("tuple").
		Where(sq.Eq{
			"store":       store,
			"object_type": filter.ObjectType,
			"relation":    filter.Relation,
			"_user":       targetUsersArg,
		}).OrderBy("object_id")

	if filter.ObjectIDs != nil && filter.ObjectIDs.Size() > 0 {
		builder = builder.Where(sq.Eq{"object_id": filter.ObjectIDs.Values()})
	}

	return sqlcommon.NewSQLTupleIterator(builder, HandleSQLError), nil
}

// MaxTuplesPerWrite see [storage.RelationshipTupleWriter].MaxTuplesPerWrite.
func (s *Datastore) MaxTuplesPerWrite() int {
	return s.maxTuplesPerWriteField
}

// ReadAuthorizationModel see [storage.AuthorizationModelReadBackend].ReadAuthorizationModel.
func (s *Datastore) ReadAuthorizationModel(ctx context.Context, store string, modelID string) (*openfgav1.AuthorizationModel, error) {
	ctx, span := startTrace(ctx, "ReadAuthorizationModel")
	defer span.End()

	return sqlcommon.ReadAuthorizationModel(ctx, s.readDbInfo, store, modelID)
}

// ReadAuthorizationModels see [storage.AuthorizationModelReadBackend].ReadAuthorizationModels.
func (s *Datastore) ReadAuthorizationModels(ctx context.Context, store string, options storage.ReadAuthorizationModelsOptions) ([]*openfgav1.AuthorizationModel, string, error) {
	ctx, span := startTrace(ctx, "ReadAuthorizationModels")
	defer span.End()

	sb := s.readStbl.
		Select("authorization_model_id").
		Distinct().
		From("authorization_model").
		Where(sq.Eq{"store": store}).
		OrderBy("authorization_model_id desc")

	if options.Pagination.From != "" {
		sb = sb.Where(sq.LtOrEq{"authorization_model_id": options.Pagination.From})
	}
	if options.Pagination.PageSize > 0 {
		sb = sb.Limit(uint64(options.Pagination.PageSize + 1)) // + 1 is used to determine whether to return a continuation token.
	}

	rows, err := sb.QueryContext(ctx)
	if err != nil {
		return nil, "", HandleSQLError(err)
	}
	defer rows.Close()

	var modelIDs []string
	var modelID string

	for rows.Next() {
		err = rows.Scan(&modelID)
		if err != nil {
			return nil, "", HandleSQLError(err)
		}

		modelIDs = append(modelIDs, modelID)
	}

	if err := rows.Err(); err != nil {
		return nil, "", HandleSQLError(err)
	}

	var token string
	numModelIDs := len(modelIDs)
	if len(modelIDs) > options.Pagination.PageSize {
		numModelIDs = options.Pagination.PageSize
		token = modelID
	}

	// TODO: make this concurrent with a maximum of 5 goroutines. This may be helpful:
	// https://stackoverflow.com/questions/25306073/always-have-x-number-of-goroutines-running-at-any-time
	models := make([]*openfgav1.AuthorizationModel, 0, numModelIDs)
	// We use numModelIDs here to avoid retrieving possibly one extra model.
	for i := 0; i < numModelIDs; i++ {
		model, err := s.ReadAuthorizationModel(ctx, store, modelIDs[i])
		if err != nil {
			return nil, "", err
		}
		models = append(models, model)
	}

	return models, token, nil
}

// FindLatestAuthorizationModel see [storage.AuthorizationModelReadBackend].FindLatestAuthorizationModel.
func (s *Datastore) FindLatestAuthorizationModel(ctx context.Context, store string) (*openfgav1.AuthorizationModel, error) {
	ctx, span := startTrace(ctx, "FindLatestAuthorizationModel")
	defer span.End()

	return sqlcommon.FindLatestAuthorizationModel(ctx, s.readDbInfo, store)
}

// MaxTypesPerAuthorizationModel see [storage.TypeDefinitionWriteBackend].MaxTypesPerAuthorizationModel.
func (s *Datastore) MaxTypesPerAuthorizationModel() int {
	return s.maxTypesPerModelField
}

// WriteAuthorizationModel see [storage.TypeDefinitionWriteBackend].WriteAuthorizationModel.
func (s *Datastore) WriteAuthorizationModel(ctx context.Context, store string, model *openfgav1.AuthorizationModel) error {
	ctx, span := startTrace(ctx, "WriteAuthorizationModel")
	defer span.End()

	return sqlcommon.WriteAuthorizationModel(ctx, s.writeDbInfo, store, model)
}

// CreateStore adds a new store to storage.
func (s *Datastore) CreateStore(ctx context.Context, store *openfgav1.Store) (*openfgav1.Store, error) {
	ctx, span := startTrace(ctx, "CreateStore")
	defer span.End()

	var id, name string
	var createdAt, updatedAt time.Time

	err := s.writeStbl.
		Insert("store").
		Columns("id", "name", "created_at", "updated_at").
		Values(store.GetId(), store.GetName(), sq.Expr("NOW()"), sq.Expr("NOW()")).
		Suffix("returning id, name, created_at, updated_at").
		QueryRowContext(ctx).
		Scan(&id, &name, &createdAt, &updatedAt)
	if err != nil {
		return nil, HandleSQLError(err)
	}

	return &openfgav1.Store{
		Id:        id,
		Name:      name,
		CreatedAt: timestamppb.New(createdAt),
		UpdatedAt: timestamppb.New(updatedAt),
	}, nil
}

// GetStore retrieves the details of a specific store using its storeID.
func (s *Datastore) GetStore(ctx context.Context, id string) (*openfgav1.Store, error) {
	ctx, span := startTrace(ctx, "GetStore")
	defer span.End()

	row := s.readStbl.
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
		return nil, HandleSQLError(err)
	}

	return &openfgav1.Store{
		Id:        storeID,
		Name:      name,
		CreatedAt: timestamppb.New(createdAt),
		UpdatedAt: timestamppb.New(updatedAt),
	}, nil
}

// ListStores provides a paginated list of all stores present in the storage.
func (s *Datastore) ListStores(ctx context.Context, options storage.ListStoresOptions) ([]*openfgav1.Store, string, error) {
	ctx, span := startTrace(ctx, "ListStores")
	defer span.End()

	whereClause := sq.And{
		sq.Eq{"deleted_at": nil},
	}

	if len(options.IDs) > 0 {
		whereClause = append(whereClause, sq.Eq{"id": options.IDs})
	}

	if options.Name != "" {
		whereClause = append(whereClause, sq.Eq{"name": options.Name})
	}

	if options.Pagination.From != "" {
		whereClause = append(whereClause, sq.GtOrEq{"id": options.Pagination.From})
	}

	sb := s.readStbl.
		Select("id", "name", "created_at", "updated_at").
		From("store").
		Where(whereClause).
		OrderBy("id")

	if options.Pagination.PageSize > 0 {
		sb = sb.Limit(uint64(options.Pagination.PageSize + 1)) // + 1 is used to determine whether to return a continuation token.
	}

	rows, err := sb.QueryContext(ctx)
	if err != nil {
		return nil, "", HandleSQLError(err)
	}
	defer rows.Close()

	var stores []*openfgav1.Store
	var id string
	for rows.Next() {
		var name string
		var createdAt, updatedAt time.Time
		err := rows.Scan(&id, &name, &createdAt, &updatedAt)
		if err != nil {
			return nil, "", HandleSQLError(err)
		}

		stores = append(stores, &openfgav1.Store{
			Id:        id,
			Name:      name,
			CreatedAt: timestamppb.New(createdAt),
			UpdatedAt: timestamppb.New(updatedAt),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, "", HandleSQLError(err)
	}

	if len(stores) > options.Pagination.PageSize {
		return stores[:options.Pagination.PageSize], id, nil
	}

	return stores, "", nil
}

// DeleteStore removes a store from storage.
func (s *Datastore) DeleteStore(ctx context.Context, id string) error {
	ctx, span := startTrace(ctx, "DeleteStore")
	defer span.End()

	_, err := s.writeStbl.
		Update("store").
		Set("deleted_at", sq.Expr("NOW()")).
		Where(sq.Eq{"id": id}).
		ExecContext(ctx)
	if err != nil {
		return HandleSQLError(err)
	}

	return nil
}

// WriteAssertions see [storage.AssertionsBackend].WriteAssertions.
func (s *Datastore) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgav1.Assertion) error {
	ctx, span := startTrace(ctx, "WriteAssertions")
	defer span.End()

	marshalledAssertions, err := proto.Marshal(&openfgav1.Assertions{Assertions: assertions})
	if err != nil {
		return err
	}

	_, err = s.writeStbl.
		Insert("assertion").
		Columns("store", "authorization_model_id", "assertions").
		Values(store, modelID, marshalledAssertions).
		Suffix("ON CONFLICT (store, authorization_model_id) DO UPDATE SET assertions = ?", marshalledAssertions).
		ExecContext(ctx)
	if err != nil {
		return HandleSQLError(err)
	}

	return nil
}

// ReadAssertions see [storage.AssertionsBackend].ReadAssertions.
func (s *Datastore) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgav1.Assertion, error) {
	ctx, span := startTrace(ctx, "ReadAssertions")
	defer span.End()

	var marshalledAssertions []byte
	err := s.readStbl.
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
			return []*openfgav1.Assertion{}, nil
		}
		return nil, HandleSQLError(err)
	}

	var assertions openfgav1.Assertions
	err = proto.Unmarshal(marshalledAssertions, &assertions)
	if err != nil {
		return nil, err
	}

	return assertions.GetAssertions(), nil
}

// ReadChanges see [storage.ChangelogBackend].ReadChanges.
func (s *Datastore) ReadChanges(ctx context.Context, store string, filter storage.ReadChangesFilter, options storage.ReadChangesOptions) ([]*openfgav1.TupleChange, string, error) {
	ctx, span := startTrace(ctx, "ReadChanges")
	defer span.End()

	objectTypeFilter := filter.ObjectType
	horizonOffset := filter.HorizonOffset

	orderBy := "ulid asc"
	if options.SortDesc {
		orderBy = "ulid desc"
	}

	sb := s.readStbl.
		Select(
			"ulid", "object_type", "object_id", "relation",
			"_user",
			"operation",
			"condition_name", "condition_context", "inserted_at",
		).
		From("changelog").
		Where(sq.Eq{"store": store}).
		Where(fmt.Sprintf("inserted_at < NOW() - interval '%dms'", horizonOffset.Milliseconds())).
		OrderBy(orderBy)

	if objectTypeFilter != "" {
		sb = sb.Where(sq.Eq{"object_type": objectTypeFilter})
	}
	if options.Pagination.From != "" {
		sb = sqlcommon.AddFromUlid(sb, options.Pagination.From, options.SortDesc)
	}
	if options.Pagination.PageSize > 0 {
		sb = sb.Limit(uint64(options.Pagination.PageSize)) // + 1 is NOT used here as we always return a continuation token.
	}

	rows, err := sb.QueryContext(ctx)
	if err != nil {
		return nil, "", HandleSQLError(err)
	}
	defer rows.Close()

	var changes []*openfgav1.TupleChange
	var ulid string
	for rows.Next() {
		var objectType, objectID, relation, user string
		var operation int
		var insertedAt time.Time
		var conditionName sql.NullString
		var conditionContext []byte

		err = rows.Scan(
			&ulid,
			&objectType,
			&objectID,
			&relation,
			&user,
			&operation,
			&conditionName,
			&conditionContext,
			&insertedAt,
		)
		if err != nil {
			return nil, "", HandleSQLError(err)
		}

		var conditionContextStruct structpb.Struct
		if conditionName.String != "" {
			if conditionContext != nil {
				if err := proto.Unmarshal(conditionContext, &conditionContextStruct); err != nil {
					return nil, "", err
				}
			}
		}

		tk := tupleUtils.NewTupleKeyWithCondition(
			tupleUtils.BuildObject(objectType, objectID),
			relation,
			user,
			conditionName.String,
			&conditionContextStruct,
		)

		changes = append(changes, &openfgav1.TupleChange{
			TupleKey:  tk,
			Operation: openfgav1.TupleOperation(operation),
			Timestamp: timestamppb.New(insertedAt.UTC()),
		})
	}

	if len(changes) == 0 {
		return nil, "", storage.ErrNotFound
	}

	return changes, ulid, nil
}

// IsReady see [sqlcommon.IsReady].
func (s *Datastore) IsReady(ctx context.Context) (storage.ReadinessStatus, error) {
	var writeStatus, readStatus storage.ReadinessStatus
	var err error

	writeStatus, err = sqlcommon.IsReady(ctx, s.writeDb)
	if err != nil {
		writeStatus.Message = err.Error()
		writeStatus.IsReady = false
	}
	readStatus, err = sqlcommon.IsReady(ctx, s.readDb)
	if err != nil {
		readStatus.Message = err.Error()
		readStatus.IsReady = false
	}

	return storage.ReadinessStatus{
		Message: fmt.Sprintf("write: %s, read: %s", writeStatus.Message, readStatus.Message),
		IsReady: writeStatus.IsReady && readStatus.IsReady,
	}, err
}

// HandleSQLError processes an SQL error and converts it into a more
// specific error type based on the nature of the SQL error.
func HandleSQLError(err error, args ...interface{}) error {
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrNotFound
	}

	if strings.Contains(err.Error(), "duplicate key value") {
		if len(args) > 0 {
			if tk, ok := args[0].(*openfgav1.TupleKey); ok {
				return storage.InvalidWriteInputError(tk, openfgav1.TupleOperation_TUPLE_OPERATION_WRITE)
			}
		}
		return storage.ErrCollision
	}

	return fmt.Errorf("sql error: %w", err)
}
