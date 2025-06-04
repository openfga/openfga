package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

var tracer = otel.Tracer("openfga/pkg/storage/sqlite")

func startTrace(ctx context.Context, name string) (context.Context, trace.Span) {
	return tracer.Start(ctx, "sqlite."+name)
}

// Datastore provides a SQLite based implementation of [storage.OpenFGADatastore].
type Datastore struct {
	stbl                   sq.StatementBuilderType
	db                     *sql.DB
	dbInfo                 *sqlcommon.DBInfo
	logger                 logger.Logger
	dbStatsCollector       prometheus.Collector
	maxTuplesPerWriteField int
	maxTypesPerModelField  int
	versionReady           bool
}

// Ensures that SQLite implements the OpenFGADatastore interface.
var _ storage.OpenFGADatastore = (*Datastore)(nil)

// Prepare a raw DSN from config for use with SQLite, specifying defaults for journal mode and busy timeout.
func PrepareDSN(uri string) (string, error) {
	// Set journal mode and busy timeout pragmas if not specified.
	query := url.Values{}
	var err error

	if i := strings.Index(uri, "?"); i != -1 {
		query, err = url.ParseQuery(uri[i+1:])
		if err != nil {
			return uri, fmt.Errorf("error parsing dsn: %w", err)
		}

		uri = uri[:i]
	}

	foundJournalMode := false
	foundBusyTimeout := false
	for _, val := range query["_pragma"] {
		if strings.HasPrefix(val, "journal_mode") {
			foundJournalMode = true
		} else if strings.HasPrefix(val, "busy_timeout") {
			foundBusyTimeout = true
		}
	}

	if !foundJournalMode {
		query.Add("_pragma", "journal_mode(WAL)")
	}
	if !foundBusyTimeout {
		query.Add("_pragma", "busy_timeout(100)")
	}

	// Set transaction mode to immediate if not specified
	if !query.Has("_txlock") {
		query.Set("_txlock", "immediate")
	}

	uri += "?" + query.Encode()

	return uri, nil
}

// New creates a new [Datastore] storage.
func New(uri string, cfg *sqlcommon.Config) (*Datastore, error) {
	uri, err := PrepareDSN(uri)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", uri)
	if err != nil {
		return nil, fmt.Errorf("initialize sqlite connection: %w", err)
	}

	var collector prometheus.Collector
	if cfg.ExportMetrics {
		collector = collectors.NewDBStatsCollector(db, "openfga")
		if err := prometheus.Register(collector); err != nil {
			return nil, fmt.Errorf("initialize metrics: %w", err)
		}
	}

	stbl := sq.StatementBuilder.RunWith(db)
	dbInfo := sqlcommon.NewDBInfo(db, stbl, HandleSQLError, "sqlite")

	return &Datastore{
		stbl:                   stbl,
		db:                     db,
		dbInfo:                 dbInfo,
		logger:                 cfg.Logger,
		dbStatsCollector:       collector,
		maxTuplesPerWriteField: cfg.MaxTuplesPerWriteField,
		maxTypesPerModelField:  cfg.MaxTypesPerModelField,
		versionReady:           false,
	}, nil
}

// Close see [storage.OpenFGADatastore].Close.
func (s *Datastore) Close() {
	if s.dbStatsCollector != nil {
		prometheus.Unregister(s.dbStatsCollector)
	}
	s.db.Close()
}

// Read see [storage.RelationshipTupleReader].Read.
func (s *Datastore) Read(
	ctx context.Context,
	store string,
	tupleKey *openfgav1.TupleKey,
	_ storage.ReadOptions,
) (storage.TupleIterator, error) {
	ctx, span := startTrace(ctx, "Read")
	defer span.End()

	return s.read(ctx, store, tupleKey, nil)
}

// ReadPage see [storage.RelationshipTupleReader].ReadPage.
func (s *Datastore) ReadPage(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options storage.ReadPageOptions) ([]*openfgav1.Tuple, string, error) {
	ctx, span := startTrace(ctx, "ReadPage")
	defer span.End()

	iter, err := s.read(ctx, store, tupleKey, &options)
	if err != nil {
		return nil, "", err
	}
	defer iter.Stop()

	return iter.ToArray(options.Pagination)
}

func (s *Datastore) read(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options *storage.ReadPageOptions) (*SQLTupleIterator, error) {
	ctx, span := startTrace(ctx, "read")
	defer span.End()

	sb := s.stbl.
		Select(
			"store", "object_type", "object_id", "relation",
			"user_object_type", "user_object_id", "user_relation",
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
		userObjectType, userObjectID, userRelation := tupleUtils.ToUserParts(tupleKey.GetUser())
		sb = sb.Where(sq.Eq{
			"user_object_type": userObjectType,
			"user_object_id":   userObjectID,
			"user_relation":    userRelation,
		})
	}
	if options != nil && options.Pagination.From != "" {
		token := options.Pagination.From
		sb = sb.Where(sq.GtOrEq{"ulid": token})
	}
	if options != nil && options.Pagination.PageSize != 0 {
		sb = sb.Limit(uint64(options.Pagination.PageSize + 1)) // + 1 is used to determine whether to return a continuation token.
	}

	rows, err := sb.QueryContext(ctx)
	if err != nil {
		return nil, HandleSQLError(err)
	}

	return NewSQLTupleIterator(rows, HandleSQLError), nil
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

	return s.write(ctx, store, deletes, writes, time.Now().UTC())
}

// Write provides the common method for writing to database across sql storage.
func (s *Datastore) write(
	ctx context.Context,
	store string,
	deletes storage.Deletes,
	writes storage.Writes,
	now time.Time,
) error {
	var txn *sql.Tx
	err := busyRetry(func() error {
		var err error
		txn, err = s.db.BeginTx(ctx, nil)
		return err
	})
	if err != nil {
		return HandleSQLError(err)
	}
	defer func() {
		_ = txn.Rollback()
	}()

	changelogBuilder := s.stbl.
		Insert("changelog").
		Columns(
			"store",
			"object_type",
			"object_id",
			"relation",
			"user_object_type",
			"user_object_id",
			"user_relation",
			"condition_name",
			"condition_context",
			"operation",
			"ulid",
			"inserted_at",
		)

	deleteBuilder := s.stbl.Delete("tuple")

	for _, tk := range deletes {
		id := ulid.MustNew(ulid.Timestamp(now), ulid.DefaultEntropy()).String()
		objectType, objectID := tupleUtils.SplitObject(tk.GetObject())
		userObjectType, userObjectID, userRelation := tupleUtils.ToUserParts(tk.GetUser())

		var res sql.Result
		var err error
		err = busyRetry(func() error {
			res, err = deleteBuilder.
				Where(sq.Eq{
					"store":            store,
					"object_type":      objectType,
					"object_id":        objectID,
					"relation":         tk.GetRelation(),
					"user_object_type": userObjectType,
					"user_object_id":   userObjectID,
					"user_relation":    userRelation,
					"user_type":        tupleUtils.GetUserTypeFromUser(tk.GetUser()),
				}).
				RunWith(txn). // Part of a txn.
				ExecContext(ctx)
			return err
		})
		if err != nil {
			return HandleSQLError(err, tk)
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return HandleSQLError(err)
		}

		if rowsAffected != 1 {
			return storage.InvalidWriteInputError(
				tk,
				openfgav1.TupleOperation_TUPLE_OPERATION_DELETE,
			)
		}

		changelogBuilder = changelogBuilder.Values(
			store,
			objectType,
			objectID,
			tk.GetRelation(),
			userObjectType,
			userObjectID,
			userRelation,
			"",
			nil, // Redact condition info for deletes since we only need the base triplet (object, relation, user).
			openfgav1.TupleOperation_TUPLE_OPERATION_DELETE,
			id,
			sq.Expr("datetime('subsec')"),
		)
	}

	insertBuilder := s.stbl.
		Insert("tuple").
		Columns(
			"store",
			"object_type",
			"object_id",
			"relation",
			"user_object_type",
			"user_object_id",
			"user_relation",
			"user_type",
			"condition_name",
			"condition_context",
			"ulid",
			"inserted_at",
		)

	for _, tk := range writes {
		id := ulid.MustNew(ulid.Timestamp(now), ulid.DefaultEntropy()).String()
		objectType, objectID := tupleUtils.SplitObject(tk.GetObject())
		userObjectType, userObjectID, userRelation := tupleUtils.ToUserParts(tk.GetUser())

		conditionName, conditionContext, err := sqlcommon.MarshalRelationshipCondition(tk.GetCondition())
		if err != nil {
			return err
		}

		err = busyRetry(func() error {
			_, err = insertBuilder.
				Values(
					store,
					objectType,
					objectID,
					tk.GetRelation(),
					userObjectType,
					userObjectID,
					userRelation,
					tupleUtils.GetUserTypeFromUser(tk.GetUser()),
					conditionName,
					conditionContext,
					id,
					sq.Expr("datetime('subsec')"),
				).
				RunWith(txn). // Part of a txn.
				ExecContext(ctx)
			return err
		})
		if err != nil {
			return HandleSQLError(err, tk)
		}

		changelogBuilder = changelogBuilder.Values(
			store,
			objectType,
			objectID,
			tk.GetRelation(),
			userObjectType,
			userObjectID,
			userRelation,
			conditionName,
			conditionContext,
			openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
			id,
			sq.Expr("datetime('subsec')"),
		)
	}

	if len(writes) > 0 || len(deletes) > 0 {
		err := busyRetry(func() error {
			_, err := changelogBuilder.RunWith(txn).ExecContext(ctx) // Part of a txn.
			return err
		})
		if err != nil {
			return HandleSQLError(err)
		}
	}

	err = busyRetry(func() error {
		return txn.Commit()
	})
	if err != nil {
		return HandleSQLError(err)
	}

	return nil
}

// ReadUserTuple see [storage.RelationshipTupleReader].ReadUserTuple.
func (s *Datastore) ReadUserTuple(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, _ storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
	ctx, span := startTrace(ctx, "ReadUserTuple")
	defer span.End()

	objectType, objectID := tupleUtils.SplitObject(tupleKey.GetObject())
	userType := tupleUtils.GetUserTypeFromUser(tupleKey.GetUser())
	userObjectType, userObjectID, userRelation := tupleUtils.ToUserParts(tupleKey.GetUser())

	var conditionName sql.NullString
	var conditionContext []byte
	var record storage.TupleRecord

	err := s.stbl.
		Select(
			"object_type", "object_id", "relation",
			"user_object_type", "user_object_id", "user_relation",
			"condition_name", "condition_context",
		).
		From("tuple").
		Where(sq.Eq{
			"store":            store,
			"object_type":      objectType,
			"object_id":        objectID,
			"relation":         tupleKey.GetRelation(),
			"user_object_type": userObjectType,
			"user_object_id":   userObjectID,
			"user_relation":    userRelation,
			"user_type":        userType,
		}).
		QueryRowContext(ctx).
		Scan(
			&record.ObjectType,
			&record.ObjectID,
			&record.Relation,
			&record.UserObjectType,
			&record.UserObjectID,
			&record.UserRelation,
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
	_ storage.ReadUsersetTuplesOptions,
) (storage.TupleIterator, error) {
	ctx, span := startTrace(ctx, "ReadUsersetTuples")
	defer span.End()

	sb := s.stbl.
		Select(
			"store", "object_type", "object_id", "relation",
			"user_object_type", "user_object_id", "user_relation",
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
				orConditions = append(orConditions, sq.Eq{
					"user_object_type": userset.GetType(),
					"user_relation":    userset.GetRelation(),
				})
			}
			if _, ok := userset.GetRelationOrWildcard().(*openfgav1.RelationReference_Wildcard); ok {
				orConditions = append(orConditions, sq.Eq{
					"user_object_type": userset.GetType(),
					"user_object_id":   "*",
				})
			}
		}
		sb = sb.Where(orConditions)
	}
	rows, err := sb.QueryContext(ctx)
	if err != nil {
		return nil, HandleSQLError(err)
	}

	return NewSQLTupleIterator(rows, HandleSQLError), nil
}

// ReadStartingWithUser see [storage.RelationshipTupleReader].ReadStartingWithUser.
func (s *Datastore) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter storage.ReadStartingWithUserFilter,
	_ storage.ReadStartingWithUserOptions,
) (storage.TupleIterator, error) {
	ctx, span := startTrace(ctx, "ReadStartingWithUser")
	defer span.End()

	var targetUsersArg sq.Or
	for _, u := range filter.UserFilter {
		userObjectType, userObjectID, userRelation := tupleUtils.ToUserPartsFromObjectRelation(u)
		targetUser := sq.Eq{
			"user_object_type": userObjectType,
			"user_object_id":   userObjectID,
		}
		if userRelation != "" {
			targetUser["user_relation"] = userRelation
		}
		targetUsersArg = append(targetUsersArg, targetUser)
	}

	builder := s.stbl.
		Select(
			"store", "object_type", "object_id", "relation",
			"user_object_type", "user_object_id", "user_relation",
			"condition_name", "condition_context", "ulid", "inserted_at",
		).
		From("tuple").
		Where(sq.Eq{
			"store":       store,
			"object_type": filter.ObjectType,
			"relation":    filter.Relation,
		}).
		Where(targetUsersArg).OrderBy("object_id")

	if filter.ObjectIDs != nil && filter.ObjectIDs.Size() > 0 {
		builder = builder.Where(sq.Eq{"object_id": filter.ObjectIDs.Values()})
	}

	rows, err := builder.QueryContext(ctx)
	if err != nil {
		return nil, HandleSQLError(err)
	}

	return NewSQLTupleIterator(rows, HandleSQLError), nil
}

// MaxTuplesPerWrite see [storage.RelationshipTupleWriter].MaxTuplesPerWrite.
func (s *Datastore) MaxTuplesPerWrite() int {
	return s.maxTuplesPerWriteField
}

func constructAuthorizationModelFromSQLRows(rows *sql.Rows) (*openfgav1.AuthorizationModel, error) {
	if rows.Next() {
		var modelID string
		var schemaVersion string
		var marshalledModel []byte

		err := rows.Scan(&modelID, &schemaVersion, &marshalledModel)
		if err != nil {
			return nil, HandleSQLError(err)
		}

		var model openfgav1.AuthorizationModel
		if err := proto.Unmarshal(marshalledModel, &model); err != nil {
			return nil, err
		}

		return &model, nil
	}

	if err := rows.Err(); err != nil {
		return nil, HandleSQLError(err)
	}

	return nil, storage.ErrNotFound
}

// ReadAuthorizationModel see [storage.AuthorizationModelReadBackend].ReadAuthorizationModel.
func (s *Datastore) ReadAuthorizationModel(ctx context.Context, store string, modelID string) (*openfgav1.AuthorizationModel, error) {
	ctx, span := startTrace(ctx, "ReadAuthorizationModel")
	defer span.End()

	rows, err := s.stbl.
		Select("authorization_model_id", "schema_version", "serialized_protobuf").
		From("authorization_model").
		Where(sq.Eq{
			"store":                  store,
			"authorization_model_id": modelID,
		}).
		QueryContext(ctx)
	if err != nil {
		return nil, HandleSQLError(err)
	}
	defer rows.Close()

	return constructAuthorizationModelFromSQLRows(rows)
}

// ReadAuthorizationModels see [storage.AuthorizationModelReadBackend].ReadAuthorizationModels.
func (s *Datastore) ReadAuthorizationModels(ctx context.Context, store string, options storage.ReadAuthorizationModelsOptions) ([]*openfgav1.AuthorizationModel, string, error) {
	ctx, span := startTrace(ctx, "ReadAuthorizationModels")
	defer span.End()

	sb := s.stbl.
		Select("authorization_model_id", "schema_version", "serialized_protobuf").
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

	var modelID string
	var schemaVersion string
	var marshalledModel []byte

	models := make([]*openfgav1.AuthorizationModel, 0, options.Pagination.PageSize)
	var token string

	for rows.Next() {
		err = rows.Scan(&modelID, &schemaVersion, &marshalledModel)
		if err != nil {
			return nil, "", HandleSQLError(err)
		}

		if options.Pagination.PageSize > 0 && len(models) >= options.Pagination.PageSize {
			return models, modelID, nil
		}

		var model openfgav1.AuthorizationModel
		if err := proto.Unmarshal(marshalledModel, &model); err != nil {
			return nil, "", err
		}

		models = append(models, &model)
	}

	if err := rows.Err(); err != nil {
		return nil, "", HandleSQLError(err)
	}

	return models, token, nil
}

// FindLatestAuthorizationModel see [storage.AuthorizationModelReadBackend].FindLatestAuthorizationModel.
func (s *Datastore) FindLatestAuthorizationModel(ctx context.Context, store string) (*openfgav1.AuthorizationModel, error) {
	ctx, span := startTrace(ctx, "FindLatestAuthorizationModel")
	defer span.End()

	rows, err := s.stbl.
		Select("authorization_model_id", "schema_version", "serialized_protobuf").
		From("authorization_model").
		Where(sq.Eq{"store": store}).
		OrderBy("authorization_model_id desc").
		Limit(1).
		QueryContext(ctx)
	if err != nil {
		return nil, HandleSQLError(err)
	}
	defer rows.Close()

	return constructAuthorizationModelFromSQLRows(rows)
}

// MaxTypesPerAuthorizationModel see [storage.TypeDefinitionWriteBackend].MaxTypesPerAuthorizationModel.
func (s *Datastore) MaxTypesPerAuthorizationModel() int {
	return s.maxTypesPerModelField
}

// WriteAuthorizationModel see [storage.TypeDefinitionWriteBackend].WriteAuthorizationModel.
func (s *Datastore) WriteAuthorizationModel(ctx context.Context, store string, model *openfgav1.AuthorizationModel) error {
	ctx, span := startTrace(ctx, "WriteAuthorizationModel")
	defer span.End()

	schemaVersion := model.GetSchemaVersion()
	typeDefinitions := model.GetTypeDefinitions()

	if len(typeDefinitions) < 1 {
		return nil
	}

	pbdata, err := proto.Marshal(model)
	if err != nil {
		return err
	}

	err = busyRetry(func() error {
		_, err := s.stbl.
			Insert("authorization_model").
			Columns("store", "authorization_model_id", "schema_version", "serialized_protobuf").
			Values(store, model.GetId(), schemaVersion, pbdata).
			ExecContext(ctx)
		return err
	})
	if err != nil {
		return HandleSQLError(err)
	}

	return nil
}

// CreateStore adds a new store to storage.
func (s *Datastore) CreateStore(ctx context.Context, store *openfgav1.Store) (*openfgav1.Store, error) {
	ctx, span := startTrace(ctx, "CreateStore")
	defer span.End()

	var id, name string
	var createdAt, updatedAt time.Time

	err := busyRetry(func() error {
		return s.stbl.
			Insert("store").
			Columns("id", "name", "created_at", "updated_at").
			Values(store.GetId(), store.GetName(), sq.Expr("datetime('subsec')"), sq.Expr("datetime('subsec')")).
			Suffix("returning id, name, created_at, updated_at").
			QueryRowContext(ctx).
			Scan(&id, &name, &createdAt, &updatedAt)
	})
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

	row := s.stbl.
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

	sb := s.stbl.
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

	_, err := s.stbl.
		Update("store").
		Set("deleted_at", sq.Expr("datetime('subsec')")).
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

	err = busyRetry(func() error {
		_, err := s.stbl.
			Insert("assertion").
			Columns("store", "authorization_model_id", "assertions").
			Values(store, modelID, marshalledAssertions).
			Suffix("ON CONFLICT (store, authorization_model_id) DO UPDATE SET assertions = ?", marshalledAssertions).
			ExecContext(ctx)
		return err
	})
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
	err := s.stbl.
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

	sb := s.stbl.
		Select(
			"ulid", "object_type", "object_id", "relation",
			"user_object_type", "user_object_id", "user_relation",
			"operation",
			"condition_name", "condition_context", "inserted_at",
		).
		From("changelog").
		Where(sq.Eq{"store": store}).
		Where(fmt.Sprintf("inserted_at <= datetime('subsec','-%f seconds')", horizonOffset.Seconds())).
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
		var objectType, objectID, relation, userObjectType, userObjectID, userRelation string
		var operation int
		var insertedAt time.Time
		var conditionName sql.NullString
		var conditionContext []byte

		err = rows.Scan(
			&ulid,
			&objectType,
			&objectID,
			&relation,
			&userObjectType,
			&userObjectID,
			&userRelation,
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
			tupleUtils.FromUserParts(userObjectType, userObjectID, userRelation),
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
	versionReady, err := sqlcommon.IsReady(ctx, s.versionReady, s.db)
	if err != nil {
		return versionReady, err
	}
	s.versionReady = versionReady.IsReady
	return versionReady, nil
}

// HandleSQLError processes an SQL error and converts it into a more
// specific error type based on the nature of the SQL error.
func HandleSQLError(err error, args ...interface{}) error {
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrNotFound
	}

	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) {
		if sqliteErr.Code()&0xFF == sqlite3.SQLITE_CONSTRAINT {
			if len(args) > 0 {
				if tk, ok := args[0].(*openfgav1.TupleKey); ok {
					return storage.InvalidWriteInputError(tk, openfgav1.TupleOperation_TUPLE_OPERATION_WRITE)
				}
			}
			return storage.ErrCollision
		}
	}

	return fmt.Errorf("sql error: %w", err)
}

// SQLite will return an SQLITE_BUSY error when the database is locked rather than waiting for the lock.
// This function retries the operation up to maxRetries times before returning the error.
func busyRetry(fn func() error) error {
	const maxRetries = 10
	for retries := 0; ; retries++ {
		err := fn()
		if err == nil {
			return nil
		}

		if isBusyError(err) {
			if retries < maxRetries {
				continue
			}

			return fmt.Errorf("sqlite busy error after %d retries: %w", maxRetries, err)
		}

		return err
	}
}

var busyErrors = map[int]struct{}{
	sqlite3.SQLITE_BUSY_RECOVERY:      {},
	sqlite3.SQLITE_BUSY_SNAPSHOT:      {},
	sqlite3.SQLITE_BUSY_TIMEOUT:       {},
	sqlite3.SQLITE_BUSY:               {},
	sqlite3.SQLITE_LOCKED_SHAREDCACHE: {},
	sqlite3.SQLITE_LOCKED:             {},
}

func isBusyError(err error) bool {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}

	_, ok := busyErrors[sqliteErr.Code()]
	return ok
}
