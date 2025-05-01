package crdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	sq "github.com/Masterminds/squirrel"
	"github.com/cenkalti/backoff/v4"
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver.
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

var tracer = otel.Tracer("openfga/pkg/storage/crdb")

func startTrace(ctx context.Context, name string) (context.Context, trace.Span) {
	return tracer.Start(ctx, "crdb."+name)
}

// Datastore provides a PostgreSQL based implementation of [storage.OpenFGADatastore].
type Datastore struct {
	stbl   sq.StatementBuilderType
	db     *pgxpool.Pool
	logger logger.Logger
	//dbStatsCollector       prometheus.Collector
	maxTuplesPerWriteField int
	maxTypesPerModelField  int
	regionalByRow          bool
	staleReads             bool
}

type CrdbConfig struct {
	cfg           *sqlcommon.Config
	regionalByRow bool // true when table is regional by row
	staleReads    bool // false when we are running tests for strong consistency, true for staleReads
}

func POCNew(url string, config *sqlcommon.Config, regionalByRow bool, staleReads bool) (*Datastore, error) {
	return New(url, &CrdbConfig{
		cfg: &sqlcommon.Config{
			MaxOpenConns:           config.MaxOpenConns,
			Logger:                 config.Logger,
			ConnMaxLifetime:        config.ConnMaxIdleTime,
			ConnMaxIdleTime:        config.ConnMaxIdleTime,
			MaxTuplesPerWriteField: config.MaxTuplesPerWriteField,
			MaxTypesPerModelField:  config.MaxTypesPerModelField,
		},
		regionalByRow: regionalByRow,
		staleReads:    staleReads,
	})
}

func NewConfig(cfg *sqlcommon.Config, regionalByRow bool, staleReads bool) *CrdbConfig {
	crdbfg := &CrdbConfig{}
	crdbfg.cfg = cfg
	crdbfg.regionalByRow = regionalByRow
	crdbfg.staleReads = staleReads
	return crdbfg
}

// Ensures that Datastore implements the OpenFGADatastore interface.
var _ storage.OpenFGADatastore = (*Datastore)(nil)

// New creates a new [Datastore] storage.
func New(uri string, crdbfg *CrdbConfig) (*Datastore, error) {
	c, err := pgxpool.ParseConfig(uri)
	if err != nil {
		return nil, err
	}
	if len(crdbfg.cfg.Username) > 0 {
		c.ConnConfig.User = crdbfg.cfg.Username
	}
	if len(crdbfg.cfg.Password) > 0 {
		c.ConnConfig.Password = crdbfg.cfg.Password
	}
	c.MaxConnLifetime = 600      // 10 min//crdbfg.cfg.ConnMaxLifetime
	c.MaxConnIdleTime = 600      //same as max connection //crdbfg.cfg.ConnMaxIdleTime
	c.MaxConns = 40              // int32(crdbfg.cfg.MaxOpenConns)
	c.MinConns = 40              // configuring max = min int32(crdb.cfg)
	c.MinIdleConns = 40          // configuring to be the same as min connection
	c.MaxConnLifetimeJitter = 60 // configured to be 10% of the max lifetime

	db, err := pgxpool.NewWithConfig(context.Background(), c)
	if err != nil {
		return nil, fmt.Errorf("initialize crdb connection: %w", err)
	}

	return NewWithDB(db, crdbfg)
}

// NewWithDB creates a new [Datastore] storage with the provided database connection.
func NewWithDB(db *pgxpool.Pool, crdbfg *CrdbConfig) (*Datastore, error) {
	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = 1 * time.Minute
	attempt := 1
	err := backoff.Retry(func() error {
		err := db.Ping(context.Background())
		if err != nil {
			crdbfg.cfg.Logger.Info("waiting for database", zap.Int("attempt", attempt))
			attempt++
			return err
		}
		return nil
	}, policy)
	if err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}

	if crdbfg.cfg.MaxTuplesPerWriteField <= 0 {
		crdbfg.cfg.MaxTuplesPerWriteField = 40
	}

	if crdbfg.cfg.MaxTypesPerModelField <= 0 {
		crdbfg.cfg.MaxTypesPerModelField = 80
	}

	/*
		Needs support for native pgx
		var collector prometheus.Collector
		if cfg.ExportMetrics {
			collector = collectors.NewDBStatsCollector(db, "openfga")
			if err := prometheus.Register(collector); err != nil {
				return nil, fmt.Errorf("initialize metrics: %w", err)
			}
		}
	*/

	stbl := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	return &Datastore{
		stbl:                   stbl,
		db:                     db,
		logger:                 crdbfg.cfg.Logger,
		maxTuplesPerWriteField: crdbfg.cfg.MaxTuplesPerWriteField,
		maxTypesPerModelField:  crdbfg.cfg.MaxTypesPerModelField,
		regionalByRow:          crdbfg.regionalByRow,
		staleReads:             crdbfg.staleReads,
	}, nil
}

// Close see [storage.OpenFGADatastore].Close.
func (s *Datastore) Close() {
	/*
		if s.dbStatsCollector != nil {
			prometheus.Unregister(s.dbStatsCollector)
		}
	*/
	s.db.Close()
}

// Read see [storage.RelationshipTupleReader].Read.
func (s *Datastore) Read(
	ctx context.Context,
	store string,
	tupleKey *openfgav1.TupleKey,
	_ storage.ReadOptions,
) (storage.TupleIterator, error) {
	s.logger.Error("in the read 1")
	s.logger.Error(fmt.Sprint("s.regionalByRow: %v", s.regionalByRow))
	ctx, span := startTrace(ctx, "Read")
	defer span.End()

	return s.read(ctx, store, tupleKey, nil)
}

// ReadPage see [storage.RelationshipTupleReader].ReadPage.
func (s *Datastore) ReadPage(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options storage.ReadPageOptions) ([]*openfgav1.Tuple, string, error) {
	s.logger.Error("in ReadPage.")
	ctx, span := startTrace(ctx, "ReadPage")
	defer span.End()

	iter, err := s.read(ctx, store, tupleKey, &options)
	if err != nil {
		return nil, "", err
	}
	defer iter.Stop()

	// TODO FIX the ToArray
	return iter.ToArray(options.Pagination)
}

func (s *Datastore) read(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options *storage.ReadPageOptions) (*CRDBTupleIterator, error) {
	s.logger.Error("in read 2.")
	ctx, span := startTrace(ctx, "read")
	defer span.End()

	sb := s.stbl.
		Select(
			// why do we get store back each time?
			// why ulid?
			"store", "object_type", "object_id", "relation",
			"user_object_type", "user_object_id", "user_relation",
			"condition_name", "condition_context", "ulid",
		).
		From("tuple").
		Where(sq.Eq{"store": store})

	if s.regionalByRow {
		s.logger.Error("inside the s.regionalByRow if statement.")
		sb = sb.Where(sq.Eq{"crdb_region": "aws-us-east-1"})
	}
	// how does this even make sense, if there are options, automatically sort by ulid?
	// this is extremely expensive, the order happens in memory instead of using the data ordered by CRDB in the index
	if options != nil {
		if s.regionalByRow {
			sb = sb.OrderBy("crdb_region", "store, object_type, object_id, relation, is_userset, user_object_type, user_relation, user_object_id")
		} else {
			sb = sb.OrderBy("store, object_type, object_id, relation, is_userset, user_object_type, user_relation, user_object_id")
		}
	}

	objectType, objectID := tupleUtils.SplitObject(tupleKey.GetObject())

	s.logger.Error("objec type: " + objectType + "OB ID: " + objectID + "relation: " + tupleKey.GetRelation())

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
		userObject, userRelation := tupleUtils.SplitObjectRelation(tupleKey.GetUser())
		userObjectType, userObjectID := tupleUtils.SplitObject(userObject)
		isUserset := userRelation == ""

		s.logger.Error("objec type: " + objectType + "OB ID: " + objectID + "relation: " + tupleKey.GetRelation() + "user Object type: " + userObjectType + "user object id: " + userObjectID)

		if userObjectType != "" {
			sb = sb.Where(sq.Eq{"user_object_type": userObjectType})
		}
		if userObjectID != "" {
			sb = sb.Where(sq.Eq{"user_object_id": userObjectID})
		}
		sb = sb.Where(sq.Eq{"is_userset": isUserset})
	}

	// this makes absolutely no sense to me, the pagination should just be the last tuple read
	// and then continue with subsequent matches. ULID doesn't give you anything concrete to go on (even for Read ALL Tuples)
	// ulid is not part of the key for search, it will required to change this to be the last tuple
	if options != nil && options.Pagination.From != "" {
		if s.regionalByRow {
			sb = sb.Where(sq.Gt{"crdb_region, store, object_type, object_id, relation, is_userset, user_object_type, user_relation, user_object_id": options.Pagination.From})
		} else {
			sb = sb.Where(sq.Gt{"store, object_type, object_id, relation, is_userset, user_object_type, user_relation, user_object_id": options.Pagination.From})
		}
	}
	if options != nil && options.Pagination.PageSize != 0 {
		sb = sb.Limit(uint64(options.Pagination.PageSize + 1)) // + 1 is used to determine whether to return a continuation token.
	}

	stmt, args, err := sb.ToSql()
	s.logger.Error("the stmt is: " + stmt)
	for i := 0; i < len(args); i++ {
		s.logger.Error(fmt.Sprint("type %T with value %v: ", args[i]))
	}
	if err != nil {
		return nil, HandleSQLError(err)
	}

	if s.staleReads {
		stmt = stmt + " AS OF SYSTEM TIME follower_read_timestamp()"
	}

	rows, err := s.db.Query(ctx, stmt, args...)
	if err != nil {
		return nil, HandleSQLError(err)
	}

	defer rows.Close()

	return NewCRDBTupleIterator(rows, s.logger), nil
}

// Write see [storage.RelationshipTupleWriter].Write.
func (s *Datastore) Write(
	ctx context.Context,
	store string,
	deletes storage.Deletes,
	writes storage.Writes,
) error {
	s.logger.Error("starting the write method.")
	ctx, span := startTrace(ctx, "Write")
	defer span.End()

	if len(writes) == 0 && len(deletes) == 0 {
		return nil
	}

	txn, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return HandleSQLError(err)
	}

	changelogInserts := s.stbl.Insert("changelog")
	if s.regionalByRow {
		changelogInserts = changelogInserts.Columns(
			"crdb_region", "store", "object_type", "object_id", "relation", "user_object_type", "user_object_id",
			"user_relation", "condition_name", "condition_context", "operation", "ulid", "inserted_at",
		)
	} else {
		changelogInserts = changelogInserts.Columns(
			"store", "object_type", "object_id", "relation", "user_object_type", "user_object_id",
			"user_relation", "condition_name", "condition_context", "operation", "ulid", "inserted_at",
		)
	}

	defer func() {
		_ = txn.Rollback(ctx)
	}()

	changelogInserts, err = s.deleteTuples(ctx, store, deletes, txn, changelogInserts, time.Now())
	if err != nil {
		fmt.Println("Error deleting tuples:", err)
		return err
	}

	changelogInserts, err = s.insertTuples(ctx, store, writes, txn, changelogInserts, time.Now())
	if err != nil {
		fmt.Println("Error inserting tuples:", err)
		return err
	}

	// Prepare the insert query
	query, args, err := changelogInserts.ToSql()

	if err != nil {
		return HandleSQLError(err)
	}
	_, err = txn.Exec(ctx, query, args...)
	if err != nil {
		return HandleSQLError(err)
	}

	if err := txn.Commit(ctx); err != nil {
		fmt.Println("Error committing the transaction:", err)
		return HandleSQLError(err)
	}

	return nil
}

func (s *Datastore) deleteTuples(ctx context.Context, store string,
	deletes storage.Deletes, txn pgx.Tx, changelogInserts sq.InsertBuilder, now time.Time) (sq.InsertBuilder, error) {

	batch := &pgx.Batch{}

	for _, tk := range deletes {
		id := ulid.MustNew(ulid.Timestamp(now), ulid.DefaultEntropy()).String()
		objectType, objectID := tupleUtils.SplitObject(tk.GetObject())
		userObject, userRelation := tupleUtils.SplitObjectRelation(tk.GetUser())
		userObjectType, userObjectID := tupleUtils.SplitObject(userObject)

		deleteTupleBuilder := s.stbl.Delete("tuple").
			Where(sq.Eq{
				"store":            store,
				"object_type":      objectType,
				"object_id":        objectID,
				"relation":         tk.GetRelation(),
				"user_object_type": userObjectType,
				"user_object_id":   userObjectID,
				"user_relation":    userRelation,
				"is_userset":       userRelation != "",
			})

		if s.regionalByRow {
			deleteTupleBuilder = deleteTupleBuilder.Where(sq.Eq{"crdb_region": "aws-us-east-1"})
		}
		query, args, err := deleteTupleBuilder.ToSql()
		if err != nil {
			return changelogInserts, HandleSQLError(err, tk)
		}

		// Add the delete query to the batch
		batch.Queue(query, args...)

		if s.regionalByRow {
			changelogInserts = changelogInserts.Values("aws-us-east-1", store, objectType, objectID, tk.GetRelation(), userObjectType, userObjectID,
				userRelation, "", nil, "delete", id, sq.Expr("NOW()"))
		} else {
			changelogInserts = changelogInserts.Values(store, objectType, objectID, tk.GetRelation(), userObjectType, userObjectID,
				userRelation, "", nil, "delete", id, sq.Expr("NOW()"))
		}
	}

	// Send the batch
	br := txn.SendBatch(ctx, batch)

	// Process the results of the batch
	for i := 0; i < len(deletes); i++ {
		ct, err := br.Exec()
		if err != nil {
			return changelogInserts, HandleSQLError(err)
		}

		rowsAffected := ct.RowsAffected()
		if rowsAffected != 1 {
			return changelogInserts, storage.InvalidWriteInputError(
				deletes[i],
				openfgav1.TupleOperation_TUPLE_OPERATION_DELETE,
			)
		}
	}

	err := br.Close()
	if err != nil {
		return changelogInserts, HandleSQLError(err)
	}

	return changelogInserts, nil
}

func (s *Datastore) insertTuples(ctx context.Context, store string,
	inserts storage.Writes, txn pgx.Tx, changelogInserts sq.InsertBuilder, now time.Time) (sq.InsertBuilder, error) {

	insertBuilder := s.stbl.Insert("tuple")
	if s.regionalByRow {
		insertBuilder = insertBuilder.Columns(
			"crdb_region", "store", "object_type", "object_id", "relation", "user_object_type", "user_object_id",
			"user_relation", "is_userset", "condition_name", "condition_context", "ulid", "inserted_at",
		)
	} else {
		insertBuilder = insertBuilder.Columns(
			"store", "object_type", "object_id", "relation", "user_object_type", "user_object_id",
			"user_relation", "is_userset", "condition_name", "condition_context", "ulid", "inserted_at",
		)
	}

	for _, tk := range inserts {
		id := ulid.MustNew(ulid.Timestamp(now), ulid.DefaultEntropy()).String()
		objectType, objectID := tupleUtils.SplitObject(tk.GetObject())
		userObject, userRelation := tupleUtils.SplitObjectRelation(tk.GetUser())
		userObjectType, userObjectID := tupleUtils.SplitObject(userObject)

		conditionName, conditionContext, err := sqlcommon.MarshalRelationshipCondition(tk.GetCondition())
		if err != nil {
			return changelogInserts, err
		}

		if s.regionalByRow {
			insertBuilder = insertBuilder.Values("aws-us-east-1", store, objectType, objectID, tk.GetRelation(), userObjectType, userObjectID,
				userRelation, userRelation != "", conditionName, conditionContext, id, sq.Expr("NOW()"))
			changelogInserts = changelogInserts.Values("aws-us-east-1", store, objectType, objectID, tk.GetRelation(), userObjectType, userObjectID,
				userRelation, conditionName, conditionContext, "write", id, sq.Expr("NOW()"))
		} else {
			insertBuilder = insertBuilder.Values(store, objectType, objectID, tk.GetRelation(), userObjectType, userObjectID,
				userRelation, userRelation != "", conditionName, conditionContext, id, sq.Expr("NOW()"))
			changelogInserts = changelogInserts.Values(store, objectType, objectID, tk.GetRelation(), userObjectType, userObjectID,
				userRelation, conditionName, conditionContext, "write", id, sq.Expr("NOW()"))
		}
	}

	// Prepare the insert query
	query, args, err := insertBuilder.ToSql()
	s.logger.Error("query is: " + query)
	s.logger.Error(strconv.Itoa(len(args)))

	if err != nil {
		return changelogInserts, HandleSQLError(err)
	}
	_, err = txn.Exec(ctx, query, args...)
	if err != nil {
		return changelogInserts, HandleSQLError(err)
	}
	return changelogInserts, nil

}

// ReadUserTuple see [storage.RelationshipTupleReader].ReadUserTuple.
func (s *Datastore) ReadUserTuple(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, _ storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
	s.logger.Error("in ReadUserTuple")
	ctx, span := startTrace(ctx, "ReadUserTuple")
	defer span.End()

	objectType, objectID := tupleUtils.SplitObject(tupleKey.GetObject())

	userObject, userRelation := tupleUtils.SplitObjectRelation(tupleKey.GetUser())
	userObjectType, userObjectID := tupleUtils.SplitObject(userObject)

	s.logger.Error("objec type: " + objectType + "OB ID: " + objectID + "relation: " + tupleKey.GetRelation() + "user Object type: " + userObjectType + "user object id: " + userObjectID)

	var conditionName sql.NullString
	var conditionContext []byte
	var record storage.TupleRecord
	isUserset := userRelation != ""

	stbl := s.stbl.
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
			"is_userset":       isUserset,
		})

	if s.regionalByRow {
		stbl = stbl.Where(sq.Eq{"crdb_region": "aws-us-east-1"})
	}

	stmt, args, err := stbl.ToSql()
	if err != nil {
		return nil, HandleSQLError(err)
	}

	if s.staleReads {
		stmt = stmt + " AS OF SYSTEM TIME follower_read_timestamp()"
	}

	row := s.db.QueryRow(ctx, stmt, args...)

	err = row.Scan(
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
	s.logger.Error("in ReadUsersetTuples")
	ctx, span := startTrace(ctx, "ReadUsersetTuples")
	defer span.End()

	sb := s.stbl.
		Select(
			"store", "object_type", "object_id", "relation",
			"user_object_type", "user_object_id", "user_relation",
			"condition_name", "condition_context",
		).
		From("tuple").
		Where(sq.Eq{"store": store}).
		Where(sq.Eq{"is_userset": true})

	if s.regionalByRow {
		sb = sb.Where(sq.Eq{"crdb_region": "aws-us-east-1"})
	}

	objectType, objectID := tupleUtils.SplitObject(filter.Object)
	s.logger.Error("objec type: " + objectType + "OB ID: " + objectID + "relation: " + filter.Relation)

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
					"user_relation":    userset.GetRelation(),
					"user_object_type": userset.GetType(),
				})
			}
			if _, ok := userset.GetRelationOrWildcard().(*openfgav1.RelationReference_Wildcard); ok {
				orConditions = append(orConditions, sq.Eq{
					"user_object_type": userset.GetType(),
					"user_object_id":   tupleUtils.Wildcard,
				})
			}
		}
		sb = sb.Where(orConditions)
	}
	stmt, args, err := sb.ToSql()
	if err != nil {
		return nil, HandleSQLError(err)
	}

	if s.staleReads {
		stmt = stmt + " AS OF SYSTEM TIME follower_read_timestamp()"
	}

	rows, err := s.db.Query(ctx, stmt, args...)

	if err != nil {
		return nil, HandleSQLError(err)
	}

	return NewCRDBTupleIterator(rows, s.logger), nil
}

// ReadStartingWithUser see [storage.RelationshipTupleReader].ReadStartingWithUser.
func (s *Datastore) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter storage.ReadStartingWithUserFilter,
	_ storage.ReadStartingWithUserOptions,
) (storage.TupleIterator, error) {
	s.logger.Error("in the ReadStartingWithUser.")
	ctx, span := startTrace(ctx, "ReadStartingWithUser")
	defer span.End()

	userFilter := sq.Or{}

	for _, u := range filter.UserFilter {
		objectType, objectID := tupleUtils.SplitObject(u.GetObject())
		relation := u.GetRelation()
		isUserset := relation != ""

		s.logger.Error("objec type: " + objectType + "OB ID: " + objectID + "relation: " + relation)

		userFilter = append(userFilter, sq.Eq{
			"user_object_type": objectType,
			"user_object_id":   objectID,
			"user_relation":    relation,
			"is_userset":       isUserset,
		})
	}

	builder := s.stbl.
		Select(
			"store", "object_type", "object_id", "relation",
			"user_object_type", "user_object_id", "user_relation",
			"condition_name", "condition_context",
		).
		From("tuple").
		Where(sq.Eq{
			"store":       store,
			"object_type": filter.ObjectType,
			"relation":    filter.Relation,
		}).Where(userFilter).OrderBy("object_id")

	if s.regionalByRow {
		builder = builder.Where(sq.Eq{"crdb_region": "aws-us-east-1"})
	}
	if filter.ObjectIDs != nil && filter.ObjectIDs.Size() > 0 {
		builder = builder.Where(sq.Eq{"object_id": filter.ObjectIDs.Values()})
	}

	stmt, args, err := builder.ToSql()
	if err != nil {
		return nil, HandleSQLError(err)
	}

	if s.staleReads {
		stmt = stmt + " AS OF SYSTEM TIME follower_read_timestamp()"
	}

	rows, err := s.db.Query(ctx, stmt, args...)
	if err != nil {
		return nil, HandleSQLError(err)
	}

	return NewCRDBTupleIterator(rows, s.logger), nil
}

// MaxTuplesPerWrite see [storage.RelationshipTupleWriter].MaxTuplesPerWrite.
func (s *Datastore) MaxTuplesPerWrite() int {
	return s.maxTuplesPerWriteField
}

// ReadAuthorizationModel see [storage.AuthorizationModelReadBackend].ReadAuthorizationModel.
func (s *Datastore) ReadAuthorizationModel(ctx context.Context, store string, modelID string) (*openfgav1.AuthorizationModel, error) {
	ctx, span := startTrace(ctx, "ReadAuthorizationModel")
	defer span.End()

	builder := s.stbl.
		Select("authorization_model_id", "schema_version", "type", "type_definition", "serialized_protobuf").
		From("authorization_model").
		Where(sq.Eq{
			"store":                  store,
			"authorization_model_id": modelID,
		})
	if s.regionalByRow {
		builder = builder.Where(sq.Eq{"crdb_region": "aws-us-east-1"})
	}
	stmt, args, err := builder.ToSql()

	if err != nil {
		return nil, HandleSQLError(err)
	}

	if s.staleReads {
		stmt = stmt + " AS OF SYSTEM TIME follower_read_timestamp()"
	}

	row := s.db.QueryRow(ctx, stmt, args...)

	return s.readAuthorizationModel(row)
}

func (s *Datastore) readAuthorizationModel(row pgx.Row) (*openfgav1.AuthorizationModel, error) {
	var typeName string
	var marshalledTypeDef []byte
	var marshalledModel []byte
	var id string
	var schemaVersion string
	err := row.Scan(&id, &schemaVersion, &typeName, &marshalledTypeDef, &marshalledModel)
	if err != nil {
		return nil, HandleSQLError(err)
	}

	if len(marshalledModel) == 0 {
		return nil, storage.ErrNotFound
	}

	// Prefer building an authorization model from the first row that has it available.
	var model openfgav1.AuthorizationModel
	if err = proto.Unmarshal(marshalledModel, &model); err != nil {
		return nil, err
	}

	return &model, nil
}

// ReadAuthorizationModels see [storage.AuthorizationModelReadBackend].ReadAuthorizationModels.
// THIS IS VERY INNEFICIENT BUT IT WON'T BE USED IN THE TEST, COPIED FROM SQL COMMON
func (s *Datastore) ReadAuthorizationModels(ctx context.Context, store string, options storage.ReadAuthorizationModelsOptions) ([]*openfgav1.AuthorizationModel, string, error) {
	ctx, span := startTrace(ctx, "ReadAuthorizationModels")
	defer span.End()

	sb := s.stbl.
		Select("authorization_model_id").
		Distinct().
		From("authorization_model").
		Where(sq.Eq{"store": store}).
		OrderBy("authorization_model_id desc")

	if s.regionalByRow {
		sb = sb.Where(sq.Eq{"crdb_region": "aws-us-east-1"})
	}

	if options.Pagination.From != "" {
		sb = sb.Where(sq.LtOrEq{"authorization_model_id": options.Pagination.From})
	}
	if options.Pagination.PageSize > 0 {
		sb = sb.Limit(uint64(options.Pagination.PageSize + 1)) // + 1 is used to determine whether to return a continuation token.
	}

	stmt, args, err := sb.ToSql()
	if err != nil {
		return nil, "", HandleSQLError(err)
	}

	rows, err := s.db.Query(ctx, stmt, args...)
	if err != nil {
		return nil, "", HandleSQLError(err)
	}

	// defer rows.Close()

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
	rows.Close()
	return models, token, nil
}

// FindLatestAuthorizationModel see [storage.AuthorizationModelReadBackend].FindLatestAuthorizationModel.
func (s *Datastore) FindLatestAuthorizationModel(ctx context.Context, store string) (*openfgav1.AuthorizationModel, error) {
	ctx, span := startTrace(ctx, "FindLatestAuthorizationModel")
	defer span.End()

	// I would sort by timestamp
	builder := s.stbl.
		Select("authorization_model_id", "schema_version", "type", "type_definition", "serialized_protobuf").
		From("authorization_model").
		Where(sq.Eq{"store": store}).
		OrderBy("authorization_model_id desc").
		Limit(1)
	if s.regionalByRow {
		builder = builder.Where(sq.Eq{"crdb_region": "aws-us-east-1"})
	}

	stmt, args, err := builder.ToSql()

	if err != nil {
		return nil, HandleSQLError(err)
	}

	if s.staleReads {
		stmt = stmt + " AS OF SYSTEM TIME follower_read_timestamp()"
	}

	row := s.db.QueryRow(ctx, stmt, args...)
	return s.readAuthorizationModel(row)

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

	insertBuilder := s.stbl.Insert("authorization_model")

	if s.regionalByRow {
		insertBuilder = insertBuilder.Columns("crdb_region", "store", "authorization_model_id", "schema_version", "type", "type_definition", "serialized_protobuf", "inserted_at").
			Values("aws-us-east-1", store, model.GetId(), schemaVersion, "", nil, pbdata, sq.Expr("NOW()"))
	} else {
		insertBuilder = insertBuilder.Columns("store", "authorization_model_id", "schema_version", "type", "type_definition", "serialized_protobuf", "inserted_at").
			Values(store, model.GetId(), schemaVersion, "", nil, pbdata, sq.Expr("NOW()"))
	}

	stmt, args, err := insertBuilder.ToSql()
	if err != nil {
		return HandleSQLError(err)
	}
	_, err = s.db.Exec(ctx, stmt, args...)

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

	stmt, args, err := s.stbl.
		Insert("store").
		Columns("id", "name", "created_at", "updated_at").
		Values(store.GetId(), store.GetName(), sq.Expr("NOW()"), sq.Expr("NOW()")).
		Suffix("returning id, name, created_at, updated_at").ToSql()

	if err != nil {
		return nil, HandleSQLError(err)
	}

	_, err = s.db.Exec(ctx, stmt, args...)
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

	stmt, args, err := s.stbl.
		Select("id", "name", "created_at", "updated_at").
		From("store").
		Where(sq.Eq{
			"id":         id,
			"deleted_at": nil,
		}).Limit(1).ToSql()

	if err != nil {
		return nil, HandleSQLError(err)
	}

	row := s.db.QueryRow(ctx, stmt, args...)

	var storeID, name string
	var createdAt, updatedAt time.Time

	err = row.Scan(&storeID, &name, &createdAt, &updatedAt)

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

	stmt, args, err := sb.ToSql()
	if err != nil {
		return nil, "", HandleSQLError(err)
	}

	rows, err := s.db.Query(ctx, stmt, args...)
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

	stmt, args, err := s.stbl.
		Update("store").
		Set("deleted_at", sq.Expr("NOW()")).
		Where(sq.Eq{"id": id}).ToSql()

	if err != nil {
		return HandleSQLError(err)
	}

	_, err = s.db.Exec(ctx, stmt, args...)
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

	stmt, args, err := s.stbl.
		Insert("assertion").
		Columns("store", "authorization_model_id", "assertions").
		Values(store, modelID, marshalledAssertions).
		Suffix("ON CONFLICT (store, authorization_model_id) DO UPDATE SET assertions = ?", marshalledAssertions).ToSql()

	if err != nil {
		return HandleSQLError(err)
	}

	_, err = s.db.Exec(ctx, stmt, args...)
	if err != nil {
		return HandleSQLError(err)
	}

	return nil
}

// ReadAssertions see [storage.AssertionsBackend].ReadAssertions.
func (s *Datastore) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgav1.Assertion, error) {
	ctx, span := startTrace(ctx, "ReadAssertions")
	defer span.End()

	stmt, args, err := s.stbl.
		Select("assertions").
		From("assertion").
		Where(sq.Eq{
			"store":                  store,
			"authorization_model_id": modelID,
		}).ToSql()

	if err != nil {
		return nil, HandleSQLError(err)
	}

	rows, err := s.db.Query(ctx, stmt, args...)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []*openfgav1.Assertion{}, nil
		}
		return nil, HandleSQLError(err)
	}

	defer rows.Close()

	var assertions []*openfgav1.Assertion
	var assertion *openfgav1.Assertion

	for rows.Next() {
		err = rows.Scan(&assertion)
		if err != nil {
			return nil, HandleSQLError(err)
		}

		assertions = append(assertions, assertion)
	}

	return assertions, nil
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
			"_user",
			"operation",
			"condition_name", "condition_context", "inserted_at",
		).
		From("changelog").
		Where(sq.Eq{"store": store}).
		Where(fmt.Sprintf("inserted_at < NOW() - interval '%dms'", horizonOffset.Milliseconds())).
		OrderBy(orderBy)

	if s.regionalByRow {
		sb = sb.Where(sq.Eq{"crdb_region": "aws-us-east-1"})
	}

	if objectTypeFilter != "" {
		sb = sb.Where(sq.Eq{"object_type": objectTypeFilter})
	}
	if options.Pagination.From != "" {
		sb = sqlcommon.AddFromUlid(sb, options.Pagination.From, options.SortDesc)
	}
	if options.Pagination.PageSize > 0 {
		sb = sb.Limit(uint64(options.Pagination.PageSize)) // + 1 is NOT used here as we always return a continuation token.
	}

	stmt, args, err := sb.ToSql()

	if err != nil {
		return nil, "", HandleSQLError(err)
	}

	if s.staleReads {
		stmt = stmt + " AS OF SYSTEM TIME follower_read_timestamp()"
	}

	rows, err := s.db.Query(ctx, stmt, args...)
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

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	conn, err := pgx.ConnectConfig(ctx, s.db.Config().ConnConfig)
	if err != nil {
		return storage.ReadinessStatus{}, err
	}
	defer conn.Close(ctx)
	if err = conn.Ping(ctx); err != nil {
		return storage.ReadinessStatus{}, err
	}

	return storage.ReadinessStatus{
		IsReady: true,
	}, nil
}

// HandleSQLError processes an SQL error and converts it into a more
// specific error type based on the nature of the SQL error.
func HandleSQLError(err error, args ...interface{}) error {
	if errors.Is(err, pgx.ErrNoRows) {
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
