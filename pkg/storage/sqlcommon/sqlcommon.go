// Package sqlcommon contains utility functions shared among all SQL data stores.
package sqlcommon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/go-sql-driver/mysql"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Config struct {
	Username               string
	Password               string
	Logger                 logger.Logger
	MaxTuplesPerWriteField int
	MaxTypesPerModelField  int

	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration
}

type DatastoreOption func(*Config)

func WithUsername(username string) DatastoreOption {
	return func(config *Config) {
		config.Username = username
	}
}

func WithPassword(password string) DatastoreOption {
	return func(config *Config) {
		config.Password = password
	}
}

func WithLogger(l logger.Logger) DatastoreOption {
	return func(cfg *Config) {
		cfg.Logger = l
	}
}

func WithMaxTuplesPerWrite(maxTuples int) DatastoreOption {
	return func(cfg *Config) {
		cfg.MaxTuplesPerWriteField = maxTuples
	}
}

func WithMaxTypesPerAuthorizationModel(maxTypes int) DatastoreOption {
	return func(cfg *Config) {
		cfg.MaxTypesPerModelField = maxTypes
	}
}

func WithMaxOpenConns(c int) DatastoreOption {
	return func(cfg *Config) {
		cfg.MaxOpenConns = c
	}
}

func WithMaxIdleConns(c int) DatastoreOption {
	return func(cfg *Config) {
		cfg.MaxIdleConns = c
	}
}

func WithConnMaxIdleTime(d time.Duration) DatastoreOption {
	return func(cfg *Config) {
		cfg.ConnMaxIdleTime = d
	}
}

func WithConnMaxLifetime(d time.Duration) DatastoreOption {
	return func(cfg *Config) {
		cfg.ConnMaxLifetime = d
	}
}

func NewConfig(opts ...DatastoreOption) *Config {
	cfg := &Config{}

	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.Logger == nil {
		cfg.Logger = logger.NewNoopLogger()
	}

	if cfg.MaxTuplesPerWriteField == 0 {
		cfg.MaxTuplesPerWriteField = storage.DefaultMaxTuplesPerWrite
	}

	if cfg.MaxTypesPerModelField == 0 {
		cfg.MaxTypesPerModelField = storage.DefaultMaxTypesPerAuthorizationModel
	}

	return cfg
}

type TupleRecord struct {
	Store            string
	ObjectType       string
	ObjectID         string
	Relation         string
	User             string
	ConditionName    string
	ConditionContext *structpb.Struct
	Ulid             string
	InsertedAt       time.Time
}

func (t *TupleRecord) AsTuple() *openfgav1.Tuple {
	tk := &openfgav1.TupleKey{
		Object:   tupleUtils.BuildObject(t.ObjectType, t.ObjectID),
		Relation: t.Relation,
		User:     t.User,
	}

	if t.ConditionName != "" {
		tk.Condition = &openfgav1.RelationshipCondition{
			Name:    t.ConditionName,
			Context: t.ConditionContext,
		}
	}

	return &openfgav1.Tuple{
		Key:       tk,
		Timestamp: timestamppb.New(t.InsertedAt),
	}
}

type ContToken struct {
	Ulid       string `json:"ulid"`
	ObjectType string `json:"ObjectType"`
}

func NewContToken(ulid, objectType string) *ContToken {
	return &ContToken{
		Ulid:       ulid,
		ObjectType: objectType,
	}
}

func UnmarshallContToken(from string) (*ContToken, error) {
	var token ContToken
	if err := json.Unmarshal([]byte(from), &token); err != nil {
		return nil, storage.ErrInvalidContinuationToken
	}
	return &token, nil
}

type SQLTupleIterator struct {
	rows     *sql.Rows
	resultCh chan *TupleRecord
	errCh    chan error
}

var _ storage.TupleIterator = (*SQLTupleIterator)(nil)

// NewSQLTupleIterator returns a SQL tuple iterator
func NewSQLTupleIterator(rows *sql.Rows) *SQLTupleIterator {
	return &SQLTupleIterator{
		rows:     rows,
		resultCh: make(chan *TupleRecord, 1),
		errCh:    make(chan error, 1),
	}
}

func (t *SQLTupleIterator) next() (*TupleRecord, error) {
	if !t.rows.Next() {
		if err := t.rows.Err(); err != nil {
			return nil, err
		}
		return nil, storage.ErrIteratorDone
	}

	var conditionContext []byte
	var record TupleRecord
	err := t.rows.Scan(
		&record.Store,
		&record.ObjectType,
		&record.ObjectID,
		&record.Relation,
		&record.User,
		&record.ConditionName,
		&conditionContext,
		&record.Ulid,
		&record.InsertedAt,
	)
	if err != nil {
		return nil, err
	}

	if conditionContext != nil {
		var conditionContextStruct structpb.Struct
		if err := proto.Unmarshal(conditionContext, &conditionContextStruct); err != nil {
			return nil, err
		}
		record.ConditionContext = &conditionContextStruct
	}

	return &record, nil
}

// ToArray converts the tupleIterator to an []*openfgav1.Tuple and a possibly empty continuation token. If the
// continuation token exists it is the ulid of the last element of the returned array.
func (t *SQLTupleIterator) ToArray(
	opts storage.PaginationOptions,
) ([]*openfgav1.Tuple, []byte, error) {
	var res []*openfgav1.Tuple
	for i := 0; i < opts.PageSize; i++ {
		tupleRecord, err := t.next()
		if err != nil {
			if err == storage.ErrIteratorDone {
				return res, nil, nil
			}
			return nil, nil, err
		}
		res = append(res, tupleRecord.AsTuple())
	}

	// Check if we are at the end of the iterator. If we are then we do not need to return a continuation token.
	// This is why we have LIMIT+1 in the query.
	tupleRecord, err := t.next()
	if err != nil {
		if errors.Is(err, storage.ErrIteratorDone) {
			return res, nil, nil
		}
		return nil, nil, err
	}

	contToken, err := json.Marshal(NewContToken(tupleRecord.Ulid, ""))
	if err != nil {
		return nil, nil, err
	}

	return res, contToken, nil
}

func (t *SQLTupleIterator) Next() (*openfgav1.Tuple, error) {
	record, err := t.next()
	if err != nil {
		return nil, err
	}

	return record.AsTuple(), nil
}

func (t *SQLTupleIterator) Stop() {
	t.rows.Close()
}

func HandleSQLError(err error, args ...interface{}) error {
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrNotFound
	} else if errors.Is(err, storage.ErrIteratorDone) {
		return err
	} else if strings.Contains(err.Error(), "duplicate key value") { // Postgres
		if len(args) > 0 {
			if tk, ok := args[0].(*openfgav1.TupleKey); ok {
				return storage.InvalidWriteInputError(tk, openfgav1.TupleOperation_TUPLE_OPERATION_WRITE)
			}
		}
		return storage.ErrCollision
	} else if me, ok := err.(*mysql.MySQLError); ok && me.Number == 1062 {
		if len(args) > 0 {
			if tk, ok := args[0].(*openfgav1.TupleKey); ok {
				return storage.InvalidWriteInputError(tk, openfgav1.TupleOperation_TUPLE_OPERATION_WRITE)
			}
		}
		return storage.ErrCollision
	}

	return fmt.Errorf("sql error: %w", err)
}

// DBInfo encapsulates DB information for use in common method
type DBInfo struct {
	db      *sql.DB
	stbl    sq.StatementBuilderType
	sqlTime interface{}
}

// NewDBInfo constructs a DBInfo objet
func NewDBInfo(db *sql.DB, stbl sq.StatementBuilderType, sqlTime interface{}) *DBInfo {
	return &DBInfo{
		db:      db,
		stbl:    stbl,
		sqlTime: sqlTime,
	}
}

// Write provides the common method for writing to database across sql storage
func Write(
	ctx context.Context,
	dbInfo *DBInfo,
	store string,
	deletes storage.Deletes,
	writes storage.Writes,
	now time.Time,
) error {
	txn, err := dbInfo.db.BeginTx(ctx, nil)
	if err != nil {
		return HandleSQLError(err)
	}
	defer func() {
		_ = txn.Rollback()
	}()

	changelogBuilder := dbInfo.stbl.
		Insert("changelog").
		Columns(
			"store", "object_type", "object_id", "relation", "_user",
			"condition_name", "condition_context", "operation", "ulid", "inserted_at",
		)

	deleteBuilder := dbInfo.stbl.Delete("tuple")

	for _, tk := range deletes {
		id := ulid.MustNew(ulid.Timestamp(now), ulid.DefaultEntropy()).String()
		objectType, objectID := tupleUtils.SplitObject(tk.GetObject())

		conditionName, conditionContext, err := marshalRelationshipCondition(tk.GetCondition())
		if err != nil {
			return err
		}

		res, err := deleteBuilder.
			Where(sq.Eq{
				"store":             store,
				"object_type":       objectType,
				"object_id":         objectID,
				"relation":          tk.GetRelation(),
				"_user":             tk.GetUser(),
				"user_type":         tupleUtils.GetUserTypeFromUser(tk.GetUser()),
				"condition_name":    conditionName,
				"condition_context": conditionContext,
			}).
			RunWith(txn). // Part of a txn
			ExecContext(ctx)
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
			store, objectType, objectID,
			tk.GetRelation(), tk.GetUser(),
			conditionName, conditionContext,
			openfgav1.TupleOperation_TUPLE_OPERATION_DELETE,
			id, dbInfo.sqlTime,
		)
	}

	insertBuilder := dbInfo.stbl.
		Insert("tuple").
		Columns("store", "object_type", "object_id", "relation", "_user", "user_type", "condition_name", "condition_context", "ulid", "inserted_at")

	for _, tk := range writes {
		id := ulid.MustNew(ulid.Timestamp(now), ulid.DefaultEntropy()).String()
		objectType, objectID := tupleUtils.SplitObject(tk.GetObject())

		conditionName, conditionContext, err := marshalRelationshipCondition(tk.GetCondition())
		if err != nil {
			return err
		}

		_, err = insertBuilder.
			Values(
				store,
				objectType,
				objectID,
				tk.GetRelation(),
				tk.GetUser(),
				tupleUtils.GetUserTypeFromUser(tk.GetUser()),
				conditionName,
				conditionContext,
				id,
				dbInfo.sqlTime,
			).
			RunWith(txn). // Part of a txn
			ExecContext(ctx)
		if err != nil {
			return HandleSQLError(err, tk)
		}

		changelogBuilder = changelogBuilder.Values(
			store,
			objectType,
			objectID,
			tk.GetRelation(),
			tk.GetUser(),
			conditionName,
			conditionContext,
			openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
			id,
			dbInfo.sqlTime,
		)
	}

	if len(writes) > 0 || len(deletes) > 0 {
		_, err := changelogBuilder.RunWith(txn).ExecContext(ctx) // Part of a txn
		if err != nil {
			return HandleSQLError(err)
		}
	}

	if err := txn.Commit(); err != nil {
		return HandleSQLError(err)
	}

	return nil
}

func WriteAuthorizationModel(
	ctx context.Context,
	dbInfo *DBInfo,
	store string,
	model *openfgav1.AuthorizationModel,
) error {
	schemaVersion := model.GetSchemaVersion()
	typeDefinitions := model.GetTypeDefinitions()

	if len(typeDefinitions) < 1 {
		return nil
	}

	pbdata, err := proto.Marshal(model)
	if err != nil {
		return err
	}

	sb := dbInfo.stbl.
		Insert("authorization_model").
		Columns("store", "authorization_model_id", "schema_version", "type", "type_definition", "serialized_protobuf")

	for _, td := range typeDefinitions {
		marshalledTypeDef, err := proto.Marshal(td)
		if err != nil {
			return err
		}

		sb = sb.Values(store, model.Id, schemaVersion, td.GetType(), marshalledTypeDef, pbdata)
	}

	_, err = sb.ExecContext(ctx)
	if err != nil {
		return HandleSQLError(err)
	}

	return nil
}

func ReadAuthorizationModel(
	ctx context.Context,
	dbInfo *DBInfo,
	store, modelID string,
) (*openfgav1.AuthorizationModel, error) {
	rows, err := dbInfo.stbl.
		Select("schema_version", "type", "type_definition", "serialized_protobuf").
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

	var schemaVersion string
	var typeDefs []*openfgav1.TypeDefinition
	for rows.Next() {
		var typeName string
		var marshalledTypeDef []byte
		var marshalledModel []byte
		err = rows.Scan(&schemaVersion, &typeName, &marshalledTypeDef, &marshalledModel)
		if err != nil {
			return nil, HandleSQLError(err)
		}

		if len(marshalledModel) > 0 {
			// Prefer building an authorization model from the first row that has it available.
			var model openfgav1.AuthorizationModel
			if err := proto.Unmarshal(marshalledModel, &model); err != nil {
				return nil, err
			}

			return &model, nil
		}

		var typeDef openfgav1.TypeDefinition
		if err := proto.Unmarshal(marshalledTypeDef, &typeDef); err != nil {
			return nil, err
		}

		typeDefs = append(typeDefs, &typeDef)
	}

	if err := rows.Err(); err != nil {
		return nil, HandleSQLError(err)
	}

	if len(typeDefs) == 0 {
		return nil, storage.ErrNotFound
	}

	return &openfgav1.AuthorizationModel{
		SchemaVersion:   schemaVersion,
		Id:              modelID,
		TypeDefinitions: typeDefs,
	}, nil
}

// IsReady returns true if the connection to the datastore is successful
func IsReady(ctx context.Context, db *sql.DB) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return false, err
	}

	return true, nil
}
