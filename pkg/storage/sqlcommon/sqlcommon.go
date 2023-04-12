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
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/pressly/goose/v3"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const latestDBVersion = 4

type Config struct {
	Logger                 logger.Logger
	MaxTuplesPerWriteField int
	MaxTypesPerModelField  int

	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration
}

type DatastoreOption func(*Config)

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
	Store      string
	ObjectType string
	ObjectID   string
	Relation   string
	User       string
	Ulid       string
	InsertedAt time.Time
}

func (t *TupleRecord) AsTuple() *openfgapb.Tuple {
	return &openfgapb.Tuple{
		Key: &openfgapb.TupleKey{
			Object:   tupleUtils.BuildObject(t.ObjectType, t.ObjectID),
			Relation: t.Relation,
			User:     t.User,
		},
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

	var record TupleRecord
	err := t.rows.Scan(&record.Store, &record.ObjectType, &record.ObjectID, &record.Relation, &record.User, &record.Ulid, &record.InsertedAt)
	if err != nil {
		return nil, err
	}

	return &record, nil
}

// ToArray converts the tupleIterator to an []*openfgapb.Tuple and a possibly empty continuation token. If the
// continuation token exists it is the ulid of the last element of the returned array.
func (t *SQLTupleIterator) ToArray(opts storage.PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	var res []*openfgapb.Tuple
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

func (t *SQLTupleIterator) Next() (*openfgapb.Tuple, error) {
	record, err := t.next()
	if err != nil {
		return nil, err
	}

	return record.AsTuple(), nil
}

func (t *SQLTupleIterator) Stop() {
	t.rows.Close()
}

type SQLObjectIterator struct {
	rows     *sql.Rows
	resultCh chan *openfgapb.Object
	errCh    chan error
}

// NewSQLObjectIterator returns a tuple iterator for Postgres
func NewSQLObjectIterator(rows *sql.Rows) *SQLObjectIterator {
	return &SQLObjectIterator{
		rows:     rows,
		resultCh: make(chan *openfgapb.Object, 1),
		errCh:    make(chan error, 1),
	}
}

var _ storage.ObjectIterator = (*SQLObjectIterator)(nil)

func (o *SQLObjectIterator) Next() (*openfgapb.Object, error) {
	if !o.rows.Next() {
		if err := o.rows.Err(); err != nil {
			return nil, err
		}
		return nil, storage.ErrIteratorDone
	}

	var object openfgapb.Object
	if err := o.rows.Scan(&object.Type, &object.Id); err != nil {
		return nil, err
	}

	return &object, nil
}

func (o *SQLObjectIterator) Stop() {
	_ = o.rows.Close()
}

func HandleSQLError(err error, args ...interface{}) error {
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrNotFound
	} else if errors.Is(err, storage.ErrIteratorDone) {
		return err
	} else if strings.Contains(err.Error(), "duplicate key value") { // Postgres
		if len(args) > 0 {
			if tk, ok := args[0].(*openfgapb.TupleKey); ok {
				return storage.InvalidWriteInputError(tk, openfgapb.TupleOperation_TUPLE_OPERATION_WRITE)
			}
		}
		return storage.ErrCollision
	} else if me, ok := err.(*mysql.MySQLError); ok && me.Number == 1062 {
		if len(args) > 0 {
			if tk, ok := args[0].(*openfgapb.TupleKey); ok {
				return storage.InvalidWriteInputError(tk, openfgapb.TupleOperation_TUPLE_OPERATION_WRITE)
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
func Write(ctx context.Context, dbInfo *DBInfo, store string, deletes storage.Deletes, writes storage.Writes, now time.Time) error {

	txn, err := dbInfo.db.BeginTx(ctx, nil)
	if err != nil {
		return HandleSQLError(err)
	}
	defer func() {
		_ = txn.Rollback()
	}()

	changelogBuilder := dbInfo.stbl.
		Insert("changelog").
		Columns("store", "object_type", "object_id", "relation", "_user", "user_object_type", "user_object_id", "user_relation", "operation", "ulid", "inserted_at")

	deleteBuilder := dbInfo.stbl.Delete("tuple")

	for _, tk := range deletes {
		id := ulid.MustNew(ulid.Timestamp(now), ulid.DefaultEntropy()).String()
		objectType, objectID := tupleUtils.SplitObject(tk.GetObject())

		res, err := deleteBuilder.
			Where(sq.Eq{
				"store":       store,
				"object_type": objectType,
				"object_id":   objectID,
				"relation":    tk.GetRelation(),
				"_user":       tk.GetUser(),
				"user_type":   tupleUtils.GetUserTypeFromUser(tk.GetUser()),
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
			return storage.InvalidWriteInputError(tk, openfgapb.TupleOperation_TUPLE_OPERATION_DELETE)
		}

		userObjectType, userObjectID, userRelation := ToUserParts(tk.GetUser())

		changelogBuilder = changelogBuilder.Values(store, objectType, objectID, tk.GetRelation(), tk.GetUser(), userObjectType, userObjectID, userRelation, openfgapb.TupleOperation_TUPLE_OPERATION_DELETE, id, dbInfo.sqlTime)
	}

	insertBuilder := dbInfo.stbl.
		Insert("tuple").
		Columns("store", "object_type", "object_id", "relation", "_user", "user_type", "user_object_type", "user_object_id", "user_relation", "ulid", "inserted_at")

	for _, tk := range writes {
		id := ulid.MustNew(ulid.Timestamp(now), ulid.DefaultEntropy()).String()
		objectType, objectID := tupleUtils.SplitObject(tk.GetObject())
		userObjectType, userObjectID, userRelation := ToUserParts(tk.GetUser())

		_, err = insertBuilder.
			Values(store, objectType, objectID, tk.GetRelation(), tk.GetUser(), tupleUtils.GetUserTypeFromUser(tk.GetUser()), userObjectType, userObjectID, userRelation, id, dbInfo.sqlTime).
			RunWith(txn). // Part of a txn
			ExecContext(ctx)
		if err != nil {
			return HandleSQLError(err, tk)
		}

		changelogBuilder = changelogBuilder.Values(store, objectType, objectID, tk.GetRelation(), tk.GetUser(), userObjectType, userObjectID, userRelation, openfgapb.TupleOperation_TUPLE_OPERATION_WRITE, id, dbInfo.sqlTime)
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

func IsReady(ctx context.Context, db *sql.DB) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return false, err
	}

	currentVersion, err := goose.GetDBVersion(db)
	if err != nil {
		return false, err
	}

	if currentVersion != latestDBVersion {
		return false, fmt.Errorf("database version is %d but expected %d", currentVersion, latestDBVersion)
	}

	return true, nil
}

func ToUserPartsFromObjectRelation(u *openfgapb.ObjectRelation) (string, string, string) {
	user := tupleUtils.GetObjectRelationAsString(u)
	return ToUserParts(user)
}

func ToUserParts(user string) (string, string, string) {
	userObject, userRelation := tupleUtils.SplitObjectRelation(user) // e.g. (person:bob, "") or (group:abc, member) or (person:*, "")

	userObjectType, userObjectID := tupleUtils.SplitObject(userObject)

	return userObjectType, userObjectID, userRelation
}

func FromUserParts(userObjectType, userObjectID, userRelation string) string {
	user := userObjectID
	if userObjectType != "" {
		user = fmt.Sprintf("%s:%s", userObjectType, userObjectID)
	}
	if userRelation != "" {
		user = fmt.Sprintf("%s:%s#%s", userObjectType, userObjectID, userRelation)
	}
	return user
}
