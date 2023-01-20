package common

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/telemetry"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Config struct {
	Tracer                 trace.Tracer
	Logger                 logger.Logger
	MaxTuplesPerWriteField int
	MaxTypesPerModelField  int

	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration
}

type DatastoreOption func(*Config)

func WithTracer(t trace.Tracer) DatastoreOption {
	return func(cfg *Config) {
		cfg.Tracer = t
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

	if cfg.Tracer == nil {
		cfg.Tracer = telemetry.NewNoopTracer()
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

func (t *SQLTupleIterator) next(ctx context.Context) (*TupleRecord, error) {
	go func() {
		if !t.rows.Next() {
			if err := t.rows.Err(); err != nil {
				t.errCh <- err
				return
			}
			t.errCh <- storage.ErrIteratorDone
			return
		}
		var record TupleRecord
		if err := t.rows.Scan(&record.Store, &record.ObjectType, &record.ObjectID, &record.Relation, &record.User, &record.Ulid, &record.InsertedAt); err != nil {
			t.errCh <- err
			return
		}

		t.resultCh <- &record
	}()

	select {
	case <-ctx.Done():
		return nil, storage.ErrIteratorDone
	case err := <-t.errCh:
		return nil, HandleSQLError(err)
	case result := <-t.resultCh:
		return result, nil
	}
}

// ToArray converts the tupleIterator to an []*openfgapb.Tuple and a possibly empty continuation token. If the
// continuation token exists it is the ulid of the last element of the returned array.
func (t *SQLTupleIterator) ToArray(ctx context.Context, opts storage.PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	var res []*openfgapb.Tuple
	for i := 0; i < opts.PageSize; i++ {
		tupleRecord, err := t.next(ctx)
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
	tupleRecord, err := t.next(ctx)
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

func (t *SQLTupleIterator) Next(ctx context.Context) (*openfgapb.Tuple, error) {
	record, err := t.next(ctx)
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

func (o *SQLObjectIterator) Next(ctx context.Context) (*openfgapb.Object, error) {
	go func() {
		if !o.rows.Next() {
			if err := o.rows.Err(); err != nil {
				o.errCh <- err
				return
			}
			o.errCh <- storage.ErrIteratorDone
			return
		}

		var objectID, objectType string
		if err := o.rows.Scan(&objectType, &objectID); err != nil {
			o.errCh <- err
			return
		}

		if err := o.rows.Err(); err != nil {
			o.errCh <- err
			return
		}

		o.resultCh <- &openfgapb.Object{
			Type: objectType,
			Id:   objectID,
		}
	}()
	select {
	case <-ctx.Done():
		return nil, storage.ErrIteratorDone
	case err := <-o.errCh:
		return nil, HandleSQLError(err)
	case result := <-o.resultCh:
		return result, nil
	}
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
