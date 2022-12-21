package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-sql-driver/mysql"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	DefaultMaxTuplesPerWrite             = 100
	DefaultMaxTypesPerAuthorizationModel = 100
)

type Datastore struct {
	db                            *sql.DB
	tracer                        trace.Tracer
	logger                        logger.Logger
	maxTuplesPerWrite             int
	maxTypesPerAuthorizationModel int

	maxOpenConns    int
	maxIdleConns    int
	connMaxIdleTime time.Duration
	connMaxLifetime time.Duration
}

type DatastoreOption func(*Datastore)

func WithTracer(t trace.Tracer) DatastoreOption {
	return func(p *Datastore) {
		p.tracer = t
	}
}

func WithLogger(l logger.Logger) DatastoreOption {
	return func(p *Datastore) {
		p.logger = l
	}
}

func WithMaxTuplesPerWrite(maxTuples int) DatastoreOption {
	return func(p *Datastore) {
		p.maxTuplesPerWrite = maxTuples
	}
}

func WithMaxTypesPerAuthorizationModel(maxTypes int) DatastoreOption {
	return func(p *Datastore) {
		p.maxTypesPerAuthorizationModel = maxTypes
	}
}

func WithMaxOpenConns(c int) DatastoreOption {
	return func(p *Datastore) {
		p.maxOpenConns = c
	}
}

func WithMaxIdleConns(c int) DatastoreOption {
	return func(p *Datastore) {
		p.maxIdleConns = c
	}
}

func WithConnMaxIdleTime(d time.Duration) DatastoreOption {
	return func(p *Datastore) {
		p.connMaxIdleTime = d
	}
}

func WithConnMaxLifetime(d time.Duration) DatastoreOption {
	return func(p *Datastore) {
		p.connMaxLifetime = d
	}
}

func NewDatastore(uri string, opts ...DatastoreOption) (*Datastore, error) {
	d := &Datastore{}

	for _, opt := range opts {
		opt(d)
	}

	if d.logger == nil {
		d.logger = logger.NewNoopLogger()
	}

	if d.tracer == nil {
		d.tracer = telemetry.NewNoopTracer()
	}

	if d.maxTuplesPerWrite == 0 {
		d.maxTuplesPerWrite = DefaultMaxTuplesPerWrite
	}

	if d.maxTypesPerAuthorizationModel == 0 {
		d.maxTypesPerAuthorizationModel = DefaultMaxTypesPerAuthorizationModel
	}

	db, err := sql.Open("pgx", uri)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Postgres connection: %w", err)
	}

	if d.maxOpenConns != 0 {
		db.SetMaxOpenConns(d.maxOpenConns)
	}

	if d.maxIdleConns != 0 {
		db.SetMaxIdleConns(d.maxIdleConns)
	}

	if d.connMaxIdleTime != 0 {
		db.SetConnMaxIdleTime(d.connMaxIdleTime)
	}

	if d.connMaxLifetime != 0 {
		db.SetConnMaxLifetime(d.connMaxLifetime)
	}

	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = 1 * time.Minute
	attempt := 1
	err = backoff.Retry(func() error {
		err = db.PingContext(context.Background())
		if err != nil {
			d.logger.Info("waiting for Postgres", zap.Int("attempt", attempt))
			attempt++
			return err
		}
		return nil
	}, policy)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Postgres connection: %w", err)
	}

	d.db = db

	return d, nil
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
		return nil, ErrInvalidContinuationToken
	}
	return &token, nil
}

type SQLTupleIterator struct {
	rows     *sql.Rows
	resultCh chan *TupleRecord
	errCh    chan error
}

var _ TupleIterator = (*SQLTupleIterator)(nil)

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
			t.errCh <- ErrIteratorDone
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
		return nil, ErrIteratorDone
	case err := <-t.errCh:
		return nil, HandleSQLError(err)
	case result := <-t.resultCh:
		return result, nil
	}
}

// ToArray converts the tupleIterator to an []*openfgapb.Tuple and a possibly empty continuation token. If the
// continuation token exists it is the ulid of the last element of the returned array.
func (t *SQLTupleIterator) ToArray(ctx context.Context, opts PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	var res []*openfgapb.Tuple
	for i := 0; i < opts.PageSize; i++ {
		tupleRecord, err := t.next(ctx)
		if err != nil {
			if err == ErrIteratorDone {
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
		if errors.Is(err, ErrIteratorDone) {
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

var _ ObjectIterator = (*SQLObjectIterator)(nil)

func (o *SQLObjectIterator) Next(ctx context.Context) (*openfgapb.Object, error) {
	go func() {
		if !o.rows.Next() {
			if err := o.rows.Err(); err != nil {
				o.errCh <- err
				return
			}
			o.errCh <- ErrIteratorDone
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
		return nil, ErrIteratorDone
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
		return ErrNotFound
	} else if errors.Is(err, ErrIteratorDone) {
		return ErrIteratorDone
	} else if strings.Contains(err.Error(), "duplicate key value") { // Postgres
		if len(args) > 0 {
			if tk, ok := args[0].(*openfgapb.TupleKey); ok {
				return InvalidWriteInputError(tk, openfgapb.TupleOperation_TUPLE_OPERATION_WRITE)
			}
		}
		return ErrCollision
	} else if me, ok := err.(*mysql.MySQLError); ok && me.Number == 1062 {
		if len(args) > 0 {
			if tk, ok := args[0].(*openfgapb.TupleKey); ok {
				return InvalidWriteInputError(tk, openfgapb.TupleOperation_TUPLE_OPERATION_WRITE)
			}
		}
		return ErrCollision
	}

	return fmt.Errorf("sql error: %w", err)
}
