package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
)

type errorHandlerFn func(error, ...interface{}) error

// SQLTupleIterator is a struct that implements the storage.TupleIterator
// interface for iterating over tuples fetched from a SQL database.
type SQLTupleIterator struct {
	rows           *sql.Rows // GUARDED_BY(mu)
	sb             sq.SelectBuilder
	handleSQLError errorHandlerFn
	firstRow       *storage.TupleRecord // GUARDED_BY(mu)
	mu             sync.Mutex
	inFlight       sync.Mutex
}

// Ensures that SQLTupleIterator implements the TupleIterator interface.
var _ storage.TupleIterator = (*SQLTupleIterator)(nil)

// NewSQLTupleIterator returns a SQL tuple iterator.
func NewSQLTupleIterator(sb sq.SelectBuilder, errHandler errorHandlerFn) *SQLTupleIterator {
	return &SQLTupleIterator{
		sb:             sb,
		rows:           nil,
		handleSQLError: errHandler,
		firstRow:       nil,
		mu:             sync.Mutex{},
	}
}

func (t *SQLTupleIterator) fetchBuffer(ctx context.Context) error {
	t.inFlight.Lock()
	defer t.inFlight.Unlock()

	ctx, span := tracer.Start(ctx, "sqlite.fetchBuffer", trace.WithAttributes())
	defer span.End()

	// We intentionally detach cancellation so DB query can complete
	ctx = context.WithoutCancel(ctx)

	start := time.Now()
	rows, err := t.sb.QueryContext(ctx)
	elapsed := time.Since(start)

	if err != nil {
		storageErr := t.handleSQLError(err)
		storage.ObserveIterQueryDuration(storage.SuccessLabel(storageErr), elapsed)
		return storageErr
	}

	storage.ObserveIterQueryDuration(true, elapsed)

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.rows != nil {
		_ = t.rows.Close()
	}

	t.rows = rows
	t.firstRow = nil

	return nil
}

func (t *SQLTupleIterator) next(ctx context.Context) (*storage.TupleRecord, error) {
	t.mu.Lock()

	if t.rows == nil {
		t.mu.Unlock()

		if err := t.fetchBuffer(ctx); err != nil {
			return nil, err
		}

		t.mu.Lock()
		if t.rows == nil {
			t.mu.Unlock()
			return nil, errors.New("failed to initialize iterator rows")
		}
	}

	if t.firstRow != nil {
		firstRow := t.firstRow
		t.firstRow = nil
		t.mu.Unlock()
		return firstRow, nil
	}

	if !t.rows.Next() {
		err := t.rows.Err()

		// FIX: close rows when exhausted
		_ = t.rows.Close()
		t.rows = nil

		t.mu.Unlock()

		if err != nil {
			return nil, t.handleSQLError(err)
		}
		return nil, storage.ErrIteratorDone
	}

	var conditionName sql.NullString
	var conditionContext []byte
	var record storage.TupleRecord

	err := t.rows.Scan(
		&record.Store,
		&record.ObjectType,
		&record.ObjectID,
		&record.Relation,
		&record.UserObjectType,
		&record.UserObjectID,
		&record.UserRelation,
		&conditionName,
		&conditionContext,
		&record.Ulid,
		&record.InsertedAt,
	)

	t.mu.Unlock()

	if err != nil {
		return nil, t.handleSQLError(err)
	}

	record.ConditionName = conditionName.String

	if conditionContext != nil {
		var conditionContextStruct structpb.Struct
		if err := proto.Unmarshal(conditionContext, &conditionContextStruct); err != nil {
			return nil, t.handleSQLError(err) // FIX: consistent error handling
		}
		record.ConditionContext = &conditionContextStruct
	}

	return &record, nil
}

func (t *SQLTupleIterator) head(ctx context.Context) (*storage.TupleRecord, error) {
	t.mu.Lock()

	if t.rows == nil {
		t.mu.Unlock()

		if err := t.fetchBuffer(ctx); err != nil {
			return nil, err
		}

		t.mu.Lock()
		if t.rows == nil {
			t.mu.Unlock()
			return nil, errors.New("failed to initialize iterator rows")
		}
	}

	defer t.mu.Unlock()

	if t.firstRow != nil {
		return t.firstRow, nil
	}

	if !t.rows.Next() {
		if err := t.rows.Err(); err != nil {
			return nil, t.handleSQLError(err)
		}

		// FIX: close rows when exhausted
		_ = t.rows.Close()
		t.rows = nil

		return nil, storage.ErrIteratorDone
	}

	var conditionName sql.NullString
	var conditionContext []byte
	var record storage.TupleRecord

	err := t.rows.Scan(
		&record.Store,
		&record.ObjectType,
		&record.ObjectID,
		&record.Relation,
		&record.UserObjectType,
		&record.UserObjectID,
		&record.UserRelation,
		&conditionName,
		&conditionContext,
		&record.Ulid,
		&record.InsertedAt,
	)

	if err != nil {
		return nil, t.handleSQLError(err)
	}

	record.ConditionName = conditionName.String

	if conditionContext != nil {
		var conditionContextStruct structpb.Struct
		if err := proto.Unmarshal(conditionContext, &conditionContextStruct); err != nil {
			return nil, t.handleSQLError(err)
		}
		record.ConditionContext = &conditionContextStruct
	}

	// FIX: avoid pointer-to-stack ambiguity by copying
	rec := record
	t.firstRow = &rec
	return &rec, nil
}

// ToArray converts the tupleIterator to an []*openfgav1.Tuple and a possibly empty continuation token.
// If the continuation token exists it is the ulid of the last element of the returned array.
func (t *SQLTupleIterator) ToArray(
	ctx context.Context,
	opts storage.PaginationOptions,
) ([]*openfgav1.Tuple, string, error) {
	var res []*openfgav1.Tuple
	for i := 0; i < opts.PageSize; i++ {
		tupleRecord, err := t.next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				return res, "", nil
			}
			return nil, "", err
		}
		res = append(res, tupleRecord.AsTuple())
	}

	// NOTE: Query must use LIMIT pageSize+1 for pagination correctness
	tupleRecord, err := t.next(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrIteratorDone) {
			return res, "", nil
		}
		return nil, "", err
	}

	return res, tupleRecord.Ulid, nil
}

// Next will return the next available item.
func (t *SQLTupleIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	record, err := t.next(ctx)
	if err != nil {
		return nil, err
	}

	return record.AsTuple(), nil
}

// Head will return the first available item.
func (t *SQLTupleIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	record, err := t.head(ctx)
	if err != nil {
		return nil, err
	}

	return record.AsTuple(), nil
}

// Stop terminates iteration.
func (t *SQLTupleIterator) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.rows != nil {
		_ = t.rows.Close()
	}
}