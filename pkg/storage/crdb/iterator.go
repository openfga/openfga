package crdb

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"sync"

	"github.com/jackc/pgx/v5"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type CRDBTupleIterator struct {
	rows pgx.Rows // GUARDED_BY(mu)

	// firstRow is used as a temporary storage place if head is called.
	// If firstRow is nil and Head is called, rows.Next() will return the first item and advance
	// the iterator. Thus, we will need to store this first item so that future Head() and Next()
	// will use this item instead. Otherwise, the first item will be lost.
	firstRow *storage.TupleRecord // GUARDED_BY(mu)
	mu       sync.Mutex
	logger   logger.Logger
	count    int
}

// NewCRDBTupleIterator returns a SQL tuple iterator.
func NewCRDBTupleIterator(rows pgx.Rows, logger logger.Logger) *CRDBTupleIterator {

	return &CRDBTupleIterator{
		rows:     rows,
		firstRow: nil,
		mu:       sync.Mutex{},
		logger:   logger,
		count:    0,
	}
}

func (t *CRDBTupleIterator) next() (*storage.TupleRecord, error) {
	t.mu.Lock()

	if t.firstRow != nil {
		// If head was called previously, we don't need to scan / next
		// again as the data is already there and the internal iterator would be advanced via `t.rows.Next()`.
		// Calling t.rows.Next() in this case would lose the first row data.
		//
		// For example, let's say there are 3 items [1,2,3]
		// If we called Head() and t.firstRow is empty, the rows will only be left with [2,3].
		// Thus, we will need to save item [1] in firstRow.  This allows future next() and head() to consume
		// [1] first.
		// If head() was not called, t.firstRow would be nil and we can follow the t.rows.Next() logic below.
		firstRow := t.firstRow
		t.count++
		t.firstRow = nil
		t.mu.Unlock()
		t.logger.Error("count is: " + strconv.Itoa(t.count))
		return firstRow, nil
	}

	if !t.rows.Next() {
		t.mu.Unlock()
		if err := t.rows.Err(); err != nil {
			t.logger.Error("error on iterator.")
			return nil, err
		}
		t.logger.Error("iterator is done 1 - count is: " + strconv.Itoa(t.count))
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
		t.logger.Error("error in scanning.")
		return nil, err
	}

	record.ConditionName = conditionName.String

	if conditionContext != nil {
		var conditionContextStruct structpb.Struct
		if err := proto.Unmarshal(conditionContext, &conditionContextStruct); err != nil {
			t.logger.Error("error on conditionContext..")
			return nil, err
		}
		record.ConditionContext = &conditionContextStruct
	}
	t.logger.Error("count is: " + strconv.Itoa(t.count))
	return &record, nil
}

func (t *CRDBTupleIterator) head() (*storage.TupleRecord, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.firstRow != nil {
		// If head was called previously, we don't need to scan / next
		// again as the data is already there and the internal iterator would be advanced via `t.rows.Next()`.
		// Calling t.rows.Next() in this case would lose the first row data.
		//
		// For example, let's say there are 3 items [1,2,3]
		// If we called Head() and t.firstRow is empty, the rows will only be left with [2,3].
		// Thus, we will need to save item [1] in firstRow.  This allows future next() and head() to return
		// [1] first. Note that for head(), we will not unset t.firstRow.  Therefore, calling head() multiple times
		// will yield the same result.
		// If head() was not called, t.firstRow would be nil, and we can follow the t.rows.Next() logic below.
		return t.firstRow, nil
	}

	if !t.rows.Next() {
		if err := t.rows.Err(); err != nil {
			t.logger.Error("error head to t.rows.next...")
			return nil, err
		}
		t.logger.Error("iterator is done 2 - count is: " + strconv.Itoa(t.count))
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
		t.logger.Error("error scanning on head")
		return nil, err
	}

	record.ConditionName = conditionName.String

	if conditionContext != nil {
		var conditionContextStruct structpb.Struct
		if err := proto.Unmarshal(conditionContext, &conditionContextStruct); err != nil {
			t.logger.Error("error on parsing condition.")
			return nil, err
		}
		record.ConditionContext = &conditionContextStruct
	}
	t.firstRow = &record

	return &record, nil
}

// ToArray converts the tupleIterator to an []*openfgav1.Tuple and a possibly empty continuation token.
// If the continuation token exists it is the ulid of the last element of the returned array.
func (t *CRDBTupleIterator) ToArray(
	opts storage.PaginationOptions,
) ([]*openfgav1.Tuple, string, error) {
	t.logger.Error("to array function.")
	var res []*openfgav1.Tuple
	for i := 0; i < opts.PageSize; i++ {
		tupleRecord, err := t.next()
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				return res, "", nil
			}
			return nil, "", err
		}
		res = append(res, tupleRecord.AsTuple())
	}

	// Check if we are at the end of the iterator.
	// If we are then we do not need to return a continuation token.
	// This is why we have LIMIT+1 in the query.
	tupleRecord, err := t.next()
	if err != nil {
		if errors.Is(err, storage.ErrIteratorDone) {
			return res, "", nil
		}
		return nil, "", err
	}

	return res, tupleRecord.Ulid, nil
}

// Next will return the next available item.
func (t *CRDBTupleIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	record, err := t.next()
	if err != nil {
		return nil, err
	}

	return record.AsTuple(), nil
}

// Head will return the first available item.
func (t *CRDBTupleIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	record, err := t.head()
	if err != nil {
		return nil, err
	}

	return record.AsTuple(), nil
}

// Stop terminates iteration.
func (t *CRDBTupleIterator) Stop() {
	t.rows.Close()
}
