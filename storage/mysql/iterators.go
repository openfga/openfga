package mysql

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/openfga/openfga/storage"
	"github.com/pkg/errors"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type tupleIterator struct {
	rows     *sql.Rows
	resultCh chan *tupleRecord
	errCh    chan error
}

var _ storage.TupleIterator = (*tupleIterator)(nil)

// NewMysqlTupleIterator returns a tuple iterator for MySQL
func NewMysqlTupleIterator(rows *sql.Rows) *tupleIterator {
	return &tupleIterator{
		rows:     rows,
		resultCh: make(chan *tupleRecord),
		errCh:    make(chan error),
	}
}

func (t *tupleIterator) next(ctx context.Context) (*tupleRecord, error) {
	go func() {
		if !t.rows.Next() {
			t.Stop()
			t.errCh <- storage.ErrIteratorDone
			return
		}

		var record tupleRecord
		if err := t.rows.Scan(&record.store, &record.objectType, &record.objectID, &record.relation, &record.user, &record.ulid, &record.insertedAt); err != nil {
			t.errCh <- err
			return
		}

		if err := t.rows.Err(); err != nil {
			t.errCh <- err
			return
		}

		t.resultCh <- &record
	}()

	select {
	case <-ctx.Done():
		return nil, storage.ErrIteratorDone
	case err := <-t.errCh:
		return nil, handleMySQLError(err)
	case result, ok := <-t.resultCh:
		if ok {
			return result, nil
		}

		return nil, storage.ErrIteratorDone
	}
}

// toArray converts the tupleIterator to an []*openfgapb.Tuple and a possibly empty continuation token. If the
// continuation token exists it is the ulid of the last element of the returned array.
func (t *tupleIterator) toArray(ctx context.Context, opts storage.PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	defer t.Stop()

	var res []*openfgapb.Tuple
	for i := 0; i < opts.PageSize; i++ {
		tupleRecord, err := t.next(ctx)
		if err != nil {
			if err == storage.ErrIteratorDone {
				return res, nil, nil
			}
			return nil, nil, err
		}
		res = append(res, tupleRecord.asTuple())
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

	contToken, err := json.Marshal(newContToken(tupleRecord.ulid, ""))
	if err != nil {
		return nil, nil, err
	}

	return res, contToken, nil
}

func (t *tupleIterator) Next(ctx context.Context) (*openfgapb.Tuple, error) {
	record, err := t.next(ctx)
	if err != nil {
		return nil, err
	}
	return record.asTuple(), nil
}

func (t *tupleIterator) Stop() {
	t.rows.Close()
}

type objectIterator struct {
	rows     *sql.Rows
	resultCh chan *openfgapb.Object
	errCh    chan error
}

// NewMysqlObjectIterator returns a tuple iterator for Postgres
func NewMysqlObjectIterator(rows *sql.Rows) *objectIterator {
	return &objectIterator{
		rows:     rows,
		resultCh: make(chan *openfgapb.Object),
		errCh:    make(chan error),
	}
}

var _ storage.ObjectIterator = (*objectIterator)(nil)

func (o *objectIterator) Next(ctx context.Context) (*openfgapb.Object, error) {
	go func() {
		if !o.rows.Next() {
			o.Stop()
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
		return nil, handleMySQLError(err)
	case result, ok := <-o.resultCh:
		if ok {
			return result, nil
		}

		return nil, storage.ErrIteratorDone
	}
}

func (o *objectIterator) Stop() {
	o.rows.Close()
}
