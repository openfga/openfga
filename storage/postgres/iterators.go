package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type tupleIterator struct {
	rows     *sql.Rows
	resultCh chan *tupleRecord
	errCh    chan error
}

var _ storage.TupleIterator = (*tupleIterator)(nil)

// NewPostgresTupleIterator returns a tuple iterator for Postgres
func NewPostgresTupleIterator(rows *sql.Rows) *tupleIterator {
	return &tupleIterator{
		rows:     rows,
		resultCh: make(chan *tupleRecord, 1),
		errCh:    make(chan error, 1),
	}
}

func (t *tupleIterator) next(ctx context.Context) (*tupleRecord, error) {
	go func() {
		if !t.rows.Next() {
			if err := t.rows.Err(); err != nil {
				t.errCh <- err
				return
			}
			t.errCh <- storage.ErrIteratorDone
			return
		}
		var record tupleRecord
		if err := t.rows.Scan(&record.store, &record.objectType, &record.objectID, &record.relation, &record.user, &record.ulid, &record.insertedAt); err != nil {
			t.errCh <- err
			return
		}

		t.resultCh <- &record
	}()

	select {
	case <-ctx.Done():
		return nil, storage.ErrIteratorDone
	case err := <-t.errCh:
		return nil, handlePostgresError(err)
	case result := <-t.resultCh:
		return result, nil
	}
}

// toArray converts the tupleIterator to an []*openfgapb.Tuple and a possibly empty continuation token. If the
// continuation token exists it is the ulid of the last element of the returned array.
func (t *tupleIterator) toArray(ctx context.Context, opts storage.PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
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

// NewPostgresObjectIterator returns a tuple iterator for Postgres
func NewPostgresObjectIterator(rows *sql.Rows) *objectIterator {
	return &objectIterator{
		rows:     rows,
		resultCh: make(chan *openfgapb.Object, 1),
		errCh:    make(chan error, 1),
	}
}

var _ storage.ObjectIterator = (*objectIterator)(nil)

func (o *objectIterator) Next(ctx context.Context) (*openfgapb.Object, error) {
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
		return nil, handlePostgresError(err)
	case result := <-o.resultCh:
		return result, nil
	}
}

func (o *objectIterator) Stop() {
	o.rows.Close()
}
