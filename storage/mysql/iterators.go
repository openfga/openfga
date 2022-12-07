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
	rows *sql.Rows
}

var _ storage.TupleIterator = (*tupleIterator)(nil)

func (t *tupleIterator) next() (*tupleRecord, error) {
	if !t.rows.Next() {
		t.Stop()
		return nil, storage.ErrIteratorDone
	}

	var record tupleRecord
	if err := t.rows.Scan(&record.store, &record.objectType, &record.objectID, &record.relation, &record.user, &record.ulid, &record.insertedAt); err != nil {
		return nil, handleMySQLError(err)
	}

	if t.rows.Err() != nil {
		return nil, handleMySQLError(t.rows.Err())
	}

	return &record, nil
}

// toArray converts the tupleIterator to an []*openfgapb.Tuple and a possibly empty continuation token. If the
// continuation token exists it is the ulid of the last element of the returned array.
func (t *tupleIterator) toArray(opts storage.PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	defer t.Stop()

	var res []*openfgapb.Tuple
	for i := 0; i < opts.PageSize; i++ {
		tupleRecord, err := t.next()
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
	tupleRecord, err := t.next()
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
	record, err := t.next()
	if err != nil {
		return nil, err
	}
	return record.asTuple(), nil
}

func (t *tupleIterator) Stop() {
	t.rows.Close()
}

type objectIterator struct {
	rows *sql.Rows
}

var _ storage.ObjectIterator = (*objectIterator)(nil)

func (o *objectIterator) Next(ctx context.Context) (*openfgapb.Object, error) {
	if !o.rows.Next() {
		o.Stop()
		return nil, storage.ErrIteratorDone
	}

	var objectID, objectType string
	if err := o.rows.Scan(&objectType, &objectID); err != nil {
		return nil, handleMySQLError(err)
	}

	if o.rows.Err() != nil {
		return nil, handleMySQLError(o.rows.Err())
	}

	return &openfgapb.Object{
		Type: objectType,
		Id:   objectID,
	}, nil
}

func (o *objectIterator) Stop() {
	o.rows.Close()
}
