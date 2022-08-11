package postgres

import (
	"encoding/json"

	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v4"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type tupleIterator struct {
	rows pgx.Rows
}

func (t *tupleIterator) next() (*tupleRecord, error) {
	if !t.rows.Next() {
		t.Stop()
		return nil, storage.TupleIteratorDone
	}

	var record tupleRecord
	if err := t.rows.Scan(&record.store, &record.objectType, &record.objectID, &record.relation, &record.user, &record.ulid, &record.insertedAt); err != nil {
		return nil, handlePostgresError(err)
	}

	if t.rows.Err() != nil {
		return nil, handlePostgresError(t.rows.Err())
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
			if err == storage.TupleIteratorDone {
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
		if errors.Is(err, storage.TupleIteratorDone) {
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

func (t *tupleIterator) Next() (*openfgapb.Tuple, error) {
	record, err := t.next()
	if err != nil {
		return nil, err
	}
	return record.asTuple(), nil
}

func (t *tupleIterator) Stop() {
	t.rows.Close()
}

type ObjectIterator struct {
	rows pgx.Rows
}

var _ storage.ObjectIterator = (*ObjectIterator)(nil)

func (o *ObjectIterator) Next() (string, error) {
	if !o.rows.Next() {
		o.Stop()
		return "", storage.ObjectIteratorDone
	}

	var objectID, objectType string
	if err := o.rows.Scan(&objectType, &objectID); err != nil {
		return "", handlePostgresError(err)
	}

	if o.rows.Err() != nil {
		return "", handlePostgresError(o.rows.Err())
	}

	return tupleUtils.BuildObject(objectType, objectID), nil
}

func (o *ObjectIterator) Stop() {
	o.rows.Close()
}
