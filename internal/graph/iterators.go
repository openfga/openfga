package graph

import (
	"context"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

// cachedTupleIterator is a wrapper around a cached iterator
// for a given object/relation.
type cachedTupleIterator struct {
	objectID   string
	objectType string
	relation   string
	iter       storage.Iterator[storage.CachedTuple]
}

var _ storage.TupleIterator = (*cachedTupleIterator)(nil)

// Next will return the next available minimal cached tuple tuple
// as a well-formed [openfgav1.Tuple].
func (c *cachedTupleIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	t, err := c.iter.Next(ctx)
	if err != nil {
		return nil, err
	}

	objectType := t.ObjectType
	objectID := t.ObjectID
	relation := t.Relation

	if c.objectType != "" {
		objectType = c.objectType
	}

	if c.objectID != "" {
		objectID = c.objectID
	}

	if c.relation != "" {
		relation = c.relation
	}

	cachedTuple := &openfgav1.Tuple{
		Key: &openfgav1.TupleKey{
			User:      t.User,
			Relation:  relation,
			Object:    tuple.BuildObject(objectType, objectID),
			Condition: t.Condition,
		},
		Timestamp: t.Timestamp,
	}

	return cachedTuple, nil
}

// Stop see [storage.Iterator].Stop.
func (c *cachedTupleIterator) Stop() {
	c.iter.Stop()
}

// Head will return the first minimal cached tuple of the iterator as
// a well-formed [openfgav1.Tuple].
func (c *cachedTupleIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	t, err := c.iter.Head(ctx)
	if err != nil {
		return nil, err
	}

	cachedTuple := &openfgav1.Tuple{
		Key: &openfgav1.TupleKey{
			User:      t.User,
			Object:    tuple.BuildObject(c.objectType, c.objectID),
			Relation:  c.relation,
			Condition: t.Condition,
		},
		Timestamp: t.Timestamp,
	}

	return cachedTuple, nil
}
