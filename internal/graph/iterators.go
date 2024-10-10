package graph

import (
	"context"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/openfga/openfga/pkg/storage"
)

// cachedUserTuple is a simple struct to hold the minimal data needed
// to store in a cached iterator.
type cachedUserTuple struct {
	user      string
	condition *openfgav1.RelationshipCondition
	timestamp *timestamppb.Timestamp
}

// cachedUserTupleIterator is a wrapper around a cached iterator
// for a given object/relation.
type cachedUserTupleIterator struct {
	object   string
	relation string
	iter     storage.Iterator[cachedUserTuple]
}

var _ storage.TupleIterator = (*cachedUserTupleIterator)(nil)

// Next will return the next available minimal cached tuple tuple
// as a well-formed [openfgav1.Tuple].
func (c *cachedUserTupleIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	t, err := c.iter.Next(ctx)
	if err != nil {
		return nil, err
	}

	cachedTuple := &openfgav1.Tuple{
		Key: &openfgav1.TupleKey{
			User:      t.user,
			Object:    c.object,
			Relation:  c.relation,
			Condition: t.condition,
		},
		Timestamp: t.timestamp,
	}

	return cachedTuple, nil
}

// Stop see [storage.Iterator].Stop.
func (c *cachedUserTupleIterator) Stop() {
	c.iter.Stop()
}

// Head will return the first minimal cached tuple of the iterator as
// a well-formed [openfgav1.Tuple].
func (c *cachedUserTupleIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	t, err := c.iter.Head(ctx)
	if err != nil {
		return nil, err
	}

	cachedTuple := &openfgav1.Tuple{
		Key: &openfgav1.TupleKey{
			User:      t.user,
			Object:    c.object,
			Relation:  c.relation,
			Condition: t.condition,
		},
		Timestamp: t.timestamp,
	}

	return cachedTuple, nil
}
