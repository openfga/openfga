package graph

import (
	"context"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

type TupleMapperKind int64

const (
	// NestedUsersetKind is a mapper that returns the userset ID from the tuple's user field.
	NestedUsersetKind TupleMapperKind = iota
	// NestedTTUKind is a mapper that returns the user field of the tuple.
	NestedTTUKind
)

// TupleMapper is an iterator that, on calls to Next and Head, returns a mapping of the tuple.
type TupleMapper interface {
	storage.Iterator[string]
}

type NestedUsersetMapper struct {
	Iter storage.TupleKeyIterator
}

func (n NestedUsersetMapper) Next(ctx context.Context) (string, error) {
	tupleRes, err := n.Iter.Next(ctx)
	if err != nil {
		return "", err
	}
	return n.doMap(tupleRes)
}

func (n NestedUsersetMapper) Stop() {
	n.Iter.Stop()
}

func (n NestedUsersetMapper) Head(ctx context.Context) (string, error) {
	tupleRes, err := n.Iter.Head(ctx)
	if err != nil {
		return "", err
	}
	return n.doMap(tupleRes)
}

func (n NestedUsersetMapper) doMap(t *openfgav1.TupleKey) (string, error) {
	usersetName, relation := tuple.SplitObjectRelation(t.GetUser())
	if relation == "" {
		// This should never happen because ReadUsersetTuples only returns usersets as users.
		return "", fmt.Errorf("unexpected userset %s with no relation", t.GetUser())
	}
	return usersetName, nil
}
