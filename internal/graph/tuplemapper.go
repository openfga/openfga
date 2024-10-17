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
	iter storage.TupleKeyIterator
}

var _ TupleMapper = (*NestedUsersetMapper)(nil)

func (n NestedUsersetMapper) Next(ctx context.Context) (string, error) {
	tupleRes, err := n.iter.Next(ctx)
	if err != nil {
		return "", err
	}
	return n.doMap(tupleRes)
}

func (n NestedUsersetMapper) Stop() {
	n.iter.Stop()
}

func (n NestedUsersetMapper) Head(ctx context.Context) (string, error) {
	tupleRes, err := n.iter.Head(ctx)
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

type NestedTTUMapper struct {
	iter storage.TupleKeyIterator
}

var _ TupleMapper = (*NestedTTUMapper)(nil)

func (n NestedTTUMapper) Next(ctx context.Context) (string, error) {
	tupleRes, err := n.iter.Next(ctx)
	if err != nil {
		return "", err
	}
	return n.doMap(tupleRes)
}

func (n NestedTTUMapper) Stop() {
	n.iter.Stop()
}

func (n NestedTTUMapper) Head(ctx context.Context) (string, error) {
	tupleRes, err := n.iter.Head(ctx)
	if err != nil {
		return "", err
	}
	return n.doMap(tupleRes)
}

func (n NestedTTUMapper) doMap(t *openfgav1.TupleKey) (string, error) {
	return t.GetUser(), nil
}

func wrapIterator(kind TupleMapperKind, iter storage.TupleKeyIterator) TupleMapper {
	switch kind {
	case NestedUsersetKind:
		return &NestedUsersetMapper{iter: iter}
	case NestedTTUKind:
		return &NestedTTUMapper{iter: iter}
	}
	return nil
}
