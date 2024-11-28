package graph

import (
	"context"
	"fmt"
	"sync"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

type TupleMapperKind int64

const (
	// NestedUsersetKind is a mapper that returns the userset ID from the tuple's user field.
	NestedUsersetKind TupleMapperKind = iota
	// TTUKind is a mapper that returns the user field of the tuple.
	TTUKind
	// ObjectIDKind is mapper that returns the object field of the tuple.
	ObjectIDKind
)

// TupleMapper is an iterator that, on calls to Next and Head, returns a mapping of the tuple.
type TupleMapper interface {
	storage.Iterator[string]
}

type NestedUsersetMapper struct {
	iter storage.TupleKeyIterator
	once *sync.Once
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
	n.once.Do(func() {
		n.iter.Stop()
	})
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

type TTUMapper struct {
	iter storage.TupleKeyIterator
	once *sync.Once
}

var _ TupleMapper = (*TTUMapper)(nil)

func (n TTUMapper) Next(ctx context.Context) (string, error) {
	tupleRes, err := n.iter.Next(ctx)
	if err != nil {
		return "", err
	}
	return n.doMap(tupleRes)
}

func (n TTUMapper) Stop() {
	n.once.Do(func() {
		n.iter.Stop()
	})
}

func (n TTUMapper) Head(ctx context.Context) (string, error) {
	tupleRes, err := n.iter.Head(ctx)
	if err != nil {
		return "", err
	}
	return n.doMap(tupleRes)
}

func (n TTUMapper) doMap(t *openfgav1.TupleKey) (string, error) {
	return t.GetUser(), nil
}

type ObjectIDMapper struct {
	iter storage.TupleKeyIterator
	once *sync.Once
}

var _ TupleMapper = (*ObjectIDMapper)(nil)

func (n ObjectIDMapper) Next(ctx context.Context) (string, error) {
	tupleRes, err := n.iter.Next(ctx)
	if err != nil {
		return "", err
	}
	return n.doMap(tupleRes)
}

func (n ObjectIDMapper) Stop() {
	n.once.Do(func() {
		n.iter.Stop()
	})
}

func (n ObjectIDMapper) Head(ctx context.Context) (string, error) {
	tupleRes, err := n.iter.Head(ctx)
	if err != nil {
		return "", err
	}
	return n.doMap(tupleRes)
}

func (n ObjectIDMapper) doMap(t *openfgav1.TupleKey) (string, error) {
	return t.GetObject(), nil
}

func wrapIterator(kind TupleMapperKind, iter storage.TupleKeyIterator) TupleMapper {
	switch kind {
	case NestedUsersetKind:
		return &NestedUsersetMapper{iter: iter, once: &sync.Once{}}
	case TTUKind:
		return &TTUMapper{iter: iter, once: &sync.Once{}}
	case ObjectIDKind:
		return &ObjectIDMapper{iter: iter, once: &sync.Once{}}
	}
	return nil
}
