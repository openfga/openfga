package tuplemapper

import (
	"context"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

type Mapper[ResultType any] interface {
	storage.TupleIterator
	Map(t *openfgav1.TupleKey) (ResultType, error)
}

type MapperKind int64

const (
	// NoOpKind is for tests only.
	NoOpKind MapperKind = 0

	// NestedUsersetKind is a mapper that returns the userset ID from the tuple's user field.
	NestedUsersetKind MapperKind = 1
	NestedTTUKind     MapperKind = 2
)

// New is a factory that returns the right mapper based on the request's kind.
func New(kind MapperKind, iter storage.TupleIterator) interface{} {
	switch kind {
	case NoOpKind:
		return &NoOpMapper{iter}
	case NestedUsersetKind:
		return &NestedUsersetMapper{iter}
	case NestedTTUKind:
		// TODO
		return nil
	default:
		panic("unsupported mapper kind")
	}
}

type NoOpMapper struct {
	iter storage.TupleIterator
}

func (n NoOpMapper) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	return n.iter.Next(ctx)
}

func (n NoOpMapper) Stop() {
	n.iter.Stop()
}

func (n NoOpMapper) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	return n.iter.Head(ctx)
}

func (n NoOpMapper) Map(t *openfgav1.TupleKey) (string, error) {
	return "", nil
}

var _ Mapper[string] = (*NoOpMapper)(nil)

var _ Mapper[string] = (*NestedUsersetMapper)(nil)

type NestedUsersetMapper struct {
	iter storage.TupleIterator
}

func (n NestedUsersetMapper) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	return n.iter.Next(ctx)
}

func (n NestedUsersetMapper) Stop() {
	n.iter.Stop()
}

func (n NestedUsersetMapper) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	return n.iter.Head(ctx)
}

func (n NestedUsersetMapper) Map(t *openfgav1.TupleKey) (string, error) {
	usersetName, relation := tuple.SplitObjectRelation(t.GetUser())
	if relation == "" {
		// This should never happen because ReadUsersetTuples only returns usersets as users.
		return "", fmt.Errorf("unexpected userset %s with no relation", t.GetUser())
	}
	return usersetName, nil
}
