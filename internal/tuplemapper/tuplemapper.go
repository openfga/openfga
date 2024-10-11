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
	UsersetKind MapperKind = 0
	TTUKind     MapperKind = 1
	// For tests only.
	NoOpKind MapperKind = 2
)

// New is a factory that returns the right mapper based on the request's kind.
func New(kind MapperKind, iter storage.TupleIterator) interface{} {
	switch kind {
	case NoOpKind:
		return &NoOpMapper{iter}
	case UsersetKind:
		return &UsersetMapper{iter}
	case TTUKind:
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

var _ Mapper[string] = (*UsersetMapper)(nil)

type UsersetMapper struct {
	iter storage.TupleIterator
}

func (n UsersetMapper) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	return n.iter.Next(ctx)
}

func (n UsersetMapper) Stop() {
	n.iter.Stop()
}

func (n UsersetMapper) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	return n.iter.Head(ctx)
}

func (n UsersetMapper) Map(t *openfgav1.TupleKey) (string, error) {
	usersetName, relation := tuple.SplitObjectRelation(t.GetUser())
	if relation == "" {
		// This should never happen because ReadUsersetTuples only returns usersets as users.
		return "", fmt.Errorf("unexpected userset %s with no relation", t.GetUser())
	}
	return usersetName, nil
}
