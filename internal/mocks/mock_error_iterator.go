package mocks

import (
	"context"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
)

var (
	ErrSimulatedError = fmt.Errorf("simulated errors")
)

// errorIterator is a mock iterator that returns error when calling next on the second Next call.
type errorIterator[T any] struct {
	items          []T
	originalLength int
}

func (s *errorIterator[T]) Next(ctx context.Context) (T, error) {
	var zero T
	if ctx.Err() != nil {
		return zero, ctx.Err()
	}

	// we want to simulate returning error after the first read
	if len(s.items) != s.originalLength {
		return zero, ErrSimulatedError
	}

	if len(s.items) == 0 {
		return zero, nil
	}

	next, rest := s.items[0], s.items[1:]
	s.items = rest

	return next, nil
}

func (s *errorIterator[T]) Head(ctx context.Context) (T, error) {
	var zero T
	if ctx.Err() != nil {
		return zero, ctx.Err()
	}

	// we want to simulate returning error after the first read
	if len(s.items) != s.originalLength {
		return zero, ErrSimulatedError
	}

	if len(s.items) == 0 {
		return zero, nil
	}

	return s.items[0], nil
}

func (s *errorIterator[T]) Stop() {}

var _ storage.TupleIterator = (*errorIterator[*openfgav1.Tuple])(nil)

// NewErrorTupleIterator mocks case where Next will return error after the first Next()
// This TupleIterator is designed to be used in tests.
func NewErrorTupleIterator(tuples []*openfgav1.Tuple) storage.TupleIterator {
	iter := &errorIterator[*openfgav1.Tuple]{
		items:          tuples,
		originalLength: len(tuples),
	}

	return iter
}

func NewErrorIterator[T any](items []T) *errorIterator[T] {
	iter := &errorIterator[T]{
		items:          items,
		originalLength: len(items),
	}

	return iter
}
