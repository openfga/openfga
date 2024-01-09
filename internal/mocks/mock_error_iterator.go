package mocks

import (
	"context"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
)

// errorIterator is a mock iterator that returns error when calling next on the second Next call
type errorIterator[T any] struct {
	items          []T
	originalLength int
}

func (s *errorIterator[T]) Next(ctx context.Context) (T, error) {
	var val T

	if ctx.Err() != nil {
		return val, ctx.Err()
	}

	// we want to simulate returning error after the first read
	if len(s.items) != s.originalLength {
		return val, fmt.Errorf("simulated errors")
	}

	if len(s.items) == 0 {
		return val, nil
	}

	next, rest := s.items[0], s.items[1:]
	s.items = rest

	return next, nil
}

func (s *errorIterator[T]) Stop() {}

// NewErrorIterator mocks case where Next will return error after the first Next()
func NewErrorIterator(tuples []*openfgav1.Tuple) storage.TupleIterator {
	iter := &errorIterator[*openfgav1.Tuple]{
		items:          tuples,
		originalLength: len(tuples),
	}

	return iter
}
