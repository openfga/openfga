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

// errorTupleIterator is a mock iterator that returns error when calling next on the second Next call.
type errorTupleIterator struct {
	items          []*openfgav1.Tuple
	originalLength int
}

func (s *errorTupleIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// we want to simulate returning error after the first read
	if len(s.items) != s.originalLength {
		return nil, ErrSimulatedError
	}

	if len(s.items) == 0 {
		return nil, nil
	}

	next, rest := s.items[0], s.items[1:]
	s.items = rest

	return next, nil
}

func (s *errorTupleIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// we want to simulate returning error after the first read
	if len(s.items) != s.originalLength {
		return nil, fmt.Errorf("simulated errors")
	}

	if len(s.items) == 0 {
		return nil, nil
	}

	return s.items[0], nil
}

func (s *errorTupleIterator) Stop() {}

var _ storage.TupleIterator = (*errorTupleIterator)(nil)

// NewErrorTupleIterator mocks case where Next will return error after the first Next()
// This TupleIterator is designed to be used in tests.
func NewErrorTupleIterator(tuples []*openfgav1.Tuple) storage.TupleIterator {
	iter := &errorTupleIterator{
		items:          tuples,
		originalLength: len(tuples),
	}

	return iter
}
