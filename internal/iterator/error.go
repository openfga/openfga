package iterator

import (
	"context"

	"github.com/openfga/openfga/pkg/storage"
)

type errorIterator[T any] struct {
	err error
}

func Error[T any](err error) storage.Iterator[T] {
	return &errorIterator[T]{err: err}
}

func (e *errorIterator[T]) Head(ctx context.Context) (T, error) {
	var t T
	return t, e.err
}

func (e *errorIterator[T]) Next(ctx context.Context) (T, error) {
	var t T
	return t, e.err
}

func (e *errorIterator[T]) Stop() {}
