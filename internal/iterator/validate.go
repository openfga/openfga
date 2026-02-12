package iterator

import (
	"context"
	"errors"

	"github.com/openfga/openfga/pkg/storage"
)

type validatingIterator[T any] struct {
	base      storage.Iterator[T]
	validator func(T) (bool, error)
}

func Validate[T any](base storage.Iterator[T], fn func(T) (bool, error)) storage.Iterator[T] {
	return &validatingIterator[T]{base: base, validator: fn}
}

func (v *validatingIterator[T]) Head(ctx context.Context) (T, error) {
	for {
		t, err := v.base.Head(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				return t, storage.ErrIteratorDone
			}
			var zero T
			return zero, err
		}

		ok, err := v.validator(t)
		if err != nil || !ok {
			if err != nil {
				var zero T
				return zero, err
			}

			_, err := v.base.Next(ctx)
			if err != nil {
				var zero T
				return zero, err
			}
			continue
		}
		return t, nil
	}
}

func (v *validatingIterator[T]) Next(ctx context.Context) (T, error) {
	for {
		t, err := v.base.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				return t, storage.ErrIteratorDone
			}
			var zero T
			return zero, err
		}

		ok, err := v.validator(t)
		if err != nil {
			var zero T
			return zero, err
		}

		if !ok {
			continue
		}
		return t, nil
	}
}

func (v *validatingIterator[T]) Stop() {
	v.base.Stop()
}
