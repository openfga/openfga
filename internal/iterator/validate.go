package iterator

import (
	"context"
	"errors"

	"github.com/openfga/openfga/pkg/storage"
)

type validatingIterator[T any] struct {
	base      storage.Iterator[T]
	validator func(T) (bool, error)
	lastError error
	onceValid bool
}

func Validate[T any](base storage.Iterator[T], fn func(T) (bool, error)) storage.Iterator[T] {
	return &validatingIterator[T]{base: base, validator: fn}
}

func (v *validatingIterator[T]) Head(ctx context.Context) (T, error) {
	for {
		t, err := v.base.Head(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				if v.onceValid || v.lastError == nil {
					return t, storage.ErrIteratorDone
				}
				lastError := v.lastError
				v.lastError = nil
				return t, lastError
			}
			return t, err
		}

		ok, err := v.validator(t)
		if err != nil || !ok {
			if err != nil {
				v.lastError = err
			}

			_, err := v.Next(ctx)
			if err != nil {
				return t, err
			}
			continue
		}
		v.onceValid = true
		return t, nil
	}
}

func (v *validatingIterator[T]) Next(ctx context.Context) (T, error) {
	for {
		t, err := v.base.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				if v.onceValid || v.lastError == nil {
					return t, storage.ErrIteratorDone
				}
				lastError := v.lastError
				v.lastError = nil
				return t, lastError
			}
			return t, err
		}

		ok, err := v.validator(t)
		if err != nil {
			v.lastError = err
			continue
		}

		if !ok {
			continue
		}
		v.onceValid = true
		return t, nil
	}
}

func (v *validatingIterator[T]) Stop() {
	v.base.Stop()
}
