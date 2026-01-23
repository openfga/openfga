package pipeline

import (
	"context"
	"errors"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
)

type errorIterator struct {
	err error
}

func (e *errorIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	return nil, e.err
}

func (e *errorIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	return nil, e.err
}

func (e *errorIterator) Stop() {}

type validator[T any] func(value T) bool

type falibleValidator[T any] func(value T) (bool, error)

func makeValidatorFalible[T any](v validator[T]) falibleValidator[T] {
	return func(value T) (bool, error) {
		return v(value), nil
	}
}

type validatingIterator[T any] struct {
	base      storage.Iterator[T]
	validator falibleValidator[T]
	lastError error
	onceValid bool
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

func newValidatingIterator[T any](base storage.Iterator[T], validator falibleValidator[T]) storage.Iterator[T] {
	return &validatingIterator[T]{base: base, validator: validator}
}

func combineValidators[T any](validators []falibleValidator[T]) falibleValidator[T] {
	return func(value T) (bool, error) {
		for _, v := range validators {
			ok, err := v(value)
			if err != nil || !ok {
				return ok, err
			}
		}
		return true, nil
	}
}
