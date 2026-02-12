package iterator

import (
	"context"
	"errors"
	"testing"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/stretchr/testify/require"
)

var _ storage.Iterator[any] = &TestIterator[any]{}

type TestIterator[T any] struct {
	items []T
	pos   int
}

func (t *TestIterator[T]) Head(_ context.Context) (T, error) {
	if t.pos >= len(t.items) {
		var zero T
		return zero, storage.ErrIteratorDone
	}
	return t.items[t.pos], nil
}

func (t *TestIterator[T]) Next(_ context.Context) (T, error) {
	if t.pos >= len(t.items) {
		var zero T
		return zero, storage.ErrIteratorDone
	}
	value := t.items[t.pos]
	t.pos++
	return value, nil
}

func (t *TestIterator[T]) Stop() {
	t.pos = len(t.items)
}

func TestValidatingIterator(t *testing.T) {
	t.Run("Head returns ErrIteratorDone when base is empty", func(t *testing.T) {
		v := Validate(&TestIterator[int]{}, func(_ int) (bool, error) { return true, nil })
		_, err := v.Head(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("Next returns ErrIteratorDone when base is empty", func(t *testing.T) {
		v := Validate(&TestIterator[int]{}, func(_ int) (bool, error) { return true, nil })
		_, err := v.Next(context.Background())
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("Head returns value when base contains a valid value", func(t *testing.T) {
		v := Validate(&TestIterator[int]{items: []int{1}}, func(_ int) (bool, error) { return true, nil })
		value, err := v.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1, value)
	})

	t.Run("Next returns value when base contains a valid value", func(t *testing.T) {
		v := Validate(&TestIterator[int]{items: []int{1}}, func(_ int) (bool, error) { return true, nil })
		value, err := v.Next(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1, value)
	})

	t.Run("Head returns ErrIteratorDone when base is done", func(t *testing.T) {
		v := Validate(&TestIterator[int]{items: []int{1, 2, 3}}, func(_ int) (bool, error) { return true, nil })
		value, err := v.Next(context.Background())
		require.Equal(t, 1, value)
		require.NoError(t, err)
		value, err = v.Next(context.Background())
		require.Equal(t, 2, value)
		require.NoError(t, err)
		value, err = v.Next(context.Background())
		require.Equal(t, 3, value)
		require.NoError(t, err)
		value, err = v.Head(context.Background())
		require.Equal(t, 0, value)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("Next returns ErrIteratorDone when base is done", func(t *testing.T) {
		v := Validate(&TestIterator[int]{items: []int{1, 2, 3}}, func(_ int) (bool, error) { return true, nil })
		value, err := v.Next(context.Background())
		require.Equal(t, 1, value)
		require.NoError(t, err)
		value, err = v.Next(context.Background())
		require.Equal(t, 2, value)
		require.NoError(t, err)
		value, err = v.Next(context.Background())
		require.Equal(t, 3, value)
		require.NoError(t, err)
		value, err = v.Next(context.Background())
		require.Equal(t, 0, value)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("Head returns only valid value", func(t *testing.T) {
		v := Validate(&TestIterator[int]{items: []int{1, 2, 3}}, func(i int) (bool, error) { return i == 2, nil })
		value, err := v.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, 2, value)
		value, err = v.Next(context.Background())
		require.Equal(t, 2, value)
		require.NoError(t, err)
		value, err = v.Head(context.Background())
		require.Equal(t, 0, value)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("Next returns only valid value", func(t *testing.T) {
		v := Validate(&TestIterator[int]{items: []int{1, 2, 3}}, func(i int) (bool, error) { return i == 2, nil })
		value, err := v.Next(context.Background())
		require.NoError(t, err)
		require.Equal(t, 2, value)
		value, err = v.Next(context.Background())
		require.Equal(t, 0, value)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	var ErrSentinel = errors.New("a sentinel error")

	t.Run("Head returns validation error but can continue", func(t *testing.T) {
		v := Validate(&TestIterator[int]{items: []int{1, 2, 3}}, func(i int) (bool, error) {
			if i == 2 {
				return false, ErrSentinel
			}
			return true, nil
		})
		value, err := v.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1, value)
		value, err = v.Next(context.Background())
		require.Equal(t, 1, value)
		require.NoError(t, err)
		value, err = v.Head(context.Background())
		require.Equal(t, 0, value)
		require.ErrorIs(t, err, ErrSentinel)
		value, err = v.Next(context.Background())
		require.Equal(t, 0, value)
		require.ErrorIs(t, err, ErrSentinel)
		value, err = v.Head(context.Background())
		require.Equal(t, 3, value)
		require.NoError(t, err)
		value, err = v.Next(context.Background())
		require.Equal(t, 3, value)
		require.NoError(t, err)
		value, err = v.Head(context.Background())
		require.Equal(t, 0, value)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("Next returns only valid value", func(t *testing.T) {
		v := Validate(&TestIterator[int]{items: []int{1, 2, 3}}, func(i int) (bool, error) {
			if i == 2 {
				return false, ErrSentinel
			}
			return true, nil
		})
		value, err := v.Next(context.Background())
		require.Equal(t, 1, value)
		require.NoError(t, err)
		value, err = v.Next(context.Background())
		require.Equal(t, 0, value)
		require.ErrorIs(t, err, ErrSentinel)
		value, err = v.Next(context.Background())
		require.Equal(t, 3, value)
		require.NoError(t, err)
		value, err = v.Next(context.Background())
		require.Equal(t, 0, value)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})
}
