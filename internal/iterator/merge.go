package iterator

import (
	"context"
	"errors"
	"fmt"

	"github.com/openfga/openfga/pkg/storage"
)

type MergedIterator[T any] struct {
	iter1       storage.Iterator[T]
	iter2       storage.Iterator[T]
	current1    T
	current2    T
	hasNext1    bool
	hasNext2    bool
	compareFn   func(a, b T) int
	initialized bool
}

func Merge[T any](iter1, iter2 storage.Iterator[T], compareFn func(a, b T) int) storage.Iterator[T] {
	return &MergedIterator[T]{
		iter1:     iter1,
		iter2:     iter2,
		compareFn: compareFn,
	}
}

func (m *MergedIterator[T]) initialize(ctx context.Context) error {
	if m.initialized {
		return nil
	}
	m.initialized = true

	// Fetch first value from iter1
	current, err := m.iter1.Next(ctx)
	if err != nil && !errors.Is(err, storage.ErrIteratorDone) {
		return err
	}
	m.hasNext1 = !errors.Is(err, storage.ErrIteratorDone)
	m.current1 = current

	// Fetch first value from iter2
	current2, err2 := m.iter2.Next(ctx)
	if err2 != nil && !errors.Is(err2, storage.ErrIteratorDone) {
		return err2
	}
	m.hasNext2 = !errors.Is(err2, storage.ErrIteratorDone)
	m.current2 = current2
	return nil
}

func (m *MergedIterator[T]) Next(ctx context.Context) (T, error) {
	var zero T

	// Initialize on first call
	if err := m.initialize(ctx); err != nil {
		return zero, err
	}

	// Both iterators exhausted
	if !m.hasNext1 && !m.hasNext2 {
		return zero, storage.ErrIteratorDone
	}

	// Only iter2 has values
	if !m.hasNext1 {
		return m.returnFromIter2(ctx)
	}

	// Only iter1 has values
	if !m.hasNext2 {
		return m.returnFromIter1(ctx)
	}

	// Both have values, compare them
	cmp := m.compareFn(m.current1, m.current2)

	if cmp < 0 {
		return m.returnFromIter1(ctx)
	} else if cmp > 0 {
		return m.returnFromIter2(ctx)
	} else {
		// Equal values - advance both iterators to skip duplicate
		val := m.current1

		// Advance iter1
		next1, err1 := m.iter1.Next(ctx)
		if errors.Is(err1, storage.ErrIteratorDone) {
			m.hasNext1 = false
		} else if err1 != nil {
			return val, err1
		} else {
			m.current1 = next1
		}

		// Advance iter2
		next2, err2 := m.iter2.Next(ctx)
		if errors.Is(err2, storage.ErrIteratorDone) {
			m.hasNext2 = false
		} else if err2 != nil {
			return val, err2
		} else {
			m.current2 = next2
		}

		return val, nil
	}
}

func (m *MergedIterator[T]) returnFromIter1(ctx context.Context) (T, error) {
	val := m.current1
	next, err := m.iter1.Next(ctx)
	if errors.Is(err, storage.ErrIteratorDone) {
		m.hasNext1 = false
		return val, nil
	}
	if err != nil {
		return val, err
	}
	m.current1 = next
	return val, nil
}

func (m *MergedIterator[T]) returnFromIter2(ctx context.Context) (T, error) {
	val := m.current2
	next, err := m.iter2.Next(ctx)
	if errors.Is(err, storage.ErrIteratorDone) {
		m.hasNext2 = false
		return val, nil
	}
	if err != nil {
		return val, err
	}
	m.current2 = next
	return val, nil
}

func (m *MergedIterator[T]) Stop() {
	m.iter1.Stop()
	m.iter2.Stop()
}

func (m *MergedIterator[T]) Head(ctx context.Context) (T, error) {
	var zero T
	return zero, fmt.Errorf("head() not supported on merged iterator")
}
