package iterator

import (
	"context"
	"errors"
	"sync"

	"github.com/openfga/openfga/pkg/storage"
)

// OperationType represents the iterator method being called.
type OperationType string

const (
	OperationNext OperationType = "next"
	OperationHead OperationType = "head"
)

// FilterFunc is a function that determines whether an item should be included in the iterator results.
// It returns true if the item passes the filter, false otherwise.
// If an error occurs during filtering, it should be returned.
type FilterFunc[T any] func(OperationType, T) (bool, error)
type filter[T any] struct {
	iter      storage.Iterator[T]
	filters   []FilterFunc[T]
	mu        *sync.Mutex
	once      *sync.Once
	lastErr   error
	onceValid bool
}

func (f *filter[T]) Stop() {
	f.once.Do(func() {
		f.iter.Stop()
	})
}

func (f *filter[T]) applyFilters(method OperationType, entry T) (bool, error) {
	for _, filter := range f.filters {
		passes, err := filter(method, entry)
		if err != nil {
			return false, err
		}
		if !passes {
			return false, nil
		}
	}
	return true, nil
}

// Next returns the next tuple that passes all filter functions.
// If none of the tuples are valid AND there are errors, returns the last error.
func (f *filter[T]) Next(ctx context.Context) (T, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var null T
	for {
		entry, err := f.iter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				if f.onceValid || f.lastErr == nil {
					return null, storage.ErrIteratorDone
				}
				lastErr := f.lastErr
				f.lastErr = nil
				return null, lastErr
			}
			return null, err
		}

		valid, err := f.applyFilters(OperationNext, entry)
		if err != nil {
			f.lastErr = err
			continue
		}

		if !valid {
			continue
		}
		f.onceValid = true
		return entry, nil
	}
}

// Head returns the next tuple that passes all filter functions without advancing.
// If none of the tuples are valid AND there are errors, returns the last error.
func (f *filter[T]) Head(ctx context.Context) (T, error) {
	var null T
	for {
		tuple, err := f.iter.Head(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				if f.onceValid || f.lastErr == nil {
					return null, storage.ErrIteratorDone
				}
				return null, f.lastErr
			}
			return null, err
		}

		valid, err := f.applyFilters(OperationHead, tuple)
		if err != nil || !valid {
			if err != nil {
				f.lastErr = err
			}
			// Note that we don't care about the item returned by Next() as this is already via Head(). We call Next() solely
			// for the purpose of getting rid of the first item.
			_, err = f.iter.Next(ctx)
			if err != nil {
				// This should never happen except if the underlying ds has error. This is because f.iter.Head() had already
				// checked whether we are at the end of list. For example, in a list of [1] (all invalid),
				// Head() will return 1. If it is invalid, Next() will return 1 and move the pointer to end of list.
				// Thus, Head() will return ErrIteratorDone next time being called.
				return null, err
			}
			continue
		}
		f.onceValid = true
		return tuple, nil
	}
}

func NewFilteredIterator[T any](iter storage.Iterator[T], filters ...FilterFunc[T]) storage.Iterator[T] {
	if len(filters) == 0 {
		return iter
	}
	return &filter[T]{
		iter:    iter,
		filters: filters,
		mu:      &sync.Mutex{},
		once:    &sync.Once{},
	}
}
