package iterator

import (
	"context"
	"errors"
	"sync"

	"github.com/openfga/openfga/pkg/storage"
)

var ErrHeadNotSupportedFilterIterator = errors.New("head() not supported on filter iterator")

// FilterFunc is a function that determines whether an item should be included in the iterator results.
// It returns true if the item passes the filter, false otherwise.
// If an error occurs during filtering, it should be returned.
type FilterFunc[T any] func(T) (bool, error)
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

func (f *filter[T]) applyFilters(entry T) (bool, error) {
	for _, filter := range f.filters {
		passes, err := filter(entry)
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

		valid, err := f.applyFilters(entry)
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
func (f *filter[T]) Head(_ context.Context) (T, error) {
	var zero T
	return zero, ErrHeadNotSupportedFilterIterator
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
