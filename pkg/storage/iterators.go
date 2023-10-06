package storage

import (
	"errors"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

var ErrIteratorDone = errors.New("iterator done")

type Iterator[T any] interface {
	// Next will return the next available item.
	Next() (T, error)
	// Stop terminates iteration over the underlying iterator.
	Stop()
}

// TupleIterator is an iterator for Tuples. It is closed by explicitly calling Stop() or by calling Next() until it
// returns an ErrIteratorDone error.
type TupleIterator = Iterator[*openfgav1.Tuple]

// TupleKeyIterator is an iterator for TupleKeys. It is closed by explicitly calling Stop() or by calling Next() until it
// returns an ErrIteratorDone error.
type TupleKeyIterator = Iterator[*openfgav1.TupleKey]

type emptyTupleIterator struct{}

var _ TupleIterator = (*emptyTupleIterator)(nil)

func (e *emptyTupleIterator) Next() (*openfgav1.Tuple, error) {
	return nil, ErrIteratorDone
}
func (e *emptyTupleIterator) Stop() {}

type combinedIterator[T any] struct {
	iters []Iterator[T]
}

func (c *combinedIterator[T]) Next() (T, error) {
	for i, iter := range c.iters {
		if iter == nil {
			continue
		}
		val, err := iter.Next()
		if err != nil {
			if !errors.Is(err, ErrIteratorDone) {
				return val, err
			}
			c.iters[i] = nil // end of this iterator
			continue
		}

		return val, nil
	}

	// all iterators ended
	var val T
	return val, ErrIteratorDone
}

func (c *combinedIterator[T]) Stop() {
	for _, iter := range c.iters {
		if iter != nil {
			iter.Stop()
		}
	}
}

// NewCombinedIterator takes generic iterators of a given type T and combines them into a single iterator that yields
// all the values from all iterators. Duplicates can be returned.
func NewCombinedIterator[T any](iters ...Iterator[T]) Iterator[T] {
	return &combinedIterator[T]{iters}
}

// NewStaticTupleIterator returns a TupleIterator that iterates over the provided slice.
func NewStaticTupleIterator(tuples []*openfgav1.Tuple) TupleIterator {
	iter := &staticIterator[*openfgav1.Tuple]{
		items: tuples,
	}

	return iter
}

// NewStaticTupleKeyIterator returns a TupleKeyIterator that iterates over the provided slice.
func NewStaticTupleKeyIterator(tupleKeys []*openfgav1.TupleKey) TupleKeyIterator {
	iter := &staticIterator[*openfgav1.TupleKey]{
		items: tupleKeys,
	}

	return iter
}

type tupleKeyIterator struct {
	iter TupleIterator
}

var _ TupleKeyIterator = (*tupleKeyIterator)(nil)

func (t *tupleKeyIterator) Next() (*openfgav1.TupleKey, error) {
	tuple, err := t.iter.Next()
	return tuple.GetKey(), err
}

func (t *tupleKeyIterator) Stop() {
	t.iter.Stop()
}

// NewTupleKeyIteratorFromTupleIterator takes a TupleIterator and yields all of the TupleKeys from it as a TupleKeyIterator.
func NewTupleKeyIteratorFromTupleIterator(iter TupleIterator) TupleKeyIterator {
	return &tupleKeyIterator{iter}
}

type staticIterator[T any] struct {
	items []T
}

func (s *staticIterator[T]) Next() (T, error) {
	var val T
	if len(s.items) == 0 {
		return val, ErrIteratorDone
	}

	next, rest := s.items[0], s.items[1:]
	s.items = rest

	return next, nil
}

func (s *staticIterator[T]) Stop() {}

// TupleKeyFilterFunc is a filter function that is used to filter out tuples from a TupleKey iterator
// that don't meet some criteria. Implementations should return true if the tuple should be returned
// and false if it should be filtered out.
type TupleKeyFilterFunc func(tupleKey *openfgav1.TupleKey) bool

type filteredTupleKeyIterator struct {
	iter   TupleKeyIterator
	filter TupleKeyFilterFunc
}

var _ TupleKeyIterator = &filteredTupleKeyIterator{}

// Next returns the next most tuple in the underlying iterator that meets
// the filter function this iterator was constructed with.
func (f *filteredTupleKeyIterator) Next() (*openfgav1.TupleKey, error) {
	for {
		tuple, err := f.iter.Next()
		if err != nil {
			return nil, err
		}

		if f.filter(tuple) {
			return tuple, nil
		}
	}
}

func (f *filteredTupleKeyIterator) Stop() {
	f.iter.Stop()
}

// NewFilteredTupleKeyIterator returns an iterator that filters out all tuples that don't
// meet the conditions of the provided TupleFilterFunc.
func NewFilteredTupleKeyIterator(iter TupleKeyIterator, filter TupleKeyFilterFunc) TupleKeyIterator {
	return &filteredTupleKeyIterator{
		iter,
		filter,
	}
}
