package storage

import (
	"errors"
	"sync"

	"github.com/openfga/openfga/pkg/tuple"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

var ErrIteratorDone = errors.New("iterator done")

type Iterator[T any] interface {
	// Next will return the next available item.
	Next() (T, error)
	// Stop terminates iteration over the underlying iterator.
	Stop()
}

// ObjectIterator is an iterator for Objects (type + id). It is closed by explicitly calling Stop() or by calling Next() until it
// returns an ErrIteratorDone error.
type ObjectIterator = Iterator[*openfgapb.Object]

// TupleIterator is an iterator for Tuples. It is closed by explicitly calling Stop() or by calling Next() until it
// returns an ErrIteratorDone error.
type TupleIterator = Iterator[*openfgapb.Tuple]

// TupleKeyIterator is an iterator for TupleKeys. It is closed by explicitly calling Stop() or by calling Next() until it
// returns an ErrIteratorDone error.
type TupleKeyIterator = Iterator[*openfgapb.TupleKey]

type emptyTupleIterator struct{}

var _ TupleIterator = (*emptyTupleIterator)(nil)

func (e *emptyTupleIterator) Next() (*openfgapb.Tuple, error) {
	return nil, ErrIteratorDone
}
func (e *emptyTupleIterator) Stop() {}

type uniqueObjectIterator struct {
	iter1, iter2 ObjectIterator
	objects      sync.Map
}

// NewUniqueObjectIterator returns an ObjectIterator that iterates over two ObjectIterators and yields only distinct
// objects with the duplicates removed.
//
// iter1 should generally be provided by a constrained iterator (e.g. contextual tuples) and iter2 should be provided
// by a storage iterator that already guarantees uniqueness.
func NewUniqueObjectIterator(iter1, iter2 ObjectIterator) ObjectIterator {
	return &uniqueObjectIterator{
		iter1: iter1,
		iter2: iter2,
	}
}

var _ ObjectIterator = (*uniqueObjectIterator)(nil)

// Next returns the next unique object from the two underlying iterators.
func (u *uniqueObjectIterator) Next() (*openfgapb.Object, error) {
	for {
		obj, err := u.iter1.Next()
		if err != nil {
			if err == ErrIteratorDone {
				break
			}

			return nil, err
		}

		// if the object has not already been seen, then store it and return it
		_, ok := u.objects.Load(tuple.ObjectKey(obj))
		if !ok {
			u.objects.Store(tuple.ObjectKey(obj), struct{}{})
			return obj, nil
		}

	}

	// assumption is that iter2 yields unique values to begin with
	for {
		obj, err := u.iter2.Next()
		if err != nil {
			if err == ErrIteratorDone {
				return nil, ErrIteratorDone
			}

			return nil, err
		}

		_, ok := u.objects.Load(tuple.ObjectKey(obj))
		if !ok {
			return obj, nil
		}
	}
}

func (u *uniqueObjectIterator) Stop() {
	u.iter1.Stop()
	u.iter2.Stop()
}

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
func NewStaticTupleIterator(tuples []*openfgapb.Tuple) TupleIterator {
	iter := &staticIterator[*openfgapb.Tuple]{
		items: tuples,
	}

	return iter
}

// NewStaticTupleKeyIterator returns a TupleKeyIterator that iterates over the provided slice.
func NewStaticTupleKeyIterator(tupleKeys []*openfgapb.TupleKey) TupleKeyIterator {
	iter := &staticIterator[*openfgapb.TupleKey]{
		items: tupleKeys,
	}

	return iter
}

type tupleKeyIterator struct {
	iter TupleIterator
}

var _ TupleKeyIterator = (*tupleKeyIterator)(nil)

func (t *tupleKeyIterator) Next() (*openfgapb.TupleKey, error) {
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

// NewTupleKeyObjectIterator returns an ObjectIterator that iterates over the objects
// contained in the provided list of TupleKeys.
func NewTupleKeyObjectIterator(tupleKeys []*openfgapb.TupleKey) ObjectIterator {
	objects := make([]*openfgapb.Object, 0, len(tupleKeys))
	for _, tk := range tupleKeys {
		objectType, objectID := tuple.SplitObject(tk.GetObject())
		objects = append(objects, &openfgapb.Object{Type: objectType, Id: objectID})
	}

	return NewStaticObjectIterator(objects)
}

type tupleKeyObjectIterator struct {
	iter TupleKeyIterator
}

var _ ObjectIterator = (*tupleKeyObjectIterator)(nil)

func (t *tupleKeyObjectIterator) Next() (*openfgapb.Object, error) {
	tk, err := t.iter.Next()
	if err != nil {
		return nil, err
	}
	objectType, objectID := tuple.SplitObject(tk.GetObject())
	return &openfgapb.Object{Type: objectType, Id: objectID}, nil
}

func (t *tupleKeyObjectIterator) Stop() {
	t.iter.Stop()
}

// NewObjectIteratorFromTupleKeyIterator takes a TupleKeyIterator and yields all the objects from it as a ObjectIterator.
func NewObjectIteratorFromTupleKeyIterator(iter TupleKeyIterator) ObjectIterator {
	return &tupleKeyObjectIterator{iter}
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

// NewStaticObjectIterator returns an ObjectIterator that iterates over the provided slice of objects.
func NewStaticObjectIterator(objects []*openfgapb.Object) ObjectIterator {

	return &staticIterator[*openfgapb.Object]{
		items: objects,
	}
}

// TupleKeyFilterFunc is a filter function that is used to filter out tuples from a TupleKey iterator
// that don't meet some criteria. Implementations should return true if the tuple should be returned
// and false if it should be filtered out.
type TupleKeyFilterFunc func(tupleKey *openfgapb.TupleKey) bool

type filteredTupleKeyIterator struct {
	iter   TupleKeyIterator
	filter TupleKeyFilterFunc
}

var _ TupleKeyIterator = &filteredTupleKeyIterator{}

// Next returns the next most tuple in the underlying iterator that meets
// the filter function this iterator was constructed with.
func (f *filteredTupleKeyIterator) Next() (*openfgapb.TupleKey, error) {

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
