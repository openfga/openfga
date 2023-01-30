package storage

import (
	"context"
	"errors"
	"sync"

	"github.com/openfga/openfga/pkg/tuple"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

var ErrIteratorDone = errors.New("iterator done")

type Iterator[T any] interface {
	// Next will return the next available item. If the context is cancelled or times out, it should return ErrIteratorDone
	Next(ctx context.Context) (T, error)
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

func (e *emptyTupleIterator) Next(ctx context.Context) (*openfgapb.Tuple, error) {
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

// Next returns the next most unique object from the two underlying iterators.
// If the context is cancelled or times out, it should return ErrIteratorDone
func (u *uniqueObjectIterator) Next(ctx context.Context) (*openfgapb.Object, error) {
	for {
		obj, err := u.iter1.Next(ctx)
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
		obj, err := u.iter2.Next(ctx)
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
	iter1, iter2 Iterator[T]
}

func (c *combinedIterator[T]) Next(ctx context.Context) (T, error) {
	val, err := c.iter1.Next(ctx)
	if err != nil {
		if !errors.Is(err, ErrIteratorDone) {
			return val, err
		}
	} else {
		return val, nil
	}

	val, err = c.iter2.Next(ctx)
	if err != nil {
		if !errors.Is(err, ErrIteratorDone) {
			return val, err
		}
	}

	return val, err
}

func (c *combinedIterator[T]) Stop() {
	c.iter1.Stop()
	c.iter2.Stop()
}

// NewCombinedIterator takes two generic iterators of a given type T and combines them into a single iterator that yields
// all of the values from both iterators. If the two iterators yield the same value then duplicates will be returned.
func NewCombinedIterator[T any](iter1, iter2 Iterator[T]) Iterator[T] {
	return &combinedIterator[T]{iter1, iter2}
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

func (t *tupleKeyIterator) Next(ctx context.Context) (*openfgapb.TupleKey, error) {
	tuple, err := t.iter.Next(ctx)
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

func (t *tupleKeyObjectIterator) Next(ctx context.Context) (*openfgapb.Object, error) {
	tk, err := t.iter.Next(ctx)
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

func (s *staticIterator[T]) Next(ctx context.Context) (T, error) {
	var val T
	select {
	case <-ctx.Done():
		return val, ErrIteratorDone
	default:
		if len(s.items) == 0 {
			return val, ErrIteratorDone
		}

		next, rest := s.items[0], s.items[1:]
		s.items = rest

		return next, nil
	}
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
func (f *filteredTupleKeyIterator) Next(ctx context.Context) (*openfgapb.TupleKey, error) {

	for {
		tuple, err := f.iter.Next(ctx)
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
