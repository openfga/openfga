package storage

import (
	"context"
	"errors"
	"sync"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

// ErrIteratorDone is returned when the iterator has finished iterating through all the items.
var ErrIteratorDone = errors.New("iterator done")

// Iterator is a generic interface defining methods for
// iterating over a collection of items of type T.
type Iterator[T any] interface {
	// Next will return the next available
	// item or ErrIteratorDone if no more
	// items are available.
	Next(ctx context.Context) (T, error)

	// Stop terminates iteration over
	// the underlying iterator.
	Stop()

	// Head will return the first item or ErrIteratorDone if the iterator
	// is empty.
	// It's possible for this method to advance the iterator internally, but a subsequent call to Next will not miss any results.
	// Calling Head() continuously without calling Next() will yield the same result (the first one) over and over.
	Head(ctx context.Context) (T, error)
}

// TupleIterator is an iterator for [*openfgav1.Tuple](s).
// It is closed by explicitly calling [Iterator.Stop] or by calling
// [Iterator.Next] until it returns an [ErrIteratorDone] error.
type TupleIterator = Iterator[*openfgav1.Tuple]

// TupleKeyIterator is an iterator for [*openfgav1.TupleKey](s). It is closed by
// explicitly calling [Iterator.Stop] or by calling [Iterator.Next] until it
// returns an [ErrIteratorDone] error.
type TupleKeyIterator = Iterator[*openfgav1.TupleKey]

type combinedIterator[T any] struct {
	mu      *sync.Mutex
	once    *sync.Once
	pending []Iterator[T]
}

var _ Iterator[any] = (*combinedIterator[any])(nil)

// Next see [Iterator.Next].
func (c *combinedIterator[T]) Next(ctx context.Context) (T, error) {
	if len(c.pending) == 0 {
		// All iterators ended.
		var val T
		return val, ErrIteratorDone
	}

	c.mu.Lock()
	iter := c.pending[0]
	val, err := iter.Next(ctx)
	if err != nil {
		if errors.Is(err, ErrIteratorDone) {
			c.pending = c.pending[1:]
			iter.Stop() // clean up before dropping the reference
			c.mu.Unlock()
			return c.Next(ctx)
		}
		c.mu.Unlock()
		return val, err
	}

	c.mu.Unlock()
	return val, nil
}

// Stop see [Iterator.Stop].
func (c *combinedIterator[T]) Stop() {
	c.once.Do(func() {
		c.mu.Lock()
		for _, iter := range c.pending {
			iter.Stop()
		}
		c.mu.Unlock()
	})
}

// Head see [Iterator.Head].
func (c *combinedIterator[T]) Head(ctx context.Context) (T, error) {
	if len(c.pending) == 0 {
		// All iterators ended.
		var val T
		return val, ErrIteratorDone
	}

	c.mu.Lock()
	iter := c.pending[0]
	val, err := iter.Head(ctx)
	if err != nil {
		if errors.Is(err, ErrIteratorDone) {
			c.pending = c.pending[1:]
			iter.Stop()
			c.mu.Unlock()
			return c.Head(ctx)
		}
		c.mu.Unlock()
		return val, err
	}
	c.mu.Unlock()
	return val, nil
}

// NewCombinedIterator takes generic iterators of a given type T
// and combines them into a single iterator that yields all the
// values from all iterators. Duplicates can be returned.
func NewCombinedIterator[T any](iters ...Iterator[T]) Iterator[T] {
	pending := make([]Iterator[T], 0, len(iters))
	for _, iter := range iters {
		if iter != nil {
			pending = append(pending, iter)
		}
	}
	return &combinedIterator[T]{pending: pending, once: &sync.Once{}, mu: &sync.Mutex{}}
}

// NewStaticTupleIterator returns a [TupleIterator] that iterates over the provided slice.
func NewStaticTupleIterator(tuples []*openfgav1.Tuple) TupleIterator {
	iter := &StaticIterator[*openfgav1.Tuple]{
		items: tuples,
	}

	return iter
}

// NewStaticTupleKeyIterator returns a [TupleKeyIterator] that iterates over the provided slice.
func NewStaticTupleKeyIterator(tupleKeys []*openfgav1.TupleKey) TupleKeyIterator {
	iter := &StaticIterator[*openfgav1.TupleKey]{
		items: tupleKeys,
	}

	return iter
}

type tupleKeyIterator struct {
	iter TupleIterator
	once *sync.Once
}

var _ TupleKeyIterator = (*tupleKeyIterator)(nil)

// Next see [Iterator.Next].
func (t *tupleKeyIterator) Next(ctx context.Context) (*openfgav1.TupleKey, error) {
	tuple, err := t.iter.Next(ctx)
	if err != nil {
		return nil, err
	}
	return tuple.GetKey(), nil
}

// Stop see [Iterator.Stop].
func (t *tupleKeyIterator) Stop() {
	t.once.Do(func() {
		t.iter.Stop()
	})
}

// Head see [Iterator.Head].
func (t *tupleKeyIterator) Head(ctx context.Context) (*openfgav1.TupleKey, error) {
	tuple, err := t.iter.Head(ctx)
	if err != nil {
		return nil, err
	}
	return tuple.GetKey(), nil
}

// NewTupleKeyIteratorFromTupleIterator takes a [TupleIterator] and yields
// all the [*openfgav1.TupleKey](s) from it as a [TupleKeyIterator].
func NewTupleKeyIteratorFromTupleIterator(iter TupleIterator) TupleKeyIterator {
	return &tupleKeyIterator{iter, &sync.Once{}}
}

type StaticIterator[T any] struct {
	items []T
}

// Next see [Iterator.Next].
func (s *StaticIterator[T]) Next(ctx context.Context) (T, error) {
	var val T

	if ctx.Err() != nil {
		return val, ctx.Err()
	}

	if len(s.items) == 0 {
		return val, ErrIteratorDone
	}

	next, rest := s.items[0], s.items[1:]
	s.items = rest

	return next, nil
}

// Stop see [Iterator.Stop].
func (s *StaticIterator[T]) Stop() {}

// Head see [Iterator.Head].
func (s *StaticIterator[T]) Head(ctx context.Context) (T, error) {
	var val T

	if ctx.Err() != nil {
		return val, ctx.Err()
	}

	if len(s.items) == 0 {
		return val, ErrIteratorDone
	}

	return s.items[0], nil
}

func NewStaticIterator[T any](items []T) Iterator[T] {
	return &StaticIterator[T]{items: items}
}

// TupleKeyFilterFunc is a filter function that is used to filter out
// tuples from a [TupleKeyIterator] that don't meet certain criteria.
// Implementations should return true if the tuple should be returned
// and false if it should be filtered out.
type TupleKeyFilterFunc func(tupleKey *openfgav1.TupleKey) bool

type filteredTupleKeyIterator struct {
	iter   TupleKeyIterator
	filter TupleKeyFilterFunc
	once   *sync.Once
}

var _ TupleKeyIterator = (*filteredTupleKeyIterator)(nil)

// Next returns the next most tuple in the underlying iterator that meets
// the filter function this iterator was constructed with.
func (f *filteredTupleKeyIterator) Next(ctx context.Context) (*openfgav1.TupleKey, error) {
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

// Stop see [Iterator.Stop].
func (f *filteredTupleKeyIterator) Stop() {
	f.once.Do(func() {
		f.iter.Stop()
	})
}

// Head returns the next most tuple in the underlying iterator that meets
// the filter function this iterator was constructed with.
// Note: the underlying iterator for unmatched filter may advance until filter is satisfied.
func (f *filteredTupleKeyIterator) Head(ctx context.Context) (*openfgav1.TupleKey, error) {
	for {
		tuple, err := f.iter.Head(ctx)
		if err != nil {
			return nil, err
		}

		if f.filter(tuple) {
			return tuple, nil
		}
		_, err = f.iter.Next(ctx)
		if err != nil {
			return nil, err
		}
	}
}

// NewFilteredTupleKeyIterator returns a [TupleKeyIterator] that filters out all
// [*openfgav1.Tuple](s) that don't meet the conditions of the provided [TupleKeyFilterFunc].
func NewFilteredTupleKeyIterator(iter TupleKeyIterator, filter TupleKeyFilterFunc) TupleKeyIterator {
	return &filteredTupleKeyIterator{
		iter,
		filter,
		&sync.Once{},
	}
}

// TupleKeyConditionFilterFunc is a filter function that is used to filter out
// tuples from a [TupleKeyIterator] that don't meet the tuple the conditions provided by the request.
// Implementations should return true if the tuple should be returned
// and false if it should be filtered out.
// Errors will be treated as false. If none of the tuples are valid AND there are errors, Next() will return
// the last error.
type TupleKeyConditionFilterFunc func(tupleKey *openfgav1.TupleKey) (bool, error)

type ConditionsFilteredTupleKeyIterator struct {
	iter      TupleKeyIterator
	filter    TupleKeyConditionFilterFunc
	lastError error
	onceValid bool
	once      *sync.Once
}

var _ TupleKeyIterator = (*ConditionsFilteredTupleKeyIterator)(nil)

// Next returns the next most tuple in the underlying iterator that meets
// the filter function this iterator was constructed with.
// This function is not thread-safe.
func (f *ConditionsFilteredTupleKeyIterator) Next(ctx context.Context) (*openfgav1.TupleKey, error) {
	for {
		tuple, err := f.iter.Next(ctx)
		if err != nil {
			if errors.Is(err, ErrIteratorDone) {
				if f.onceValid || f.lastError == nil {
					return nil, ErrIteratorDone
				}
				lastError := f.lastError
				f.lastError = nil
				return nil, lastError
			}
			return nil, err
		}

		valid, err := f.filter(tuple)
		if err != nil {
			f.lastError = err
			continue
		}
		if !valid {
			continue
		}
		f.onceValid = true
		return tuple, nil
	}
}

// Stop see [Iterator.Stop].
func (f *ConditionsFilteredTupleKeyIterator) Stop() {
	f.once.Do(func() {
		f.iter.Stop()
	})
}

// Head returns the next most tuple in the underlying iterator that meets
// the filter function this iterator was constructed with.
// The underlying iterator may advance but calling consecutive Head will yield consistent result.
// Further, calling Head following by Next will also yield consistent result.
// This function is not thread-safe.
func (f *ConditionsFilteredTupleKeyIterator) Head(ctx context.Context) (*openfgav1.TupleKey, error) {
	for {
		tuple, err := f.iter.Head(ctx)
		if err != nil {
			if errors.Is(err, ErrIteratorDone) {
				if f.onceValid || f.lastError == nil {
					return nil, ErrIteratorDone
				}
				return nil, f.lastError
			}
			return nil, err
		}

		valid, err := f.filter(tuple)
		if err != nil || !valid {
			if err != nil {
				f.lastError = err
			}
			// Note that we don't care about the item returned by Next() as this is already via Head(). We call Next() solely
			// for the purpose of getting rid of the first item.
			_, err = f.iter.Next(ctx)
			if err != nil {
				// This should never happen except if the underlying ds has error. This is because f.iter.Head() had already
				// checked whether we are at the end of list. For example, in a list of [1] (all invalid),
				// Head() will return 1. If it is invalid, Next() will return 1 and move the pointer to end of list.
				// Thus, Head() will return ErrIteratorDone next time being called.
				return nil, err
			}
			continue
		}
		f.onceValid = true
		return tuple, nil
	}
}

// NewConditionsFilteredTupleKeyIterator returns a [TupleKeyIterator] that filters out all
// [*openfgav1.Tuple](s) that don't meet the conditions of the provided [TupleKeyFilterFunc].
func NewConditionsFilteredTupleKeyIterator(iter TupleKeyIterator, filter TupleKeyConditionFilterFunc) TupleKeyIterator {
	return &ConditionsFilteredTupleKeyIterator{
		iter:   iter,
		filter: filter,
		once:   &sync.Once{},
	}
}

// IterIsDoneOrCancelled is true if the error is due to done or cancelled or deadline exceeded.
func IterIsDoneOrCancelled(err error) bool {
	return errors.Is(err, ErrIteratorDone) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

type OrderedCombinedIterator struct {
	mu      *sync.Mutex
	once    *sync.Once
	pending []TupleIterator // GUARDED_BY(mu)
	mapper  TupleMapper
}

var _ TupleIterator = (*OrderedCombinedIterator)(nil)

type TupleMapper func(t *openfgav1.Tuple) string

func UserMapper() TupleMapper {
	return func(t *openfgav1.Tuple) string {
		return t.GetKey().GetUser()
	}
}

func ObjectMapper() TupleMapper {
	return func(t *openfgav1.Tuple) string {
		return t.GetKey().GetObject()
	}
}

// NewOrderedCombinedIterator is a thread-safe iterator that combines a list of iterators into a single ordered iterator.
// All the input iterators must be individually ordered already according to mapper.
// Iterators can yield the same value (as defined by mapper) multiple times, but it will only be returned once.
func NewOrderedCombinedIterator(mapper TupleMapper, sortedIters ...TupleIterator) *OrderedCombinedIterator {
	pending := make([]TupleIterator, 0, len(sortedIters))
	for _, sortedIter := range sortedIters {
		if sortedIter != nil {
			pending = append(pending, sortedIter)
		}
	}
	return &OrderedCombinedIterator{pending: pending, once: &sync.Once{}, mu: &sync.Mutex{}, mapper: mapper}
}

func (c *OrderedCombinedIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	head, iteratorsToMove, err := c.head(ctx)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, iterIndex := range iteratorsToMove {
		_, err = c.pending[iterIndex].Next(ctx)
		if err != nil {
			if errors.Is(err, ErrIteratorDone) {
				c.pending[iterIndex] = nil
				continue
			}
			return nil, err
		}
	}

	return head, nil
}

func (c *OrderedCombinedIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	head, _, err := c.head(ctx)
	return head, err
}

func (c *OrderedCombinedIterator) head(ctx context.Context) (*openfgav1.Tuple, []int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	heads := make([]*openfgav1.Tuple, 0, len(c.pending))
	headIdxToPendingIdx := make(map[int]int, len(c.pending))
	for i := range c.pending {
		if c.pending[i] == nil {
			continue
		}
		head, err := c.pending[i].Head(ctx)
		if err != nil {
			if errors.Is(err, ErrIteratorDone) {
				c.pending[i] = nil
				continue
			}
			return nil, []int{}, err
		}
		heads = append(heads, head)
		headIdxToPendingIdx[len(heads)-1] = i
	}

	if len(heads) == 0 {
		return nil, []int{}, ErrIteratorDone
	}

	// Pick the minimum element.
	headMin := heads[0]
	for i, h := range heads {
		if c.mapper(headMin) > c.mapper(h) {
			headMin = heads[i]
		}
	}

	// Gather all iterators that have the same head.
	indexesWithSameHead := make([]int, 0, len(heads))
	for i, h := range heads {
		if c.mapper(headMin) == c.mapper(h) {
			indexesWithSameHead = append(indexesWithSameHead, headIdxToPendingIdx[i])
		}
	}

	return headMin, indexesWithSameHead, nil
}

func (c *OrderedCombinedIterator) Stop() {
	c.once.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, iter := range c.pending {
			if iter != nil {
				iter.Stop()
			}
		}
	})
}
