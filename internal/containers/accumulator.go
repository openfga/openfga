package containers

import (
	"iter"
	"sync"
)

// Accumulator is a struct type that can concurrently store an
// unbounded set of elements and stream those elements out to a
// consumer.
type Accumulator[T any] struct {
	tail   *node[T]
	pos    *node[T]
	posx   sync.Mutex
	nomo   *sync.Cond
	closed bool
}

func NewAccumulator[T any]() *Accumulator[T] {
	var a Accumulator[T]
	a.nomo = sync.NewCond(&a.posx)
	return &a
}

// Close informs any iter.Seq[T] value created from a call to Seq()
// that no more values should be expected. Subsequent calls to Add
// will result in a panic. It is safe to call Close multiple times
// from different goroutines.
func (a *Accumulator[T]) Close() error {
	a.posx.Lock()
	defer a.posx.Unlock()
	a.closed = true
	a.nomo.Broadcast()
	return nil
}

// Add adds values to the Accumulator[T] in the order they were
// provided. Add acquires a lock on the Accumulator when adding
// all values and signals any waiting iter.Seq[T] instances to
// wake and continue iterating.
func (a *Accumulator[T]) Add(values ...T) {
	if len(values) == 0 {
		return
	}

	var newTail *node[T]
	var tail *node[T]

	for _, i := range values {
		n := &node[T]{
			value: i,
		}
		if newTail == nil {
			newTail = n
			tail = n
			continue
		}
		tail.next = n
		tail = n
	}

	a.posx.Lock()
	defer a.posx.Unlock()

	if a.pos == nil {
		a.pos = newTail
	}

	if a.tail != nil {
		a.tail.next = newTail
	}
	a.tail = tail
	a.nomo.Signal()
}

// Seq returns an iter.Seq[T] that iterates over the elements in
// the Accumulator. As long as elements that have yet to be yielded
// are present, a lock will be held until the iterator reaches the
// head of the Accumulator. Therefore, writing is blocked while the
// iterator is actively iterating.
//
// The behavior of multiple calls to Seq() on the same Accumulator
// is unpredictable because all iter.Seq[T] values share the same
// position. It is best to only call Seq() once.
func (a *Accumulator[T]) Seq() iter.Seq[T] {
	return func(yield func(T) bool) {
		a.posx.Lock()
		defer a.posx.Unlock()

		for {
			for a.pos == nil && !a.closed {
				a.nomo.Wait()
			}

			if a.closed && a.pos == nil {
				break
			}

			if !yield(a.pos.value) {
				break
			}
			a.pos = a.pos.next
		}
	}
}
