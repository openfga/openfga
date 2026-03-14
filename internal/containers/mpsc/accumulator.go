package mpsc

import (
	"iter"
	"runtime"
	"sync/atomic"
)

type node[T any] struct {
	Value T
	Next  atomic.Pointer[node[T]]
}

// Accumulator is a struct type that can concurrently store an
// unbounded set of elements and stream those elements out to a
// consumer.
type Accumulator[T any] struct {
	head   atomic.Pointer[node[T]]
	tail   atomic.Pointer[node[T]]
	closed atomic.Bool
}

func NewAccumulator[T any]() *Accumulator[T] {
	var a Accumulator[T]
	var n node[T]
	a.head.Store(&n)
	a.tail.Store(&n)
	return &a
}

// Close informs an iter.Seq[T] value created from a call to Seq()
// that no more values should be expected. Subsequent calls to Add
// will result in a panic. It is safe to call Close multiple times
// from different goroutines.
func (a *Accumulator[T]) Close() error {
	a.closed.Store(true)
	return nil
}

// Add adds values to the Accumulator[T] in the order they were
// provided.
func (a *Accumulator[T]) Add(values ...T) {
	if a.closed.Load() {
		panic("call to add on a closed accumulator")
	}

	if len(values) == 0 {
		return
	}

	var newTail *node[T]
	var tail *node[T]

	for _, i := range values {
		n := &node[T]{
			Value: i,
		}
		if newTail == nil {
			newTail = n
			tail = n
			continue
		}
		tail.Next.Store(n)
		tail = n
	}

	for {
		oldTail := a.tail.Load()
		if a.tail.CompareAndSwap(oldTail, tail) {
			oldTail.Next.Store(newTail)
			break
		}
	}
}

// Seq returns an iter.Seq[T] that iterates over the elements in
// the Accumulator.
//
// The behavior of multiple calls to Seq() on the same Accumulator
// is unpredictable because all iter.Seq[T] values share the same
// position. Only call Seq() once.
func (a *Accumulator[T]) Seq() iter.Seq[T] {
	return func(yield func(T) bool) {
		var spinCount int

		for {
			currentHead := a.head.Load()
			nextNode := currentHead.Next.Load()

			if nextNode == nil {
				if a.closed.Load() {
					if currentHead.Next.Load() == nil {
						break
					}
					continue
				}
				spinCount++

				if spinCount >= 100 {
					spinCount = 0

					// Give up some processor time.
					runtime.Gosched()
				}
				continue
			}

			if !yield(nextNode.Value) {
				break
			}
			a.head.Store(nextNode)
		}
	}
}
