package mpsc

import (
	"iter"
	"runtime"
	"sync/atomic"
)

type kind int

const (
	end kind = iota
	data
)

type node[T any] struct {
	Value T
	Kind  kind
	Next  atomic.Pointer[node[T]]
}

// Accumulator is a lock-free MPSC (multiple-producer, single-consumer)
// queue that streams an unbounded set of elements to a consumer via
// iter.Seq[T]. Producers call Add concurrently; a single consumer
// iterates with Seq(). Close inserts a sentinel terminal node that
// causes Seq to return, and the Accumulator can then be reused.
type Accumulator[T any] struct {
	head atomic.Pointer[node[T]]
	tail *node[T]
}

func NewAccumulator[T any]() *Accumulator[T] {
	var a Accumulator[T]
	var n node[T]
	a.head.Store(&n)
	a.tail = &n
	return &a
}

// Close signals a running Seq() iterator to terminate by atomically
// swapping in a sentinel terminal node. It is safe to call Close
// multiple times from different goroutines. Values added after Close
// are not visible to the current Seq() but can be consumed by calling
// Close again followed by a new Seq().
func (a *Accumulator[T]) Close() error {
	var n node[T]
	n.Kind = end
	oldHead := a.head.Swap(&n)
	oldHead.Next.Store(&n)
	return nil
}

// Add adds values to the Accumulator[T] in the order they were
// provided.
func (a *Accumulator[T]) Add(values ...T) {
	if len(values) == 0 {
		return
	}

	var newHead *node[T]
	var head *node[T]

	for _, i := range values {
		n := &node[T]{
			Value: i,
			Kind:  data,
		}
		if newHead == nil {
			newHead = n
			head = n
			continue
		}
		head.Next.Store(n)
		head = n
	}

	oldHead := a.head.Swap(head)
	oldHead.Next.Store(newHead)
}

// Seq returns an iter.Seq[T] that yields elements in insertion order,
// spinning until new nodes appear and terminating when it encounters
// a sentinel terminal node produced by Close().
//
// After Seq() returns, the Accumulator can be reused: call Add to
// enqueue more items, Close to insert a new terminal node, and Seq
// again to consume them.
//
// Only one Seq() should be active at a time — concurrent iterators
// share the same tail position and the behavior is unpredictable.
func (a *Accumulator[T]) Seq() iter.Seq[T] {
	return func(yield func(T) bool) {
		var spinCount int

		for {
			currentTail := a.tail
			nextNode := currentTail.Next.Load()

			if nextNode == nil {
				spinCount++

				if spinCount >= 100 {
					spinCount = 0

					// Give up some processor time.
					runtime.Gosched()
				}
				continue
			}

			if nextNode.Kind == end {
				a.tail = nextNode
				break
			}

			if !yield(nextNode.Value) {
				break
			}
			a.tail = nextNode
		}
	}
}
