package mpsc

import (
	"context"
	"iter"
	"sync/atomic"
)

// kind distinguishes data nodes from the sentinel that terminates the list.
type kind int

const (
	end  kind = iota // sentinel inserted by Close
	data             // node carrying a producer value
)

// node is a singly linked list element used by [Accumulator].
type node[T any] struct {
	Value T
	Kind  kind
	Next  atomic.Pointer[node[T]]
}

// Accumulator is a lock-free MPSC (multiple-producer, single-consumer)
// queue that streams an unbounded set of elements to a consumer via
// [Accumulator.Seq]. Producers call [Accumulator.Send] concurrently;
// a single consumer iterates with Seq.
//
// [Accumulator.Close] must be called only after all producers have
// completed their Send calls. It inserts a sentinel node that causes
// Seq to return. The Accumulator is single-use and cannot be reused
// after Close.
//
// Delivery is exactly-once: on early break from Seq, the last yielded
// item is consumed.
type Accumulator[T any] struct {
	head   atomic.Pointer[node[T]]
	tail   *node[T]
	signal chan struct{}
	done   chan struct{}
	closed atomic.Bool
}

// NewAccumulator returns a new, empty Accumulator ready for use.
func NewAccumulator[T any]() *Accumulator[T] {
	var a Accumulator[T]
	var n node[T]
	a.head.Store(&n)
	a.tail = &n
	a.signal = make(chan struct{}, 1)
	a.done = make(chan struct{})
	return &a
}

// Close signals a running Seq() iterator to terminate by atomically
// swapping in a sentinel terminal node and closing an internal done
// channel. Close must be called only after all producers have
// completed their Send calls.
//
// It is safe to call Close multiple times from different goroutines;
// only the first call has any effect.
func (a *Accumulator[T]) Close() {
	if a.closed.Swap(true) {
		return
	}
	var n node[T]
	n.Kind = end
	oldHead := a.head.Swap(nil)
	oldHead.Next.Store(&n)
	close(a.done)
}

// Send adds a value to the Accumulator[T].
// It is safe to call Send concurrently.
// After Close has been called, Send will always return false.
func (a *Accumulator[T]) Send(value T) bool {
	head := node[T]{
		Value: value,
		Kind:  data,
	}

	var sent bool

	for {
		currentHead := a.head.Load()
		if currentHead == nil {
			break
		}
		if !a.head.CompareAndSwap(currentHead, &head) {
			continue
		}
		currentHead.Next.Store(&head)
		select {
		case a.signal <- struct{}{}:
		default:
		}
		sent = true
		break
	}
	return sent
}

// Recv returns the next value from the queue, blocking until one is
// available. It returns false when the Accumulator has been closed and
// drained, or when ctx is cancelled.
func (a *Accumulator[T]) Recv(ctx context.Context) (T, bool) {
	var value T
	var ok bool

PopLoop:
	for {
		currentTail := a.tail
		nextNode := currentTail.Next.Load()

		if nextNode == nil {
			select {
			case <-a.signal:
			case <-a.done:
			case <-ctx.Done():
				break PopLoop
			}
			continue
		}

		if nextNode.Kind == end {
			break
		}

		value = nextNode.Value
		ok = true
		a.tail = nextNode
		break
	}
	return value, ok
}

// TryRecv attempts a non-blocking receive from the queue. It returns the
// next value and true if one is immediately available, or the zero value
// and false if the queue is empty. Unlike [Accumulator.Recv], it never
// blocks waiting for a producer.
func (a *Accumulator[T]) TryRecv() (T, bool) {
	var value T
	var ok bool

	currentTail := a.tail
	nextNode := currentTail.Next.Load()

	if nextNode != nil && nextNode.Kind != end {
		value = nextNode.Value
		ok = true
		a.tail = nextNode
	}
	return value, ok
}

// Seq returns an iter.Seq[T] that yields elements in insertion order,
// blocking until new nodes appear and terminating when it encounters
// a sentinel terminal node produced by Close().
//
// Delivery is exactly-once: tail advances past each yielded item
// immediately, so on early break the last yielded item is consumed.
//
// Only one Seq() should be active at a time — concurrent iterators
// share the same tail position and the behavior is unpredictable.
func (a *Accumulator[T]) Seq(ctx context.Context) iter.Seq[T] {
	return func(yield func(T) bool) {
		for {
			value, ok := a.Recv(ctx)
			if !ok {
				break
			}

			if !yield(value) {
				break
			}
		}
	}
}
