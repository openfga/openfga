package containers

import (
	"iter"
	"sync/atomic"
)

// node is a singly linked list element used by [Bag].
type node[T any] struct {
	value T
	next  *node[T]
}

// Bag is a lock-free, append-only collection. Values can be added
// concurrently via [Bag.Add] and drained via [Bag.Seq], but individual
// elements cannot be indexed or removed.
//
// Bag uses an atomic linked list internally, so all methods are safe
// for concurrent use. The zero value is ready to use.
type Bag[T any] struct {
	head atomic.Pointer[node[T]]
}

// Add adds the provided values to the Bag.
func (b *Bag[T]) Add(v ...T) {
	if len(v) == 0 {
		return
	}

	var newHead *node[T]
	var tail *node[T]

	for _, i := range v {
		n := &node[T]{
			value: i,
		}
		if newHead == nil {
			newHead = n
			tail = n
			continue
		}
		n.next = newHead
		newHead = n
	}

	for {
		oldHead := b.head.Load()
		tail.next = oldHead
		if b.head.CompareAndSwap(oldHead, newHead) {
			break
		}
	}
}

// Seq atomically removes all values from the Bag and returns an
// iterator that yields them in LIFO (most-recently-added-first) order.
// The returned iterator is single-use: on early break it resumes from
// the break point rather than restarting, and a fully consumed iterator
// yields nothing on subsequent calls.
func (b *Bag[T]) Seq() iter.Seq[T] {
	head := b.head.Swap(nil)
	return func(yield func(T) bool) {
		for head != nil {
			if !yield(head.value) {
				break
			}
			head = head.next
		}
	}
}

// Clear removes all elements from the Bag.
func (b *Bag[T]) Clear() {
	b.head.Store(nil)
}
