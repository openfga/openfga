package iterator

import (
	"context"
	"errors"
	"fmt"

	"github.com/openfga/openfga/pkg/storage"
)

// Concat returns an iterator that first yields all items from iter1, then all items from iter2.
// It exhausts iter1 completely before moving to iter2.
//
// This iterator is not thread-safe and should only be consumed by a single goroutine.
func Concat[T any](iter1, iter2 storage.Iterator[T]) storage.Iterator[T] {
	return &concatIterator[T]{
		current: iter1,
		next:    iter2,
	}
}

type concatIterator[T any] struct {
	current storage.Iterator[T]
	next    storage.Iterator[T]
	done    bool
}

func (c *concatIterator[T]) Next(ctx context.Context) (T, error) {
	var zero T

	if c.done {
		return zero, storage.ErrIteratorDone
	}

	// Try to get next item from current iterator
	item, err := c.current.Next(ctx)

	// If current iterator is done, switch to next iterator
	if errors.Is(err, storage.ErrIteratorDone) {
		// If we've already switched once, we're fully done
		if c.next == nil {
			c.done = true
			return zero, storage.ErrIteratorDone
		}

		// Switch to next iterator
		c.current.Stop() // ensure we stop the current iterator before dropping reference
		c.current = c.next
		c.next = nil

		// Try to get item from the new current iterator
		return c.current.Next(ctx)
	}

	// Propagate any other error
	if err != nil {
		c.done = true
		return zero, err
	}

	return item, nil
}

func (c *concatIterator[T]) Stop() {
	if c.current != nil {
		c.current.Stop()
	}
	if c.next != nil {
		c.next.Stop()
	}
}

func (c *concatIterator[T]) Head(_ context.Context) (T, error) {
	var zero T
	return zero, fmt.Errorf("head() not supported on concat iterator")
}
