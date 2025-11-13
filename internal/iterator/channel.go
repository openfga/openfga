package iterator

import (
	"context"
	"sync"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/pkg/storage"
)

func Drain(ch <-chan *Msg) *sync.WaitGroup {
	if ch == nil {
		return nil
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for msg := range ch {
			if msg.Iter != nil {
				msg.Iter.Stop()
			}
		}
		wg.Done()
	}()
	return wg
}

func ToChannel[T any](ctx context.Context, iter storage.Iterator[T], batchSize int) chan ValueMsg[T] {
	out := make(chan ValueMsg[T], batchSize)

	go func() {
		defer close(out)
		for {
			t, err := iter.Next(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					return
				}
			}
			concurrency.TrySendThroughChannel(ctx, ValueMsg[T]{Err: err, Value: t}, out)
		}
	}()

	return out
}

type channelIterator struct {
	source  <-chan *Msg
	current storage.Iterator[string] // could easily be converted to generic
	stopped bool
}

func (c *channelIterator) Next(ctx context.Context) (string, error) {
	if c.stopped {
		return "", storage.ErrIteratorDone
	}

	for {
		// If no current iterator, fetch next from channel
		if c.current == nil {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case msg, ok := <-c.source:
				if !ok {
					return "", storage.ErrIteratorDone
				}
				if msg.Err != nil {
					return "", msg.Err
				}
				if msg.Iter != nil {
					c.current = msg.Iter
				}
				continue // Try again with new iterator
			}
		}

		// Try to get next from current iterator
		val, err := c.current.Next(ctx)
		if err != nil {
			if storage.IterIsDoneOrCancelled(err) {
				c.current.Stop()
				c.current = nil
				continue // Fetch next iterator
			}
			return "", err // Return non-done errors
		}

		return val, nil
	}
}

func (c *channelIterator) Head(ctx context.Context) (string, error) {
	if c.stopped {
		return "", storage.ErrIteratorDone
	}

	for {
		// If no current iterator, fetch next from channel
		if c.current == nil {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case msg, ok := <-c.source:
				if !ok {
					return "", storage.ErrIteratorDone
				}
				if msg.Err != nil {
					return "", msg.Err
				}
				if msg.Iter != nil {
					c.current = msg.Iter
				}
				continue
			}
		}

		// Try to get head from current iterator
		val, err := c.current.Head(ctx)
		if err != nil {
			if storage.IterIsDoneOrCancelled(err) {
				c.current.Stop()
				c.current = nil
				continue // Fetch next iterator
			}
			return "", err
		}

		return val, nil
	}
}

func (c *channelIterator) Stop() {
	if c.stopped {
		return
	}

	c.stopped = true
	if c.current != nil {
		c.current.Stop()
		c.current = nil
	}

	// Drain remaining messages and stop their iterators
	Drain(c.source)
}

func FromChannel(in chan *Msg) storage.Iterator[string] {
	return &channelIterator{
		source: in,
	}
}
