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
