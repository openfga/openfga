package iterator

import (
	"context"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/pkg/storage"
)

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
