package iterator

import (
	"context"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/pkg/storage"
)

type iteratorMsg[T any] struct {
	Value T
	Err   error
}

func ToChannel[T any](ctx context.Context, iter storage.Iterator[T], batchSize int) chan iteratorMsg[T] {
	out := make(chan iteratorMsg[T], batchSize)

	go func() {
		defer close(out)
		for {
			t, err := iter.Next(ctx)
			if err != nil {
				if storage.IterIsDoneOrCancelled(err) {
					return
				}
			}
			concurrency.TrySendThroughChannel(ctx, iteratorMsg[T]{Err: err, Value: t}, out)
		}
	}()

	return out
}
