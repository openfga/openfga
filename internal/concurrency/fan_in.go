package concurrency

import (
	"context"
	"sync"
)

func Drain[T any](ch <-chan T, drain func(T)) *sync.WaitGroup {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for msg := range ch {
			drain(msg)
		}
		wg.Done()
	}()
	return wg
}

func FanInChannels[T any](ctx context.Context, chans []<-chan T, onFail func(T)) <-chan T {
	limit := len(chans)

	out := make(chan T, limit)

	if limit == 0 {
		close(out)
		return out
	}

	pool := NewPool(ctx, limit)

	for _, c := range chans {
		pool.Go(func(ctx context.Context) error {
			for v := range c {
				if !TrySendThroughChannel(ctx, v, out) {
					if onFail != nil {
						onFail(v)
					}
				}
			}
			return nil
		})
	}

	go func() {
		// NOTE: the consumer of this channel will block waiting for it to close
		_ = pool.Wait()
		close(out)
	}()

	return out
}
