package iterator

import (
	"context"
	"fmt"
	"sync"

	"github.com/sourcegraph/conc/panics"
	"github.com/sourcegraph/conc/pool"
	"go.uber.org/zap"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/pkg/logger"
)

type FanIn struct {
	ctx       context.Context
	logger    logger.Logger
	cancel    context.CancelFunc
	count     int
	out       chan *Msg
	addCh     chan (chan *Msg)
	mu        sync.Mutex
	accepting bool
	pool      *pool.ContextPool
	drained   chan bool
}

func NewFanIn(ctx context.Context, logger logger.Logger, limit int) *FanIn {
	ctx, cancel := context.WithCancel(ctx)

	f := &FanIn{
		ctx:       ctx,
		logger:    logger,
		cancel:    cancel,
		count:     0,
		out:       make(chan *Msg, limit),
		addCh:     make(chan (chan *Msg), limit),
		accepting: true,
		mu:        sync.Mutex{},
		pool:      concurrency.NewPool(ctx, limit),
		drained:   make(chan bool, 1),
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.ErrorWithContext(ctx, "panic recoverred",
					zap.Any("error", r),
					zap.String("function", "NewFanIn"),
				)
			}
		}()

		f.run()
	}()
	return f
}

func (f *FanIn) run() {
	defer func() {
		f.Done()
		err := f.pool.Wait()
		if err != nil {
			fmt.Printf("error waiting for pool: %v\n", err)
		}
		close(f.out)
		for ch := range f.addCh {
			drainOnExit(ch)
		}
		f.drained <- true
		close(f.drained)
	}()
	for {
		select {
		case <-f.ctx.Done():
			return
		case ch, ok := <-f.addCh:
			if !ok {
				return
			}
			f.handleChannel(ch)
		}
	}
}

func (f *FanIn) handleChannel(ch chan *Msg) {
	f.pool.Go(func(ctx context.Context) error {
		recoverredError := panics.Try(func() {
			defer drainOnExit(ch)
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-ch:
					if !ok {
						return
					}
					if !concurrency.TrySendThroughChannel(ctx, v, f.out) {
						if v.Iter != nil {
							v.Iter.Stop()
						}
					}
				}
			}
		})
		if recoverredError != nil {
			f.logger.ErrorWithContext(ctx, "panic recoverred",
				zap.Any("error", recoverredError),
				zap.String("function", "FanIn.handleChannel"),
			)
		}
		return recoverredError.AsError()
	})
}

func drainOnExit(ch chan *Msg) {
	for msg := range ch {
		if msg.Iter != nil {
			msg.Iter.Stop()
		}
	}
}

func (f *FanIn) Add(ch chan *Msg) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count++
	if !f.accepting {
		drainOnExit(ch)
		return
	}
	if !concurrency.TrySendThroughChannel(f.ctx, ch, f.addCh) {
		drainOnExit(ch)
		return
	}
}

func (f *FanIn) Out() chan *Msg {
	return f.out
}

func (f *FanIn) Count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.count
}

func (f *FanIn) Done() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.accepting {
		f.accepting = false
		close(f.addCh)
	}
}

func (f *FanIn) Close() {
	// Done gets called internally
	f.cancel()
	drainOnExit(f.out)
}
