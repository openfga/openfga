package iterator

import (
	"context"
	"sync"

	"github.com/sourcegraph/conc/pool"

	"github.com/openfga/openfga/internal/concurrency"
)

type FanIn struct {
	ctx        context.Context
	cancel     context.CancelFunc
	out        chan *Msg
	addCh      chan (chan *Msg)
	drainQueue []chan *Msg
	accepting  bool
	mu         sync.Mutex
	wg         sync.WaitGroup
	pool       *pool.ContextPool
}

func NewFanIn(ctx context.Context, limit int) *FanIn {
	ctx, cancel := context.WithCancel(ctx)

	f := &FanIn{
		ctx:        ctx,
		cancel:     cancel,
		out:        make(chan *Msg, limit),
		addCh:      make(chan (chan *Msg), limit),
		drainQueue: make([]chan *Msg, 0),
		accepting:  true,
		mu:         sync.Mutex{},
		wg:         sync.WaitGroup{},
		pool:       concurrency.NewPool(ctx, limit),
	}

	f.wg.Add(1)
	go f.run()

	return f
}

func (f *FanIn) cleaner() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ch := range f.drainQueue {
		Drain(ch)
	}
	f.wg.Done()
}

func (f *FanIn) run() {
	defer func() {
		f.Done()
		_ = f.pool.Wait()
		close(f.out)
		for ch := range f.addCh {
			f.drainOnExit(ch)
		}
		f.mu.Lock()
		queueSize := len(f.drainQueue)
		f.mu.Unlock()
		if queueSize > 0 {
			f.wg.Add(1)
			go f.cleaner()
		}
		f.wg.Done()
	}()
	for {
		select {
		case <-f.ctx.Done():
			return
		case ch, ok := <-f.addCh:
			if !ok {
				return
			}
			f.pool.Go(func(ctx context.Context) error {
				defer f.drainOnExit(ch)
				for {
					select {
					case <-ctx.Done():
						return nil
					case v, ok := <-ch:
						if !ok {
							return nil
						}
						if !concurrency.TrySendThroughChannel(ctx, v, f.out) {
							if v.Iter != nil {
								v.Iter.Stop()
							}
						}
					}
				}
			})
		}
	}
}

func (f *FanIn) drainOnExit(ch chan *Msg) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.drainQueue = append(f.drainQueue, ch)
}

// Add will not block if the amount of messages accumulated is (limit * 2) + 1 (out, pool, buffer).
func (f *FanIn) Add(ch chan *Msg) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.accepting {
		return false
	}
	return concurrency.TrySendThroughChannel(f.ctx, ch, f.addCh)
}

func Drain(ch chan *Msg) *sync.WaitGroup {
	// sync drain
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

func (f *FanIn) Out() chan *Msg {
	return f.out
}

func (f *FanIn) Done() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.accepting {
		f.accepting = false
		close(f.addCh)
	}
}

func (f *FanIn) Stop() {
	f.cancel()
}
