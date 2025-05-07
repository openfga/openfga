package iterator

import (
	"context"
	"sync"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/sourcegraph/conc/pool"
)

type FanIn struct {
	ctx       context.Context
	cancel    context.CancelFunc
	count     int
	out       chan *Msg
	addCh     chan (chan *Msg)
	mu        *sync.Mutex
	accepting bool
	group     *pool.ContextPool
}

func NewFanIn(ctx context.Context, limit int) *FanIn {
	ctx, cancel := context.WithCancel(ctx)

	f := &FanIn{
		ctx:       ctx,
		cancel:    cancel,
		count:     0,
		out:       make(chan *Msg, limit),
		addCh:     make(chan (chan *Msg), limit),
		accepting: true,
		mu:        &sync.Mutex{},
		group:     concurrency.NewPool(ctx, limit),
	}

	go f.run()
	return f
}

func (f *FanIn) run() {
	defer func() {
		_ = f.group.Wait()
		close(f.out)
		f.Done()
		for ch := range f.addCh {
			drainOnExit(ch)
		}
	}()
	for {
		select {
		case <-f.ctx.Done():
			return
		case ch, ok := <-f.addCh:
			if !ok {
				return
			}
			f.group.Go(func(ctx context.Context) error {
				defer drainOnExit(ch)
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
	if !f.accepting || !concurrency.TrySendThroughChannel(f.ctx, ch, f.addCh) {
		drainOnExit(ch)
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
	f.cancel()
}
