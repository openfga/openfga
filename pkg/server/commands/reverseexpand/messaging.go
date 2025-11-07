package reverseexpand

import (
	"context"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"
)

type message[T any] struct {
	Value  T
	finite func()
}

func (m *message[T]) done() {
	if m.finite != nil {
		m.finite()
	}
}

type producer[T any] interface {
	recv(context.Context) (message[T], bool)
	seq(context.Context) iter.Seq[message[T]]
}

type consumer[T any] interface {
	send(T)
	close()
	cancel()
}

const maxPipeSize int = 100

type pipe struct {
	ch     chan group
	end    chan struct{}
	finite func()
	trk    tracker
	mu     sync.Mutex
	done   atomic.Bool
}

func newPipe(trk tracker) *pipe {
	p := &pipe{
		ch:  make(chan group, maxPipeSize),
		end: make(chan struct{}),
		trk: trk,
	}

	p.finite = sync.OnceFunc(func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.done.Store(true)
		close(p.end)
	})

	return p
}

func (p *pipe) seq(ctx context.Context) iter.Seq[message[group]] {
	return func(yield func(message[group]) bool) {
		defer p.cancel()

		for {
			msg, ok := p.recv(ctx)
			if !ok {
				break
			}

			if !yield(msg) {
				break
			}
		}
	}
}

func (p *pipe) send(g group) {
	if !p.done.Load() {
		p.mu.Lock()
		defer p.mu.Unlock()

		if p.done.Load() {
			return
		}
		p.trk.Add(1)

		select {
		case p.ch <- g:
		case <-p.end:
			p.trk.Add(-1)
		}
	}
}

func (p *pipe) recv(ctx context.Context) (message[group], bool) {
	if ctx.Err() != nil {
		return message[group]{}, false
	}

	select {
	case <-ctx.Done():
		return message[group]{}, false
	case g := <-p.ch:
		fn := func() {
			p.trk.Add(-1)
		}
		return message[group]{Value: g, finite: sync.OnceFunc(fn)}, true
	case <-p.end:
		return message[group]{}, false
	}
}

func (p *pipe) close() {
	for p.trk.Load() != 0 {
		runtime.Gosched()
	}
	p.finite()
}

func (p *pipe) cancel() {
	p.finite()

	for {
		select {
		case <-p.ch:
			p.trk.Add(-1)
		default:
			return
		}
	}
}

type staticProducer struct {
	mu     sync.Mutex
	groups []group
	pos    int
	trk    tracker
}

func (p *staticProducer) recv(ctx context.Context) (message[group], bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if ctx.Err() != nil {
		return message[group]{}, false
	}

	if p.pos == len(p.groups) {
		return message[group]{}, false
	}

	value := p.groups[p.pos]
	p.pos++

	fn := func() {
		p.trk.Add(-1)
	}

	return message[group]{Value: value, finite: sync.OnceFunc(fn)}, true
}

func (p *staticProducer) close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.pos = len(p.groups)
}

func (p *staticProducer) seq(ctx context.Context) iter.Seq[message[group]] {
	return func(yield func(message[group]) bool) {
		defer p.close()

		for {
			msg, ok := p.recv(ctx)
			if !ok {
				break
			}

			if !yield(msg) {
				break
			}
		}
	}
}

func newStaticProducer(trk tracker, groups ...group) producer[group] {
	if trk != nil {
		trk.Add(int64(len(groups)))
	}

	return &staticProducer{
		groups: groups,
		trk:    trk,
	}
}
