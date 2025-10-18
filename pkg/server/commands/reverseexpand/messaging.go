package reverseexpand

import (
	"iter"
	"runtime"
	"sync"
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
	recv() (message[T], bool)
	seq() iter.Seq[message[T]]
}

type consumer[T any] interface {
	send(T) bool
	close()
}

const maxPipeSize int = 100

type pipe struct {
	ch     chan group
	end    chan struct{}
	finite func()
	trk    tracker
}

func newPipe(trk tracker) *pipe {
	p := &pipe{
		ch:  make(chan group, maxPipeSize),
		end: make(chan struct{}),
		trk: trk,
	}

	p.finite = sync.OnceFunc(func() {
		close(p.end)
	})

	return p
}

func (p *pipe) seq() iter.Seq[message[group]] {
	return func(yield func(message[group]) bool) {
		defer p.close()

		for {
			msg, ok := p.recv()
			if !ok {
				break
			}

			if !yield(msg) {
				break
			}
		}
	}
}

func (p *pipe) send(g group) bool {
	p.trk.Add(1)

	select {
	case p.ch <- g:
		return true
	case <-p.end:
		p.trk.Add(-1)
		return false
	}
}

func (p *pipe) recv() (message[group], bool) {
	select {
	case g, _ := <-p.ch:
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

type staticProducer struct {
	mu     sync.Mutex
	groups []group
	pos    int
	trk    tracker
}

func (p *staticProducer) recv() (message[group], bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

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

func (p *staticProducer) seq() iter.Seq[message[group]] {
	return func(yield func(message[group]) bool) {
		defer p.close()

		for {
			msg, ok := p.recv()
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
