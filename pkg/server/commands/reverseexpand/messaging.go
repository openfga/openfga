package reverseexpand

import (
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
	recv() (message[T], bool)
	done() bool
}

type consumer[T any] interface {
	send(T)
	close()
}

const maxPipeSize int = 100

type pipe struct {
	ch     chan group
	end    chan struct{}
	closed atomic.Bool
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
		p.closed.Store(true)
		close(p.end)
	})

	return p
}

func (p *pipe) send(g group) {
	p.trk.Add(1)

	select {
	case p.ch <- g:
	case <-p.end:
		p.trk.Add(-1)
		panic("called send on a closed pipe")
	}
}

func (p *pipe) recv() (message[group], bool) {
	select {
	case g, ok := <-p.ch:
		if !ok {
			return message[group]{}, false
		}

		fn := func() {
			p.trk.Add(-1)
		}
		return message[group]{Value: g, finite: sync.OnceFunc(fn)}, true
	case <-p.end:
		return message[group]{}, false
	}
}

func (p *pipe) done() bool {
	return p.closed.Load() && p.trk.Load() == 0
}

func (p *pipe) close() {
	p.finite()
}

type staticProducer struct {
	groups []group
	pos    int
	trk    tracker
}

func (s *staticProducer) recv() (message[group], bool) {
	if s.pos < len(s.groups) {
		value := s.groups[s.pos]
		s.pos++

		fn := func() {
			s.trk.Add(-1)
		}
		return message[group]{Value: value, finite: sync.OnceFunc(fn)}, true
	}
	return message[group]{}, false
}

func (s *staticProducer) done() bool {
	return s.pos >= len(s.groups)
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
