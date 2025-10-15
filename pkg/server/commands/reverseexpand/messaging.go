package reverseexpand

import (
	"sync"
	"sync/atomic"
)

type Message[T any] struct {
	Value T
	done  func()
}

func (m *Message[T]) Done() {
	if m.done != nil {
		m.done()
	}
}

type producer[T any] interface {
	Recv() (Message[T], bool)
	Done() bool
}

type consumer[T any] interface {
	Send(T)
	Close()
}

const maxPipeSize int = 100

type Pipe struct {
	ch     chan Group
	done   chan struct{}
	closed atomic.Bool
	finite func()
	trk    tracker
}

func NewPipe(trk tracker) *Pipe {
	p := &Pipe{
		ch:   make(chan Group, maxPipeSize),
		done: make(chan struct{}),
		trk:  trk,
	}

	p.finite = sync.OnceFunc(func() {
		p.closed.Store(true)
		close(p.done)
	})

	return p
}

func (p *Pipe) Send(g Group) {
	p.trk.Add(1)

	select {
	case p.ch <- g:
	case <-p.done:
		p.trk.Add(-1)
		panic("called Send on a closed Pipe")
	}
}

func (p *Pipe) Recv() (Message[Group], bool) {
	select {
	case g, ok := <-p.ch:
		if !ok {
			return Message[Group]{}, false
		}

		fn := func() {
			p.trk.Add(-1)
		}
		return Message[Group]{Value: g, done: sync.OnceFunc(fn)}, true
	case <-p.done:
		return Message[Group]{}, false
	}
}

func (p *Pipe) Done() bool {
	return p.closed.Load() && p.trk.Load() == 0
}

func (p *Pipe) Close() {
	p.finite()
}

type staticProducer struct {
	groups []Group
	pos    int
	trk    tracker
}

func (s *staticProducer) Recv() (Message[Group], bool) {
	if s.pos < len(s.groups) {
		value := s.groups[s.pos]
		s.pos++

		fn := func() {
			s.trk.Add(-1)
		}
		return Message[Group]{Value: value, done: sync.OnceFunc(fn)}, true
	}
	return Message[Group]{}, false
}

func (s *staticProducer) Done() bool {
	return s.pos >= len(s.groups)
}

func newStaticProducer(trk tracker, groups ...Group) producer[Group] {
	if trk != nil {
		trk.Add(int64(len(groups)))
	}

	return &staticProducer{
		groups: groups,
		trk:    trk,
	}
}
