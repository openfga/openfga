package reverseexpand

import (
	"sync"
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

type Pipe struct {
	ch   chan Group
	mu   sync.Mutex
	trk  tracker
	done bool
}

func (p *Pipe) Send(g Group) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.done {
		// fmt.Printf("CLOSED PIPE %#v", g)
		panic("called Send on a closed Pipe")
	}
	p.trk.Add(1)
	p.ch <- g
}

func (p *Pipe) Recv() (Message[Group], bool) {
	select {
	case group, ok := <-p.ch:
		if !ok {
			return Message[Group]{}, ok
		}

		fn := func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			p.trk.Add(-1)
		}
		return Message[Group]{Value: group, done: sync.OnceFunc(fn)}, ok
	default:
	}
	return Message[Group]{}, false
}

func (p *Pipe) Done() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.done && p.trk.Load() == 0
}

func (p *Pipe) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.done {
		return
	}
	close(p.ch)
	p.done = true
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
