package reverseexpand

import (
	"runtime"
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
	mu    sync.Mutex
	buf   [maxPipeSize]Group
	state atomic.Uint64
	trk   tracker
}

type state struct {
	head    uint16
	tail    uint16
	size    uint16
	sending bool
	done    bool
}

const (
	done uint16 = 1 << iota
	sending
)

func (p *Pipe) set(current uint64, st state) bool {
	var packed uint64
	packed |= uint64(st.head)
	packed |= uint64(st.tail) << 16
	packed |= uint64(st.size) << 32

	var flags uint16
	if st.done {
		flags |= done
	} else {
		flags &^= done
	}

	if st.sending {
		flags |= sending
	} else {
		flags &^= sending
	}

	packed |= uint64(flags) << 48

	return p.state.CompareAndSwap(current, packed)
}

func (p *Pipe) get() (uint64, state) {
	var st state

	packed := p.state.Load()
	st.head = uint16(packed & 0xFFFF)
	st.tail = uint16((packed >> 16) & 0xFFFF)
	st.size = uint16((packed >> 32) & 0xFFFF)
	flags := uint16((packed >> 48) & 0xFFFF)
	st.done = (flags & done) != 0

	return packed, st
}

func (p *Pipe) Send(g Group) {
	for {
		current, st := p.get()

		if st.done {
			panic("called Send on a closed Pipe")
		}

		if st.sending {
			runtime.Gosched()
			continue
		}

		st.sending = true

		if p.set(current, st) {
			p.trk.Add(1)
			break
		}

		runtime.Gosched()
	}

	for {
		current, st := p.get()

		if st.done {
			p.trk.Add(-1)
			panic("called Send on a closed Pipe")
		}

		if maxPipeSize == int(st.size) {
			runtime.Gosched()
			continue
		}

		head := st.head
		st.head = (st.head + 1) % uint16(maxPipeSize)
		st.size++

		st.sending = false

		p.mu.Lock()
		if !p.set(current, st) {
			p.mu.Unlock()
			runtime.Gosched()
			continue
		}
		p.buf[head] = g
		p.mu.Unlock()
		break
	}
}

func (p *Pipe) Recv() (Message[Group], bool) {
	var v Group

	for {
		current, st := p.get()

		if st.size == 0 {
			return Message[Group]{}, false
		}

		tail := st.tail
		st.tail = (st.tail + 1) % uint16(maxPipeSize)
		st.size--

		p.mu.Lock()
		if p.set(current, st) {
			v = p.buf[tail]
			p.mu.Unlock()
			break
		}
		p.mu.Unlock()

		runtime.Gosched()
	}

	fn := func() {
		p.trk.Add(-1)
	}

	return Message[Group]{Value: v, done: sync.OnceFunc(fn)}, true
}

func (p *Pipe) Done() bool {
	_, st := p.get()

	return st.done && st.size == 0 && p.trk.Load() == 0
}

func (p *Pipe) Close() {
	for {
		current, st := p.get()

		if st.done {
			break
		}

		if st.sending || st.size > 0 {
			runtime.Gosched()
			continue
		}

		st.done = true

		if p.set(current, st) {
			break
		}

		runtime.Gosched()
	}
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
