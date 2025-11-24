package pipe

import (
	"iter"
	"sync"
)

type Rx[T any] interface {
	Recv(*T) bool
	Seq() iter.Seq[T]
}

type Tx[T any] interface {
	Send(T) bool
	Close()
}

const defaultPipeSize int = 1

type Pipe[T any] struct {
	data  []T
	head  int
	tail  int
	count int
	done  bool
	mu    sync.Mutex
	full  *sync.Cond
	empty *sync.Cond
	init  sync.Once
}

func (p *Pipe[T]) Grow(n int) {
	if n < 0 {
		panic("negative value provided to Pipe.Grow")
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	replacement := make([]T, len(p.data)+n)
	copy(replacement, p.data)
	p.data = replacement

	p.initialize()

	p.full.Broadcast()
}

func (p *Pipe[T]) Seq() iter.Seq[T] {
	return func(yield func(T) bool) {
		defer p.Close()

		for {
			var msg T
			ok := p.Recv(&msg)
			if !ok {
				break
			}

			if !yield(msg) {
				break
			}
		}
	}
}

func (p *Pipe[T]) initialize() {
	if len(p.data) == 0 {
		p.data = make([]T, defaultPipeSize)
	}
	p.full = sync.NewCond(&p.mu)
	p.empty = sync.NewCond(&p.mu)
}

func (p *Pipe[T]) Send(item T) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.init.Do(p.initialize)

	if p.done {
		return false
	}

	// Wait if the buffer is full.
	for p.count == len(p.data) && !p.done {
		p.full.Wait()
	}

	if p.done {
		return false
	}

	p.data[p.head] = item
	p.head = (p.head + 1) % len(p.data)
	p.count++

	// Signal that the buffer is no longer empty.
	p.empty.Signal()
	return true
}

func (p *Pipe[T]) Recv(t *T) bool {
	p.mu.Lock()

	p.init.Do(p.initialize)

	// Wait while the buffer is empty and the pipe is not yet done.
	for p.count == 0 && !p.done {
		p.empty.Wait()
	}

	if p.count == 0 && p.done {
		p.mu.Unlock()
		return false
	}

	*t = p.data[p.tail]
	p.tail = (p.tail + 1) % len(p.data)
	p.count--

	// Signal that the buffer is no longer full.
	p.full.Signal()

	p.mu.Unlock()

	return true
}

func (p *Pipe[T]) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.init.Do(p.initialize)

	p.done = true

	p.empty.Broadcast()
	p.full.Broadcast()
}

type staticRx[T any] struct {
	mu    sync.Mutex
	items []T
	pos   int
}

func (p *staticRx[T]) Recv(t *T) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.pos == len(p.items) {
		return false
	}

	*t = p.items[p.pos]
	p.pos++

	return true
}

func (p *staticRx[T]) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.pos = len(p.items)
}

func (p *staticRx[T]) Seq() iter.Seq[T] {
	return func(yield func(T) bool) {
		defer p.Close()

		for {
			var msg T
			ok := p.Recv(&msg)
			if !ok {
				break
			}

			if !yield(msg) {
				break
			}
		}
	}
}

func StaticRx[T any](items ...T) Rx[T] {
	return &staticRx[T]{
		items: items,
	}
}
