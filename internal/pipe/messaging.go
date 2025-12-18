package pipe

import (
	"io"
	"iter"
	"sync"
)

type Rx[T any] interface {
	Recv(*T) bool
	Seq() iter.Seq[T]
}

type Tx[T any] interface {
	Send(T) bool
}

type TxCloser[T any] interface {
	Tx[T]
	io.Closer
}

func powerOfTwo(n int) bool {
	return n > 0 && (n&(n-1)) == 0
}

type Pipe[T any] struct {
	data      []T
	head      uint
	tail      uint
	done      bool
	mu        sync.Mutex
	condFull  *sync.Cond
	condEmpty *sync.Cond
}

func New[T any](n int) *Pipe[T] {
	if !powerOfTwo(n) {
		panic("value provided to Pipe.Grow must be a power of 2")
	}
	var p Pipe[T]
	p.data = make([]T, n)
	p.condFull = sync.NewCond(&p.mu)
	p.condEmpty = sync.NewCond(&p.mu)
	return &p
}

func (p *Pipe[T]) size() uint {
	return p.head - p.tail
}

func (p *Pipe[T]) empty() bool {
	return p.head == p.tail
}

func (p *Pipe[T]) full() bool {
	return p.size() == uint(len(p.data))
}

func (p *Pipe[T]) mask(value uint) uint {
	return value & uint(len(p.data)-1)
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

func (p *Pipe[T]) Send(item T) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.done {
		return false
	}

	// Wait if the buffer is full.
	for p.full() && !p.done {
		p.condFull.Wait()
	}

	if p.done {
		return false
	}

	p.head++
	p.data[p.mask(p.head)] = item

	// Signal that the buffer is no longer empty.
	p.condEmpty.Signal()
	return true
}

func (p *Pipe[T]) Recv(t *T) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Wait while the buffer is empty and the pipe is not yet done.
	for p.empty() && !p.done {
		p.condEmpty.Wait()
	}

	if p.empty() && p.done {
		return false
	}

	p.tail++
	*t = p.data[p.mask(p.tail)]

	// Signal that the buffer is no longer full.
	p.condFull.Signal()
	return true
}

func (p *Pipe[T]) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.done = true

	p.condEmpty.Broadcast()
	p.condFull.Broadcast()
	return nil
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
