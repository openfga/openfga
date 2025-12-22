package pipe

import (
	"errors"
	"io"
	"iter"
	"sync"

	"github.com/openfga/openfga/internal/bitutil"
)

var ErrInvalidSize = errors.New("pipe size must be a power of two")

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

type Pipe[T any] struct {
	data      []T
	head      uint
	tail      uint
	done      bool
	mu        sync.Mutex
	condFull  *sync.Cond
	condEmpty *sync.Cond
}

// New is a function that instantiates a new Pipe with a size of n.
// The value of n must be a valid power of two. Any other value will
// result in an error.
func New[T any](n int) (*Pipe[T], error) {
	if !bitutil.PowerOfTwo(n) {
		return nil, ErrInvalidSize
	}
	var p Pipe[T]
	p.data = make([]T, n)
	p.condFull = sync.NewCond(&p.mu)
	p.condEmpty = sync.NewCond(&p.mu)
	return &p, nil
}

// Must is a function that returns a new instance of a Pipe, or panics
// if an error is encountered.
func Must[T any](n int) *Pipe[T] {
	p, err := New[T](n)
	if err != nil {
		panic(err)
	}
	return p
}

func (p *Pipe[T]) empty() bool {
	return p.head == p.tail
}

func (p *Pipe[T]) full() bool {
	return (p.head - p.tail) == uint(len(p.data))
}

func (p *Pipe[T]) mask(value uint) uint {
	return value & (uint(len(p.data)) - 1)
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

	p.data[p.mask(p.head)] = item
	p.head++

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

	*t = p.data[p.mask(p.tail)]
	p.tail++

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
