package mpmc

import (
	"context"
	"errors"
	"iter"
	"sync"
)

// ErrInvalidCapacity indicates that a capacity value is not a power of two.
var ErrInvalidCapacity = errors.New("capacity must be a power of two")

func isPowerOfTwo(n int) bool {
	return n > 0 && (n&(n-1)) == 0
}

// Queue is a bounded, concurrency-safe FIFO buffer. All methods are
// safe for use by multiple goroutines.
//
// Send blocks when the buffer is full; Recv blocks when it is empty.
// When extensions are configured (see [NewQueue]), the buffer doubles
// automatically on a blocking Send, up to the configured limit. A
// negative extensions value allows unlimited growth.
//
// After [Queue.Close] is called, Send and Recv return false and any
// blocked goroutines are woken.
type Queue[T any] struct {
	// Ring buffer storage. Length must be a power of two so that
	// mask() can use bitwise AND instead of modulo.
	data []T

	capacity   int
	extensions int

	// chFull wakes senders blocked on a full buffer.
	chFull chan struct{}

	// chEmpty wakes receivers blocked on an empty buffer.
	chEmpty chan struct{}

	// head and tail are monotonically increasing write and read
	// positions. They are allowed to overflow; correct indexing
	// relies on mask() and the power-of-two buffer length.
	head uint
	tail uint

	mu       sync.Mutex
	done     bool
	extended int
}

// NewQueue returns a Queue with the given initial capacity. The capacity
// must be a power of two. The extensions parameter limits how many times
// the buffer may double when full; a negative value allows unlimited growth.
func NewQueue[T any](capacity int, extensions int) (*Queue[T], error) {
	if !isPowerOfTwo(capacity) {
		return nil, ErrInvalidCapacity
	}
	var p Queue[T]
	p.capacity = capacity
	p.extensions = extensions
	p.data = make([]T, capacity)
	p.chFull = make(chan struct{}, 1)
	p.chEmpty = make(chan struct{}, 1)
	return &p, nil
}

// MustQueue is like [NewQueue] but panics if the capacity is invalid.
func MustQueue[T any](capacity int, extensions int) *Queue[T] {
	p, err := NewQueue[T](capacity, extensions)
	if err != nil {
		panic(err)
	}
	return p
}

// extend doubles the ring buffer up to size n (which must be a power of
// two) so that senders can continue without blocking. It is a no-op if
// the queue is closed or already large enough.
func (p *Queue[T]) extend(n uint) {
	if p.done {
		return
	}

	if uint(p.capacity) >= n {
		return
	}

	currentSize := p.head - p.tail

	newData := make([]T, n)

	// Compact the ring so that tail maps to index 0 in the new buffer.
	for i := range currentSize {
		oldIndex := p.mask(p.tail + i)
		newData[i] = p.data[oldIndex]
	}

	p.data = newData

	p.tail = 0
	p.head = currentSize

	p.extended++

	p.capacity = len(p.data)

	close(p.chFull)
	p.chFull = make(chan struct{})
}

// Grow increases the buffer capacity to at least n items. The value of n
// must be a power of two; otherwise [ErrInvalidCapacity] is returned.
func (p *Queue[T]) Grow(n int) error {
	if !isPowerOfTwo(n) {
		return ErrInvalidCapacity
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.extend(uint(n))
	return nil
}

// empty reports whether the ring buffer contains no elements.
func (p *Queue[T]) empty() bool {
	return p.head == p.tail
}

// full reports whether the ring buffer has reached capacity.
// The subtraction is correct even when head has overflowed past tail
// because both are unsigned and the buffer length is a power of two.
func (p *Queue[T]) full() bool {
	return (p.head - p.tail) == uint(p.capacity)
}

// mask maps a monotonically increasing counter to a valid buffer index
// using bitwise AND, which works because capacity is always a power of two.
func (p *Queue[T]) mask(value uint) uint {
	return value & (uint(p.capacity) - 1)
}

// Size returns the number of items currently buffered in the queue.
func (p *Queue[T]) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return int(p.head - p.tail)
}

// Capacity returns the current buffer capacity. This may increase over
// time if extensions are configured.
func (p *Queue[T]) Capacity() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.capacity
}

// Seq returns an iterator that yields values as they are received from
// the queue. Breaking out of the loop closes the queue, which also
// terminates any other active Seq iterators on the same queue.
func (p *Queue[T]) Seq(ctx context.Context) iter.Seq[T] {
	return func(yield func(T) bool) {
		defer p.Close()

		for {
			msg, ok := p.Recv(ctx)
			if !ok {
				break
			}

			if !yield(msg) {
				break
			}
		}
	}
}

// Send enqueues item, blocking if the buffer is full. It returns true on
// success, or false if the queue has been closed or ctx is cancelled.
func (p *Queue[T]) Send(ctx context.Context, item T) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.done || ctx.Err() != nil {
		return false
	}

	for p.full() && !p.done && ctx.Err() == nil {
		if p.extensions < 0 || p.extended < p.extensions {
			p.extend(uint(p.capacity) << 1)
		} else {
			p.mu.Unlock()
			select {
			case <-p.chFull:
			case <-ctx.Done():
			}
			p.mu.Lock()
		}
	}

	if p.done || ctx.Err() != nil {
		return false
	}

	p.data[p.mask(p.head)] = item
	p.head++

	select {
	case p.chEmpty <- struct{}{}:
	default:
	}
	return true
}

// Recv dequeues and returns the next item, blocking if the buffer is
// empty. It returns false if the queue has been closed and drained,
// or if ctx is cancelled.
func (p *Queue[T]) Recv(ctx context.Context) (T, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var t T

	for p.empty() && !p.done && ctx.Err() == nil {
		p.mu.Unlock()
		select {
		case <-p.chEmpty:
		case <-ctx.Done():
		}
		p.mu.Lock()
	}

	if p.empty() && (p.done || ctx.Err() != nil) {
		if p.done {
			p.data = nil
		}
		return t, false
	}

	t = p.data[p.mask(p.tail)]
	var zero T
	p.data[p.mask(p.tail)] = zero
	p.tail++

	if !p.done {
		select {
		case p.chFull <- struct{}{}:
		default:
		}
	}
	return t, true
}

// Close shuts down the queue. Blocked Send and Recv calls are woken and
// return false. It is safe to call Close multiple times.
func (p *Queue[T]) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.done {
		return nil
	}

	p.done = true

	close(p.chEmpty)
	close(p.chFull)
	return nil
}
