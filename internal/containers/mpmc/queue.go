package mpmc

import (
	"context"
	"errors"
	"iter"
	"sync"
	"sync/atomic"
)

// ErrInvalidCapacity indicates that a capacity value is invalid.
var ErrInvalidCapacity = errors.New("capacity must be a power of two >= 2")

func isPowerOfTwo(n int) bool {
	return n > 0 && (n&(n-1)) == 0
}

// slot is a single element in the ring buffer. Sequence coordinates
// concurrent access: a slot is writable when Sequence == pos and
// readable when Sequence == pos+1, where pos is the caller's claimed
// head or tail position.
type slot[T any] struct {
	Sequence atomic.Int64
	Data     T
}

// Queue is a bounded, concurrency-safe FIFO buffer based on Dmitry
// Vyukov's MPMC algorithm. Multiple goroutines may call Send and Recv
// concurrently under a shared read lock, claiming positions via atomic
// CAS on head and tail and coordinating through per-slot sequence
// counters. A write lock is only acquired for buffer extension, Grow,
// and Close.
//
// Send blocks when the buffer is full; Recv blocks when it is empty.
// When extensions are configured (see [NewQueue]), the buffer doubles
// automatically on a blocking Send, up to the configured limit. A
// negative extensions value allows unlimited growth.
//
// After [Queue.Close] is called, Send and Recv return false and any
// blocked goroutines are woken. Items enqueued before Close can still
// be drained by subsequent Recv calls.
type Queue[T any] struct {
	// --- cache line 1: read-mostly, stable after init ---

	data       []slot[T]
	capacity   int
	full       chan struct{} // buffered(1); Recv signals after freeing a slot
	empty      chan struct{} // buffered(1); Send signals after publishing a value
	extensions int
	extended   int
	done       atomic.Bool

	// --- cache line 2: written on every Send/Recv ---

	mu   sync.RWMutex // write lock: extend, Grow, Close; read lock: Send, Recv
	head atomic.Int64 // next write position, CAS by senders
	tail atomic.Int64 // next read position, CAS by receivers
}

// NewQueue returns a Queue with the given initial capacity. The capacity
// must be a power of two >= 2 so that index masking can use bitwise AND.
// A minimum of 2 is required because the per-slot sequence counter cannot
// disambiguate the readable and writable states when capacity is 1.
// The extensions parameter limits how many times the buffer may double
// when full; a negative value allows unlimited growth.
//
// Each slot's sequence counter is initialized to its index, which is
// required by the Vyukov algorithm to distinguish writable from
// readable positions on the first pass through the ring.
func NewQueue[T any](capacity int, extensions int) (*Queue[T], error) {
	if capacity < 2 || !isPowerOfTwo(capacity) {
		return nil, ErrInvalidCapacity
	}
	var p Queue[T]
	p.capacity = capacity
	p.extensions = extensions
	p.full = make(chan struct{}, 1)
	p.empty = make(chan struct{}, 1)
	p.data = make([]slot[T], capacity)
	for i := range capacity {
		p.data[i].Sequence.Store(int64(i))
	}
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

// extend grows the ring buffer to n slots, which must be a power of two.
// The caller must hold the write lock. It is a no-op if the buffer is
// already large enough.
//
// The ring is compacted so that the logical tail maps to physical index 0
// in the new buffer. Existing items are copied with sequence = index+1
// (readable), and empty slots are initialized with sequence = index
// (writable), re-establishing the Vyukov invariant for the new capacity.
// Head and tail are reset to [0, currentSize) so that mask() produces
// correct indices with the new capacity.
func (p *Queue[T]) extend(n uint) {
	if uint(p.capacity) >= n {
		return
	}

	oldHead := p.head.Load()
	oldTail := p.tail.Load()
	currentSize := oldHead - oldTail

	newData := make([]slot[T], n)

	for i := range currentSize {
		oldIndex := p.mask(oldTail + i)
		newData[i].Data = p.data[oldIndex].Data
		newData[i].Sequence.Store(i + 1)
	}

	for i := currentSize; i < int64(n); i++ {
		newData[i].Sequence.Store(i)
	}

	p.data = newData

	p.tail.Store(0)
	p.head.Store(currentSize)

	p.extended++

	p.capacity = len(p.data)
}

// Grow increases the buffer capacity to at least n items, bypassing the
// extensions limit. The value of n must be a power of two; otherwise
// [ErrInvalidCapacity] is returned.
func (p *Queue[T]) Grow(n int) error {
	if !isPowerOfTwo(n) {
		return ErrInvalidCapacity
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.extend(uint(n))
	return nil
}

// mask maps a monotonically increasing counter to a valid buffer index
// using bitwise AND, which works because capacity is always a power of two.
func (p *Queue[T]) mask(value int64) int64 {
	return value & (int64(p.capacity) - 1)
}

// Size returns the approximate number of items currently buffered.
// The result may be stale by the time the caller acts on it.
func (p *Queue[T]) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return int(p.head.Load() - p.tail.Load())
}

// Capacity returns the current buffer capacity. This may increase over
// time if extensions are configured.
func (p *Queue[T]) Capacity() int {
	p.mu.RLock()
	defer p.mu.RUnlock()

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
//
// On the fast path (slot available, CAS wins) concurrent senders
// proceed without mutual exclusion under the shared read lock.
// When the buffer is full, Send either
// doubles the buffer (if extensions remain) or parks on the full channel
// until a Recv frees a slot.
func (p *Queue[T]) Send(ctx context.Context, item T) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.done.Load() || ctx.Err() != nil {
		return false
	}

	pos := p.head.Load()

	for !p.done.Load() && ctx.Err() == nil {
		cell := &p.data[p.mask(pos)]
		seq := cell.Sequence.Load()
		diff := seq - pos

		if diff == 0 {
			// Slot is writable. Try to claim it.
			if p.head.CompareAndSwap(pos, pos+1) {
				cell.Data = item
				// Publish: storing seq = pos+1 makes the slot readable.
				cell.Sequence.Store(pos + 1)
				// Wake one parked receiver, if any.
				select {
				case p.empty <- struct{}{}:
				default:
				}
				return true
			}
		} else if diff < 0 {
			// Slot's sequence is behind our position: the buffer is full
			// (a receiver has not yet recycled this slot).
			//
			// Snapshot the mutable fields under read lock, then release
			// it so that extend (write lock) or Close can proceed.
			capacity := p.capacity
			extensions := p.extensions
			extended := p.extended
			p.mu.RUnlock()
			if extensions < 0 || extended < extensions {
				// Extensions remain: try to double the buffer.
				// The capacity guard prevents a redundant extend if
				// another sender already grew the buffer.
				p.mu.Lock()
				if capacity == p.capacity && !p.done.Load() && ctx.Err() == nil {
					p.extend(uint(p.capacity) << 1)
				}
				p.mu.Unlock()
			} else {
				// Extensions exhausted: park until a receiver frees a slot.
				select {
				case <-p.full:
				case <-ctx.Done():
				}
			}
			p.mu.RLock()
			pos = p.head.Load()
		} else {
			// diff > 0: another sender claimed this position first.
			// Re-read head and retry.
			pos = p.head.Load()
		}
	}
	return false
}

// Recv dequeues and returns the next item, blocking if the buffer is
// empty. It returns false if the queue has been closed and fully
// drained, or if ctx is cancelled.
func (p *Queue[T]) Recv(ctx context.Context) (T, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	pos := p.tail.Load()

	for {
		cell := &p.data[p.mask(pos)]
		seq := cell.Sequence.Load()
		diff := seq - (pos + 1)

		if diff == 0 {
			// Slot is readable. Try to claim it.
			if p.tail.CompareAndSwap(pos, pos+1) {
				value := cell.Data
				var zero T
				cell.Data = zero
				// Recycle: storing seq = pos+capacity makes the slot
				// writable again on the next pass through the ring.
				cell.Sequence.Store(pos + int64(p.capacity))

				// Wake one parked sender, if any. The done guard
				// is required because Recv may drain items after
				// Close, at which point the full channel is closed
				// and a send would panic.
				if !p.done.Load() {
					select {
					case p.full <- struct{}{}:
					default:
					}
				}
				return value, true
			}
		} else if diff < 0 {
			// Slot has not been written yet: the buffer is empty.
			if p.done.Load() || ctx.Err() != nil {
				var zero T
				return zero, false
			}
			// Park until a sender publishes a value.
			p.mu.RUnlock()
			select {
			case <-p.empty:
			case <-ctx.Done():
			}
			p.mu.RLock()
			pos = p.tail.Load()
		} else {
			// diff > 0: another receiver claimed this position first.
			// Re-read tail and retry.
			pos = p.tail.Load()
		}
	}
}

// Close shuts down the queue. Blocked Send and Recv calls are woken and
// return false. It is safe to call Close multiple times.
func (p *Queue[T]) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.done.Swap(true) {
		close(p.empty)
		close(p.full)
	}
}
