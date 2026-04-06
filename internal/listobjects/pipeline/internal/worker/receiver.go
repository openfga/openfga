package worker

import (
	"context"
	"sync/atomic"
)

// Receiver is a pull-based stream of values. Recv blocks until the
// next value is available, returning false when the stream is exhausted
// or the context is cancelled. Close releases any resources held by
// the receiver; it is safe to call multiple times.
type Receiver[T any] interface {
	Recv(context.Context) (T, bool)
	Close()
}

type noopReceiver[T any] struct{}

// NewEmptyReceiver returns a Receiver that is immediately exhausted.
func NewEmptyReceiver[T any]() Receiver[T] {
	return &noopReceiver[T]{}
}

// Recv always returns the zero value and false.
func (r *noopReceiver[T]) Recv(_ context.Context) (T, bool) {
	var zero T
	return zero, false
}

// Close is a no-op.
func (r *noopReceiver[T]) Close() {}

// MappingReceiver applies a transformation function to each value
// produced by an inner Receiver.
type MappingReceiver[T, U any] struct {
	inner Receiver[T]
	fn    func(T) U
}

// MapReceiver returns a Receiver that applies fn to each value from inner.
func MapReceiver[T, U any](inner Receiver[T], fn func(T) U) Receiver[U] {
	return &MappingReceiver[T, U]{
		inner: inner,
		fn:    fn,
	}
}

// Recv returns the next transformed value, or false when the inner
// receiver is exhausted.
func (r *MappingReceiver[T, U]) Recv(ctx context.Context) (U, bool) {
	var zero U
	value, ok := r.inner.Recv(ctx)
	if !ok {
		return zero, false
	}
	return r.fn(value), true
}

// Close releases the inner receiver.
func (r *MappingReceiver[T, U]) Close() {
	r.inner.Close()
}

// SliceReceiver iterates over a fixed slice of values. It is safe for
// concurrent use: Close may be called from any goroutine to stop iteration.
type SliceReceiver[T any] struct {
	inner  []T
	pos    atomic.Int64
	closed atomic.Bool
}

// NewValueReceiver returns a Receiver that yields the given values in order.
func NewValueReceiver[T any](values ...T) Receiver[T] {
	return &SliceReceiver[T]{
		inner: values,
	}
}

// NewSliceReceiver returns a SliceReceiver that yields elements from inner.
func NewSliceReceiver[T any](inner []T) *SliceReceiver[T] {
	return &SliceReceiver[T]{
		inner: inner,
	}
}

// Recv atomically claims the next position and returns the element at
// that index. Concurrent callers each receive a distinct element.
func (r *SliceReceiver[T]) Recv(ctx context.Context) (T, bool) {
	var value T
	if ctx.Err() != nil {
		return value, false
	}
	pos := r.pos.Add(1) - 1
	if r.closed.Load() || pos >= int64(len(r.inner)) {
		return value, false
	}
	value = r.inner[pos]
	return value, true
}

// Close marks the receiver as exhausted.
func (r *SliceReceiver[T]) Close() {
	r.closed.Store(true)
}
