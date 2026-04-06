package worker

import (
	"context"
	"iter"
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

// SeqReceiver adapts an [iter.Seq] into a [Receiver] using [iter.Pull].
// This allows push-based iterators to be consumed through the pull-based
// Receiver interface without requiring a dedicated goroutine at the call
// site.
//
// SeqReceiver is NOT safe for concurrent use: the pull and cancel
// functions returned by [iter.Pull] must not be called simultaneously.
type SeqReceiver[T any] struct {
	cancel func()
	pull   func() (T, bool)
}

// NewSeqReceiver converts seq into a pull-based [Receiver]. The caller
// must call [SeqReceiver.Close] when done to release the iterator
// goroutine created by [iter.Pull].
func NewSeqReceiver[T any](seq iter.Seq[T]) *SeqReceiver[T] {
	pull, cancel := iter.Pull(seq)
	return &SeqReceiver[T]{
		cancel: cancel,
		pull:   pull,
	}
}

// Recv returns the next value from the underlying iterator, or
// (zero, false) when the sequence is exhausted or the receiver
// has been closed. If the context has been cancelled the iterator
// is released immediately and (zero, false) is returned.
func (r *SeqReceiver[T]) Recv(ctx context.Context) (T, bool) {
	var zero T
	if ctx.Err() != nil {
		r.cancel()
		return zero, false
	}
	return r.pull()
}

// Close releases the iterator goroutine. It is safe to call
// multiple times; subsequent calls are no-ops.
func (r *SeqReceiver[T]) Close() {
	r.cancel()
}
