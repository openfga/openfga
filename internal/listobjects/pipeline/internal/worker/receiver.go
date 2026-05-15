package worker

import (
	"context"
	"iter"
)

// Receiver is a pull-based stream of values. Recv blocks until the
// next value is available, returning false when the stream is exhausted
// or the context is cancelled. Close releases any resources held by
// the receiver; it is safe to call multiple times. A Receiver is not
// safe for concurrent use.
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

// ValueReceiver yields a single value and then exhausts itself.
// It is not safe for concurrent use.
type ValueReceiver[T any] struct {
	value  T
	closed bool
}

// NewValueReceiver returns a Receiver that yields value exactly once.
func NewValueReceiver[T any](value T) *ValueReceiver[T] {
	return &ValueReceiver[T]{
		value: value,
	}
}

// Recv returns the value on the first call, then (zero, false) on
// subsequent calls.
func (r *ValueReceiver[T]) Recv(ctx context.Context) (T, bool) {
	var value T
	if r.closed || ctx.Err() != nil {
		return value, false
	}
	value = r.value
	r.closed = true
	return value, true
}

// Close marks the receiver as exhausted and releases the held value.
func (r *ValueReceiver[T]) Close() {
	r.closed = true
	var zero T
	r.value = zero
}

// SliceReceiver iterates over a fixed slice of values. It is NOT safe for
// concurrent use.
type SliceReceiver[T any] struct {
	inner  []T
	pos    int
	closed bool
}

// NewSliceReceiver returns a SliceReceiver that yields elements from inner.
func NewSliceReceiver[T any](inner []T) *SliceReceiver[T] {
	return &SliceReceiver[T]{
		inner: inner,
	}
}

// Recv returns the next element from the slice, advancing the position
// by one.
func (r *SliceReceiver[T]) Recv(ctx context.Context) (T, bool) {
	var value T
	if r.closed || r.pos >= len(r.inner) || ctx.Err() != nil {
		return value, false
	}
	value = r.inner[r.pos]
	r.pos++
	return value, true
}

// Close marks the receiver as exhausted.
func (r *SliceReceiver[T]) Close() {
	r.closed = true
	r.inner = nil
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
