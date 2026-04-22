package worker

import (
	"context"
	"fmt"
	"math/bits"
	"sync/atomic"

	"github.com/openfga/openfga/internal/containers/mpmc"
	"github.com/openfga/openfga/internal/containers/mpsc"
)

// FloorPowerOfTwo returns the largest power of two not exceeding i,
// or 2 if i is equal to or less than 2.
func FloorPowerOfTwo(i uint) int {
	if i <= 2 {
		return 2
	}
	return 1 << (bits.Len(i) - 1)
}

// Sender is the read end of a [Medium]. A worker reads from its senders
// to receive messages produced by upstream workers.
type Sender interface {
	Key() *Edge
	Recv(context.Context) (*Message, bool)
	fmt.Stringer
}

// Listener is the write end of a [Medium]. A worker writes to its listeners
// to deliver messages to downstream workers.
//
// Send transfers ownership of the message to the listener on success
// (returns true): the listener is responsible for eventually calling
// [Message.Done]. On failure (returns false), ownership remains with the
// caller, who must call [Message.Done] itself to release pooled resources.
type Listener interface {
	Close()
	Key() *Edge
	Send(context.Context, *Message) bool
	fmt.Stringer
}

// Medium is a unidirectional pipe between two workers, combining the
// [Sender] (read) and [Listener] (write) ends. All Medium variants must
// be considered multiple-producer, single-consumer.
type Medium interface {
	Sender
	Listener
}

// NewStandardMedium returns a medium for non-cyclical edges.
func NewStandardMedium(edge *Edge, capacity int) Medium {
	return NewChannelMedium(edge, capacity)
}

// NewCyclicalMedium returns a medium for cyclical edges.
func NewCyclicalMedium(edge *Edge, capacity int) Medium {
	return NewQueueMedium(edge, capacity)
}

// NoopMedium is a [Medium] that discards all sends and never produces
// values. It is used to satisfy listener slots for edges that are
// unreachable in the current query.
type NoopMedium struct {
	key   *Edge
	label string
}

// String returns a label of the form "source->destination".
func (m *NoopMedium) String() string {
	return m.label
}

// NewNoopMedium returns a NoopMedium for the given edge.
func NewNoopMedium(edge *Edge) *NoopMedium {
	src, dst := EdgeLabels(edge)
	return &NoopMedium{
		key:   edge,
		label: src + "->" + dst,
	}
}

// Key returns the edge associated with this medium.
func (m *NoopMedium) Key() *Edge {
	return m.key
}

// Recv always returns (nil, false).
func (m *NoopMedium) Recv(_ context.Context) (*Message, bool) {
	return nil, false
}

// Send returns false without consuming the message when the context
// has been canceled. Otherwise, the message is marked as done and
// discarded, and Send returns true.
func (m *NoopMedium) Send(ctx context.Context, msg *Message) bool {
	if ctx.Err() != nil {
		return false
	}
	msg.Done()
	return true
}

// Close is a no-op.
func (m *NoopMedium) Close() {}

// QueueMedium is a [Medium] backed by an [mpmc.Queue]. Like all
// [Medium] variants, it is multiple-producer, single-consumer.
type QueueMedium struct {
	key    *Edge
	queue  *mpmc.Queue[*Message]
	label  string
	closed bool
}

// String returns a label of the form "source->destination".
func (m *QueueMedium) String() string {
	return m.label
}

// NewQueueMedium returns a QueueMedium for the given edge. The capacity
// is rounded down to the nearest power of two as required by the
// underlying ring buffer.
func NewQueueMedium(edge *Edge, capacity int) *QueueMedium {
	if capacity < 0 {
		capacity = 2
	}
	medium := mpmc.MustQueue[*Message](FloorPowerOfTwo(uint(capacity)), -1)
	src, dst := EdgeLabels(edge)
	return &QueueMedium{
		key:   edge,
		queue: medium,
		label: src + "->" + dst,
	}
}

// Key returns the edge associated with this medium.
func (m *QueueMedium) Key() *Edge {
	return m.key
}

// Recv blocks until a message is available, the queue is closed, or the
// context is cancelled.
func (m *QueueMedium) Recv(ctx context.Context) (*Message, bool) {
	if m.closed {
		return nil, false
	}
	msg, more := m.queue.Recv(ctx)
	if !more && ctx.Err() == nil {
		m.closed = true
	}
	return msg, more
}

// Send enqueues a message. It returns false if the context is already cancelled.
func (m *QueueMedium) Send(ctx context.Context, value *Message) bool {
	if ctx.Err() != nil {
		return false
	}
	return m.queue.Send(ctx, value)
}

// Close closes the underlying queue, signaling no more messages will be sent.
func (m *QueueMedium) Close() {
	m.queue.Close()
}

// AccumulatorMedium is a [Medium] backed by an [mpsc.Accumulator].
// It is used for cyclical edges where multiple producers may send
// concurrently.
type AccumulatorMedium struct {
	key    *Edge
	acc    *mpsc.Accumulator[*Message]
	label  string
	closed bool
}

// String returns a label of the form "source->destination".
func (m *AccumulatorMedium) String() string {
	return m.label
}

// NewAccumulatorMedium returns an [AccumulatorMedium] for the given edge.
func NewAccumulatorMedium(edge *Edge) *AccumulatorMedium {
	medium := mpsc.NewAccumulator[*Message]()
	src, dst := EdgeLabels(edge)
	return &AccumulatorMedium{
		key:   edge,
		acc:   medium,
		label: src + "->" + dst,
	}
}

// Key returns the edge associated with this medium.
func (m *AccumulatorMedium) Key() *Edge {
	return m.key
}

// Recv blocks until a message is available or the context is cancelled.
// It returns false after the accumulator is closed and drained.
func (m *AccumulatorMedium) Recv(ctx context.Context) (*Message, bool) {
	if m.closed {
		return nil, false
	}
	msg, more := m.acc.Recv(ctx)
	if !more && ctx.Err() == nil {
		m.closed = true
	}
	return msg, more
}

// Close closes the underlying accumulator, signaling no more messages will be sent.
func (m *AccumulatorMedium) Close() {
	m.acc.Close()
}

// Send enqueues a message. It returns false if the context is already cancelled.
func (m *AccumulatorMedium) Send(ctx context.Context, value *Message) bool {
	if ctx.Err() != nil {
		return false
	}
	return m.acc.Send(value)
}

// ChannelMedium is a [Medium] backed by a buffered channel.
// It is used for non-cyclical edges. Close is safe to call multiple times.
type ChannelMedium struct {
	key    *Edge
	ch     chan *Message
	label  string
	closed atomic.Bool
}

// NewChannelMedium returns a [ChannelMedium] for the given edge with the
// specified buffer capacity.
func NewChannelMedium(edge *Edge, capacity int) *ChannelMedium {
	src, dst := EdgeLabels(edge)
	return &ChannelMedium{
		key:   edge,
		ch:    make(chan *Message, capacity),
		label: src + "->" + dst,
	}
}

// String returns a label of the form "source->destination".
func (m *ChannelMedium) String() string {
	return m.label
}

// Key returns the edge associated with this medium.
func (m *ChannelMedium) Key() *Edge {
	return m.key
}

// Recv blocks until a message is available, the channel is closed, or the
// context is cancelled.
func (m *ChannelMedium) Recv(ctx context.Context) (*Message, bool) {
	select {
	case value, more := <-m.ch:
		return value, more
	case <-ctx.Done():
		return nil, false
	}
}

// Close closes the underlying channel. It is safe to call multiple times.
func (m *ChannelMedium) Close() {
	if !m.closed.Swap(true) {
		close(m.ch)
	}
}

// Send enqueues a message, blocking until space is available or the context
// is cancelled.
func (m *ChannelMedium) Send(ctx context.Context, value *Message) bool {
	select {
	case m.ch <- value:
		return true
	case <-ctx.Done():
		return false
	}
}
