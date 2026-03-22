package worker

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/openfga/openfga/internal/containers/mpsc"
)

// Sender is the read end of a [Medium]. A worker reads from its senders
// to receive messages produced by upstream workers.
type Sender interface {
	Key() *Edge
	Recv(context.Context) (*Message, bool)
	fmt.Stringer
}

// Listener is the write end of a [Medium]. A worker writes to its listeners
// to deliver messages to downstream workers.
type Listener interface {
	Close()
	Key() *Edge
	Send(context.Context, *Message) bool
	fmt.Stringer
}

// Medium is a unidirectional pipe between two workers, combining the
// [Sender] (read) and [Listener] (write) ends.
type Medium interface {
	Sender
	Listener
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
