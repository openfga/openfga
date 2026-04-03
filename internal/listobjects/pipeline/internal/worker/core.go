package worker

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	weightedGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/concurrency"
)

var tracer = otel.Tracer("openfga/internal/listobjects/pipeline/internal/worker")

// Edge is an alias for the weighted authorization model edge type.
type Edge = weightedGraph.WeightedAuthorizationModelEdge

// Interpreter transforms raw input items by querying storage through
// an edge's relation definition.
type Interpreter interface {
	Interpret(ctx context.Context, edge *Edge, items []string) Receiver[Item]
}

// Worker processes messages from upstream senders, transforms them
// through an [Interpreter], and broadcasts results to downstream listeners.
type Worker interface {
	Listen(s Sender)
	Execute(context.Context)
	Subscribe(*Edge, int) Sender
	Cleanup()
	Len() int
	fmt.Stringer
}

// MessageProcessor handles a single message received from a sender.
// The index parameter identifies which sender produced the message.
type MessageProcessor interface {
	ProcessMessage(context.Context, int, *Message)
}

// Message carries a batch of values between workers.
// Callback, when non-nil, is invoked by Done to release pooled resources.
type Message struct {
	pool     *MessagePool
	Value    []string
	Callback func()
}

// Done invokes the message's Callback if one is set.
func (m *Message) Done() {
	if m.Callback != nil {
		m.Callback()
	}
	m.Callback = nil

	pool := m.pool
	m.pool = nil
	if pool != nil {
		pool.Put(m)
	}
}

// Item represents a single result from an [Interpreter], carrying either
// a value or an error.
type Item struct {
	Value string
	Err   error
}

// Object returns the item's value and error.
func (i Item) Object() (string, error) {
	return i.Value, i.Err
}

// Stats records per-sender throughput and timing metrics collected
// during [Core.ProcessSender] execution.
type Stats struct {
	Source              string
	Destination         string
	Cyclical            bool
	SumMessagesReceived int64
	SumObjectsReceived  int64
	MaxObjectsReceived  int64
}

// MessagePool is a channel-based free list of reusable string slices.
// Unlike [sync.Pool], items in the channel are strong references that
// are not subject to garbage collection between GC cycles, which
// prevents allocation thrashing on hot paths.
type MessagePool struct {
	free chan *Message
	size int
}

// InitMessagePool configures an existing MessagePool with the given
// element size and channel capacity.
func InitMessagePool(pool *MessagePool, size, capacity int) {
	pool.free = make(chan *Message, capacity)
	pool.size = size
}

// NewMessagePool returns a BufferPool that retains up to capacity buffers,
// each pre-sized to hold size elements.
func NewMessagePool(size, capacity int) *MessagePool {
	return &MessagePool{
		free: make(chan *Message, capacity),
		size: size,
	}
}

// Get returns a buffer from the pool, or allocates a new one if the pool
// is empty.
func (p *MessagePool) Get() *Message {
	select {
	case msg := <-p.free:
		msg.pool = p
		return msg
	default:
		msg := &Message{
			pool:  p,
			Value: make([]string, 0, p.size),
		}
		return msg
	}
}

// Put clears buf and returns it to the pool. If the pool is already full
// the buffer is dropped for garbage collection.
func (p *MessagePool) Put(msg *Message) {
	clear(msg.Value[:cap(msg.Value)])
	msg.Value = msg.Value[:0]
	select {
	case p.free <- msg:
	default:
	}
}

// Core provides shared infrastructure for all worker types, including
// listener management, message broadcasting, and buffer pooling.
type Core struct {
	senders     []Sender
	stats       []Stats
	listeners   []Listener
	MediumFunc  func(*Edge, int) Medium
	MsgFunc     func(*Message, *Edge)
	Label       string
	Errors      chan<- error
	Interpreter Interpreter
	ChunkSize   int
	NumProcs    int
	Pool        *MessagePool
}

func (c *Core) instrument(span trace.Span) {
	attrs := make([]attribute.KeyValue, 0, len(c.senders)*6)
	for i := range len(c.senders) {
		index := strconv.Itoa(i)
		prefix := "worker.sender." + index + "."
		attrs = append(
			attrs,
			attribute.String(prefix+"source", c.stats[i].Source),
			attribute.String(prefix+"destination", c.stats[i].Destination),
			attribute.Bool(prefix+"cyclical", c.stats[i].Cyclical),
			attribute.Int64(prefix+"received.message.sum", c.stats[i].SumMessagesReceived),
			attribute.Int64(prefix+"received.object.sum", c.stats[i].SumObjectsReceived),
			attribute.Int64(prefix+"received.object.max", c.stats[i].MaxObjectsReceived),
		)
	}
	span.SetAttributes(attrs...)
}

// error sends a non-nil *err to the shared error channel. If the channel
// is full, the error is silently dropped to avoid blocking the caller.
func (c *Core) error(err *error) {
	if err != nil && *err != nil {
		select {
		case c.Errors <- *err:
		default:
		}
	}
}

// String returns the worker's label.
func (c *Core) String() string {
	return c.Label
}

// Cleanup closes all downstream listeners.
func (c *Core) Cleanup() {
	for _, listener := range c.listeners {
		listener.Close()
	}
}

// Len returns the number of downstream listeners.
func (c *Core) Len() int {
	return len(c.listeners)
}

// Key is a typed context key used to store per-goroutine scratch buffers.
type Key int

const (
	// BufferKey is the context key for the reusable []string scratch buffer
	// allocated per processing goroutine in [Core.ProcessSender].
	BufferKey Key = iota
)

// ProcessSender receives messages from the sender at the given index and
// dispatches each to processor. It runs NumProcs goroutines in parallel to
// process messages and records throughput metrics in the corresponding
// [Stats] entry. When the sender is exhausted or ctx is cancelled, any
// remaining messages are drained.
func (c *Core) ProcessSender(ctx context.Context, index int, processor MessageProcessor) {
	sender := c.senders[index]
	stats := &c.stats[index]

	edge := sender.Key()
	src, dst := EdgeLabels(edge)

	cyclical := IsCyclical(edge)

	stats.Cyclical = cyclical
	stats.Source = src
	stats.Destination = dst

	// The drain must fully release all queued messages in order to prevent
	// deadlocks during cleanup.
	defer DrainSender(context.Background(), c.senders[index])

	var wg sync.WaitGroup
	defer wg.Wait()

	input := make(chan *Message)
	defer close(input)

	for range c.NumProcs {
		wg.Go(func() {
			var err error
			var msg *Message
			defer func() {
				if msg != nil {
					msg.Done()
				}
			}()
			defer c.error(&err)
			defer concurrency.RecoverFromPanic(&err)

			buffer := make([]string, 0, c.ChunkSize)
			ctx := context.WithValue(ctx, BufferKey, buffer)

			for msg = range input {
				if processor != nil {
					processor.ProcessMessage(ctx, index, msg)
				}
				msg.Done()
				msg = nil
			}
		})
	}

MessageLoop:
	for {
		msg, ok := sender.Recv(ctx)
		if !ok {
			break
		}
		stats.SumMessagesReceived++
		size := int64(len(msg.Value))
		stats.MaxObjectsReceived = max(stats.MaxObjectsReceived, size)
		stats.SumObjectsReceived += size

		select {
		case input <- msg:
		case <-ctx.Done():
			msg.Done()
			break MessageLoop
		}
	}
}

// DefaultMediumFunc selects a Medium implementation based on whether
// the edge is cyclical: an [AccumulatorMedium] for cyclical edges,
// or a [ChannelMedium] otherwise.
var DefaultMediumFunc = func(edge *Edge, capacity int) Medium {
	var medium Medium

	if IsCyclical(edge) {
		medium = NewCyclicalMedium(edge, capacity)
	} else {
		medium = NewStandardMedium(edge, capacity)
	}
	return medium
}

// Subscribe creates a [Medium] for the given edge and returns its [Sender] end.
// The [Listener] end is retained internally and receives messages broadcast
// by [Core.Broadcast]. If MediumFunc is set, it is used to create the Medium;
// otherwise [DefaultMediumFunc] is used.
func (c *Core) Subscribe(edge *Edge, capacity int) Sender {
	fn := DefaultMediumFunc
	if c.MediumFunc != nil {
		fn = c.MediumFunc
	}

	medium := fn(edge, capacity)

	c.listeners = append(c.listeners, medium)
	return medium
}

// Listen registers an upstream [Sender] as an input source for the worker.
func (c *Core) Listen(sender Sender) {
	c.senders = append(c.senders, sender)
	c.stats = append(c.stats, Stats{})
}

// send copies buffer into a pooled slice for each listener and enqueues
// the resulting message. The pooled slice is returned to the pool when the
// downstream consumer calls [Message.Done].
func (c *Core) send(ctx context.Context, buffer []string) {
	for _, listener := range c.listeners {
		msg := c.Pool.Get()
		msg.Value = msg.Value[:len(buffer)]
		copy(msg.Value, buffer)

		if c.MsgFunc != nil {
			c.MsgFunc(msg, listener.Key())
		}

		if !listener.Send(ctx, msg) {
			msg.Done()
		}
	}
}

// Broadcast reads values from the iterator in batches of ChunkSize and
// broadcasts each batch to all registered listeners. It stops when the
// iterator is exhausted or ctx is cancelled.
func (c *Core) Broadcast(ctx context.Context, values Receiver[string]) {
	buffer, ok := ctx.Value(BufferKey).([]string)
	if !ok || buffer == nil {
		buffer = make([]string, c.ChunkSize)
	}
	buffer = buffer[:0]

	for {
		value, ok := values.Recv(ctx)
		if !ok {
			break
		}
		buffer = append(buffer, value)

		if len(buffer) != c.ChunkSize {
			continue
		}
		c.send(ctx, buffer)
		clear(buffer)
		buffer = buffer[:0]
	}

	if len(buffer) > 0 {
		c.send(ctx, buffer)
	}
	clear(buffer)
}
