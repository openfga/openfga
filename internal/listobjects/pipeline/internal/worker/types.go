package worker

import (
	"context"
	"fmt"
	"iter"
	"strings"
	"sync"

	weightedGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/listobjects/pipeline/internal/track"
	"github.com/openfga/openfga/internal/seq"
)

// Edge is an alias for the weighted authorization model edge type.
type Edge = weightedGraph.WeightedAuthorizationModelEdge

// Color represents the activity state of a worker during cycle detection.
type Color int32

const (
	// White indicates no activity since the last probe.
	White Color = iota
	// Black indicates activity has occurred since the last probe.
	Black
)

// Probe carries cycle detection state through a ring of workers.
// The Label identifies the originating member, and the Color accumulates
// the activity state observed along the ring.
type Probe struct {
	Label string
	Color Color
}

// Message carries a batch of values between workers.
// Callback, when non-nil, is invoked by Done to release pooled resources.
type Message struct {
	Value    []string
	Callback func()
}

// Done invokes the message's Callback if one is set.
func (m *Message) Done() {
	if m.Callback != nil {
		m.Callback()
	}
	m.Callback = nil
	m.Value = nil
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

// Core provides shared infrastructure for all worker types, including
// listener management, message broadcasting, and buffer pooling.
type Core struct {
	senders     []Sender
	listeners   []Listener
	MediumFunc  func(*Edge, int) Medium
	MsgFunc     func(*Message, *Edge)
	Label       string
	Errors      chan<- error
	Interpreter Interpreter
	ChunkSize   int
	NumProcs    int
	Pool        *sync.Pool
}

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

// DefaultMediumFunc selects a Medium implementation based on whether
// the edge is cyclical: an [AccumulatorMedium] for cyclical edges,
// or a [ChannelMedium] otherwise.
var DefaultMediumFunc = func(edge *Edge, capacity int) Medium {
	var medium Medium

	if IsCyclical(edge) {
		medium = NewAccumulatorMedium(edge)
	} else {
		medium = NewChannelMedium(edge, capacity)
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
}

// Broadcast reads values from the iterator in batches of ChunkSize and
// broadcasts each batch to all registered listeners. It stops when the
// iterator is exhausted or ctx is cancelled.
func (c *Core) Broadcast(ctx context.Context, values iter.Seq[string]) {
	reader := seq.NewSeqReader(values)
	defer reader.Close()

	buffer := c.Pool.Get().(*[]string)
	buf := (*buffer)[:c.ChunkSize]

	for ctx.Err() == nil {
		written := reader.Read(buf)
		if written == 0 {
			break
		}
		for _, listener := range c.listeners {
			pooled := c.Pool.Get().(*[]string)
			output := (*pooled)[:written]
			copy(output, buf)

			m := &Message{
				Value: output,
				Callback: func() {
					clear((*pooled)[:cap(*pooled)])
					*pooled = (*pooled)[:0]
					c.Pool.Put(pooled)
				},
			}

			if c.MsgFunc != nil {
				c.MsgFunc(m, listener.Key())
			}

			if !listener.Send(ctx, m) {
				m.Done()
			}
		}
	}
	clear((*buffer)[:cap(*buffer)])
	*buffer = (*buffer)[:0]
	c.Pool.Put(buffer)
}

// Membership represents a worker's participation in a [CycleGroup].
// It provides probe-based cycle detection and readiness coordination.
type Membership struct {
	reporter *track.Reporter
	next     *Membership
	prev     *Membership
	label    string
	leader   bool
	probe    chan *Probe
	wake     chan struct{}
}

// String returns a representation of the cycle path starting from this member.
func (m *Membership) String() string {
	var sb strings.Builder
	sb.WriteString(m.label)

	for next := m.prev; next != m; next = next.prev {
		sb.WriteString("->" + next.label)
	}
	sb.WriteString("->" + m.label)
	return sb.String()
}

// IsLeader reports whether this member is the cycle group's leader.
// The leader initiates cycle detection probes.
func (m *Membership) IsLeader() bool {
	return m.leader
}

// RecvProbe blocks until a probe arrives, the context is cancelled,
// or the member is woken. It returns false when no probe was received.
func (m *Membership) RecvProbe(ctx context.Context) (*Probe, bool) {
	select {
	case probe, ok := <-m.probe:
		return probe, ok
	case <-ctx.Done():
		return nil, false
	case <-m.wake:
		return nil, false
	}
}

// SendProbe sends a probe to the next member in the cycle.
// It returns false if the context is cancelled before the send completes.
func (m *Membership) SendProbe(ctx context.Context, probe *Probe) bool {
	next := m.prev
	select {
	case next.probe <- probe:
		return true
	case <-ctx.Done():
		return false
	}
}

// EndProbe closes the probe channels of all members in the group,
// unblocking any pending [Membership.RecvProbe] calls.
func (m *Membership) EndProbe() {
	close(m.probe)
	for next := m.prev; next != m; next = next.prev {
		close(next.probe)
	}
}

// SignalReady indicates that this member's non-cyclical inputs are exhausted.
func (m *Membership) SignalReady() {
	m.reporter.Report()
	m.reporter.Dec()
}

// WaitForAllReady blocks until all members in the group have signaled ready
// and the in-flight message count has reached zero. It returns false if ctx
// is cancelled first.
func (m *Membership) WaitForAllReady(ctx context.Context) bool {
	return m.reporter.Wait(ctx)
}

// Inc increments the group's in-flight message count.
func (m *Membership) Inc() {
	m.reporter.Inc()
}

// Dec decrements the group's in-flight message count.
func (m *Membership) Dec() {
	m.reporter.Dec()
}

// CycleGroup coordinates a set of workers that form a cycle in the
// authorization model graph. Members are linked in a ring and use
// probe-based detection to determine when the cycle has reached quiescence.
// The members are always a subset of the authorization model graph that form
// a cycle, and are ordered as a reverse topological sort of the graph.
// It is possible for multiple independent CycleGroup instances to exist
// within a pipeline.
type CycleGroup struct {
	statusPool *track.StatusPool
	size       int
	head       *Membership
	tail       *Membership
}

// NewCycleGroup returns an empty CycleGroup.
func NewCycleGroup() *CycleGroup {
	var g CycleGroup
	g.statusPool = track.NewStatusPool()
	return &g
}

// Size returns the number of members in the group.
func (g *CycleGroup) Size() int {
	return g.size
}

// Join adds a new member to the group and returns its [Membership] handle.
// The most recently joined member becomes the leader.
func (g *CycleGroup) Join(label string) *Membership {
	reporter := g.statusPool.Register()

	m := Membership{
		label:    label,
		reporter: reporter,
		probe:    make(chan *Probe, 1),
		wake:     make(chan struct{}, 1),
	}

	if g.head == nil {
		g.head = &m
	}

	if g.tail == nil {
		g.tail = &m
	}

	g.tail.prev = &m
	m.prev = g.head

	g.head.leader = false
	g.head.next = &m
	g.head = &m
	g.head.leader = true

	g.size++
	// Each member starts with an in-flight count of 1. This is decremented
	// by SignalReady once the member's non-cyclical inputs are exhausted,
	// preventing the pool from reaching quiescence prematurely.
	m.reporter.Inc()
	return &m
}

// Interpreter transforms raw input items by querying storage through
// an edge's relation definition.
type Interpreter interface {
	Interpret(ctx context.Context, edge *Edge, items []string) iter.Seq[Item]
}

// Worker processes messages from upstream senders, transforms them
// through an [Interpreter], and broadcasts results to downstream listeners.
type Worker interface {
	Listen(s Sender)
	Execute(context.Context)
	Subscribe(*Edge, int) Sender
	Cleanup()
	fmt.Stringer
}
