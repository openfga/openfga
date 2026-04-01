package worker

import (
	"context"
	"strings"
	"sync/atomic"

	"github.com/openfga/openfga/internal/listobjects/pipeline/internal/track"
)

// Membership represents a worker's participation in a [CycleGroup].
// It coordinates readiness signaling, quiescence detection via in-flight
// message counting, and ordered teardown through a sleep/wake chain.
type Membership struct {
	reporter *track.Reporter
	next     *Membership
	prev     *Membership
	label    string
	leader   bool
	wake     chan struct{}
	awake    atomic.Bool
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

// Next returns the next member in the ring. This is the member that
// should be woken during ordered teardown.
func (m *Membership) Next() *Membership {
	return m.prev
}

// IsLeader reports whether this member is the cycle group's leader.
// The leader initiates the ordered teardown cascade after quiescence.
func (m *Membership) IsLeader() bool {
	return m.leader
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

// Sleep blocks until this member is woken by its predecessor in the
// teardown cascade, or until ctx is cancelled.
func (m *Membership) Sleep(ctx context.Context) {
	select {
	case <-m.wake:
	case <-ctx.Done():
	}
}

// Wake unblocks a pending Sleep call. It is safe to call multiple times;
// only the first call has any effect.
func (m *Membership) Wake() {
	if !m.awake.Swap(true) {
		close(m.wake)
	}
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
// authorization model graph. Members are linked in a ring and share a
// [track.StatusPool] that detects quiescence: all members have exhausted
// their non-cyclical inputs and the in-flight message count has reached
// zero. After quiescence, the leader initiates an ordered teardown that
// cascades through the ring via [Membership.Wake].
//
// The members are always a subset of the authorization model graph that
// form a cycle, and are ordered as a reverse topological sort of the
// graph. It is possible for multiple independent CycleGroup instances
// to exist within a pipeline.
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
		wake:     make(chan struct{}),
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
