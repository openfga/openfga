package pipeline

import "github.com/openfga/openfga/pkg/server/commands/reverseexpand/pipeline/track"

// cycleGroup coordinates shutdown among workers connected by cyclical edges
// in the authorization graph. Workers in the same group share a message
// tracker and synchronize their shutdown through the status pool.
//
// Workers not in a cycle still use a cycleGroup, but it contains only
// that single worker.
type cycleGroup struct {
	tracker    *track.Tracker
	statusPool *track.StatusPool
}

// newCycleGroup creates a new cycle group.
func newCycleGroup() *cycleGroup {
	return &cycleGroup{
		tracker:    new(track.Tracker),
		statusPool: new(track.StatusPool),
	}
}

// Join registers a worker with this group and returns a Membership
// that provides access to coordination primitives.
func (g *cycleGroup) Join() *membership {
	reporter := g.statusPool.Register()
	reporter.Report(true)
	return &membership{
		tracker:  g.tracker,
		reporter: reporter,
	}
}

// membership represents a worker's participation in a CycleGroup and
// provides the coordination primitives needed for shutdown.
type membership struct {
	tracker  *track.Tracker
	reporter *track.Reporter
}

// Tracker returns the group's message tracker. Used when creating
// messages to enable cleanup tracking.
func (m *membership) Tracker() *track.Tracker {
	return m.tracker
}

// SignalReady indicates this worker has exhausted its non-cyclical inputs
// and is ready to shut down once all group members are ready.
func (m *membership) SignalReady() {
	m.reporter.Report(false)
}

// WaitForAllReady blocks until all group members have signaled ready.
func (m *membership) WaitForAllReady() {
	m.reporter.Wait(func(busy bool) bool { return !busy })
}

// WaitForDrain blocks until all tracked messages have been processed.
func (m *membership) WaitForDrain() {
	m.tracker.Wait(func(count int64) bool { return count < 1 })
}

// isCyclical returns true if the edge is recursive or part of a tuple cycle.
// Cyclical edges require special handling:
//   - Input deduplication to prevent infinite loops
//   - Shared CycleGroup for coordinated shutdown
func isCyclical(edge *Edge) bool {
	if edge == nil {
		return false
	}
	return len(edge.GetRecursiveRelation()) > 0 || edge.IsPartOfTupleCycle()
}
