package pipeline

import "github.com/openfga/openfga/pkg/server/commands/reverseexpand/pipeline/track"

// cycleGroup coordinates shutdown among workers in a cycle.
// Workers share a message tracker and synchronize via the status pool.
type cycleGroup struct {
	tracker    *track.Tracker
	statusPool track.StatusPool
}

func newCycleGroup() *cycleGroup {
	return &cycleGroup{
		tracker: track.NewTracker(),
	}
}

func (g *cycleGroup) Join() *membership {
	reporter := g.statusPool.Register()
	reporter.Report(true)
	return &membership{
		tracker:  g.tracker,
		reporter: reporter,
	}
}

// membership provides coordination primitives for cycle shutdown.
type membership struct {
	tracker  *track.Tracker
	reporter *track.Reporter
}

func (m *membership) Tracker() *track.Tracker {
	return m.tracker
}

// SignalReady indicates non-cyclical inputs are exhausted.
func (m *membership) SignalReady() {
	m.reporter.Report(false)
}

func (m *membership) WaitForAllReady() {
	m.reporter.Wait(func(busy bool) bool { return !busy })
}

func (m *membership) WaitForDrain() {
	m.tracker.Wait(func(count int64) bool { return count < 1 })
}

// isCyclical edges require deduplication and coordinated shutdown.
func isCyclical(edge *Edge) bool {
	if edge == nil {
		return false
	}
	return len(edge.GetRecursiveRelation()) > 0 || edge.IsPartOfTupleCycle()
}
