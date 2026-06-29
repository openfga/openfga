package planner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage/adapter"
)

// fiveUsersetModel unions five distinct weight-2 userset hops, so a Check compiles to a
// unitMulti plan with five independent leaf queries — enough to observe a concurrency bound
// below the leaf count.
const fiveUsersetModel = `
	model
		schema 1.1
	type user
	type g1
		relations
			define member: [user]
	type g2
		relations
			define member: [user]
	type g3
		relations
			define member: [user]
	type g4
		relations
			define member: [user]
	type g5
		relations
			define member: [user]
	type document
		relations
			define viewer: [g1#member, g2#member, g3#member, g4#member, g5#member]`

// concurrencyTracker records the high-water mark of simultaneous executions.
type concurrencyTracker struct {
	active atomic.Int32
	max    atomic.Int32
	hold   time.Duration
}

func (c *concurrencyTracker) enter() {
	n := c.active.Add(1)
	for {
		m := c.max.Load()
		if n <= m || c.max.CompareAndSwap(m, n) {
			break
		}
	}
	time.Sleep(c.hold) // hold the slot so concurrent leaves overlap and the max is observable
	c.active.Add(-1)
}

// countingExecutor records concurrency on every query and answers each as denied, so a
// unitMulti plan runs all its leaves (an empty union) without granting.
type countingExecutor struct{ tracker *concurrencyTracker }

func (e countingExecutor) Query(_ context.Context, _ string, _ []any) (adapter.Rows, error) {
	e.tracker.enter()
	return &boolRows{}, nil
}

// TestExecuteMulti_ConcurrencyBounded verifies the configured limit bounds the goroutines the
// multi-query executor spawns. With a five-leaf plan and a limit of 2, at most two leaves run
// on spawned goroutines at once; the executor's own goroutine may run one more leaf inline
// when the limit is saturated, so concurrent executions never exceed limit+1.
func TestExecuteMulti_ConcurrencyBounded(t *testing.T) {
	const limit = 2
	tracker := &concurrencyTracker{hold: 25 * time.Millisecond}
	exec := countingExecutor{tracker: tracker}

	p := w2PlanFor(t, exec, fiveUsersetModel)
	require.Greater(t, len(p.unit.multi), limit+1, "plan must have more leaves than limit+1 to exercise the bound")

	got, err := p.Execute(context.Background(), nil, WithConcurrencyLimit(limit))
	require.NoError(t, err)
	require.False(t, got)

	require.LessOrEqual(t, tracker.max.Load(), int32(limit+1), "concurrency exceeded limit+1 (limit goroutines + 1 inline)")
	require.GreaterOrEqual(t, tracker.max.Load(), int32(2), "expected leaves to run concurrently")
}

// errExecutor fails every query with a sentinel error.
type errExecutor struct{ err error }

func (e errExecutor) Query(_ context.Context, _ string, _ []any) (adapter.Rows, error) {
	return nil, e.err
}

// TestExecuteMulti_ErrorPropagates verifies a leaf query error fails the whole execution,
// whether the leaf ran on a spawned goroutine or inline under a saturated limit.
func TestExecuteMulti_ErrorPropagates(t *testing.T) {
	sentinel := errors.New("datastore exploded")
	p := w2PlanFor(t, errExecutor{err: sentinel}, fiveUsersetModel)

	_, err := p.Execute(context.Background(), nil, WithConcurrencyLimit(1))
	require.ErrorIs(t, err, sentinel)
}

// TestLimiter_DeadlockFreeNestedFanout is the core deadlock-freedom guarantee: with the
// tightest possible bound (1), a task that itself fans out into more tasks on the same limiter
// — and depends on their completion before resolving — still runs to completion. A blocking
// bounded pool would deadlock here (the parent holds the only slot while its children wait
// forever for one); the inline fallback lets the children run on the parent's goroutine.
func TestLimiter_DeadlockFreeNestedFanout(t *testing.T) {
	withTimeout(t, 2*time.Second, func() {
		lim, _ := newLimiter(context.Background(), 1)

		var count atomic.Int32
		childResults := make([]bool, 3)
		err := lim.run(func() error {
			// A parent task that must execute three child plans before it can resolve.
			for i := range childResults {
				if err := lim.run(func() error {
					count.Add(1)
					childResults[i] = true
					return nil
				}); err != nil {
					return err
				}
			}
			// Under limit 1 the children all ran inline, so their results are ready here.
			for _, ok := range childResults {
				require.True(t, ok)
			}
			count.Add(1)
			return nil
		})
		require.NoError(t, err)
		require.NoError(t, lim.wait())
		require.Equal(t, int32(4), count.Load(), "parent + 3 children must all run")
	})
}

// TestLimiter_InlineErrorShortCircuits verifies that when the limit is saturated and a task
// runs inline, its error is returned directly and cancels the shared context so sibling tasks
// stop.
func TestLimiter_InlineErrorShortCircuits(t *testing.T) {
	withTimeout(t, 2*time.Second, func() {
		lim, ctx := newLimiter(context.Background(), 1)

		// Occupy the single slot with a goroutine blocked until released.
		release := make(chan struct{})
		require.NoError(t, lim.run(func() error {
			<-release
			return nil
		}))

		// The slot is taken, so this task runs inline and its error returns directly.
		sentinel := errors.New("boom")
		err := lim.run(func() error { return sentinel })
		require.ErrorIs(t, err, sentinel)
		require.Error(t, ctx.Err(), "inline error must cancel the shared context")

		close(release)
		_ = lim.wait()
	})
}

// withTimeout runs fn and fails the test if it does not finish within d, turning a deadlock
// into a test failure rather than a hung suite.
func withTimeout(t *testing.T, d time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("timed out — possible deadlock")
	}
}
