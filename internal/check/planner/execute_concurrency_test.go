package planner

import (
	"context"
	"errors"
	"strings"
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
	withTimeout(t, func() {
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
	withTimeout(t, func() {
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

// argsContain reports whether any bind arg equals s. Join queries carry their intermediate
// object type (e.g. "ga", "folder") in the bind args, so a test executor can route a specific
// leaf's query by the type it joins through.
func argsContain(args []any, s string) bool {
	for _, a := range args {
		if str, ok := a.(string); ok && str == s {
			return true
		}
	}
	return false
}

// shortCircuitExecutor drives short-circuit tests. For each boolean query it consults decide
// for the answer; if decide returns nil the query is "slow" and blocks until the context is
// cancelled, then returns ctx.Err(). A correct short-circuit cancels the slow queries, so the
// test finishes; a regression that awaits every leaf trips the test timeout.
type shortCircuitExecutor struct {
	decide func(sql string, args []any) *bool // non-nil => return a row iff *bool
}

func (e shortCircuitExecutor) Query(ctx context.Context, sql string, args []any) (adapter.Rows, error) {
	if d := e.decide(sql, args); d != nil {
		if *d {
			return &boolRows{remaining: 1}, nil
		}
		return &boolRows{}, nil
	}
	// Slow query: block until short-circuit cancels us.
	<-ctx.Done()
	return nil, ctx.Err()
}

// boolPtr returns a pointer to b, for shortCircuitExecutor.decide.
func boolPtr(b bool) *bool { return &b }

// TestExecuteMulti_IntersectionShortCircuitsOnFalse covers `direct and admin from parent`: the
// weight-1 direct leaf resolves false immediately while the weight-2 TTU join blocks until
// cancelled. The intersection must short-circuit to false and cancel the join.
func TestExecuteMulti_IntersectionShortCircuitsOnFalse(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type folder
			relations
				define admin: [user]
		type document
			relations
				define parent: [folder]
				define direct: [user]
				define viewer: direct and admin from parent`

	withTimeout(t, func() {
		exec := shortCircuitExecutor{decide: func(_ string, args []any) *bool {
			if argsContain(args, "folder") {
				return nil // the TTU join: block until cancelled
			}
			return boolPtr(false) // the direct leaf: deny fast
		}}
		p := w2PlanFor(t, exec, model)
		got, err := p.Execute(context.Background(), nil)
		require.NoError(t, err)
		require.False(t, got)
	})
}

// TestExecuteMulti_UnionShortCircuitsOnTrue covers `[user, group#member]`: the weight-1 direct
// leaf grants immediately while the weight-2 userset join blocks until cancelled. The union
// must short-circuit to true and cancel the join.
func TestExecuteMulti_UnionShortCircuitsOnTrue(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type group
			relations
				define member: [user]
		type document
			relations
				define viewer: [user, group#member]`

	withTimeout(t, func() {
		exec := shortCircuitExecutor{decide: func(sql string, _ []any) *bool {
			if strings.Contains(sql, "INNER JOIN") {
				return nil // the userset join: block until cancelled
			}
			return boolPtr(true) // the direct leaf: grant fast
		}}
		p := w2PlanFor(t, exec, model)
		got, err := p.Execute(context.Background(), nil)
		require.NoError(t, err)
		require.True(t, got)
	})
}

// TestExecuteMulti_ExclusionShortCircuits covers `admin from parent but not blocked`, where the
// subtract (blocked) resolves true fast — forcing the whole expression false — while the base
// join blocks until cancelled.
func TestExecuteMulti_ExclusionShortCircuits(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type folder
			relations
				define admin: [user]
		type document
			relations
				define parent: [folder]
				define blocked: [user]
				define viewer: admin from parent but not blocked`

	withTimeout(t, func() {
		exec := shortCircuitExecutor{decide: func(_ string, args []any) *bool {
			if argsContain(args, "folder") {
				return nil // the base TTU join: block until cancelled
			}
			return boolPtr(true) // the `blocked` subtract leaf: true fast => whole expr false
		}}
		p := w2PlanFor(t, exec, model)
		got, err := p.Execute(context.Background(), nil)
		require.NoError(t, err)
		require.False(t, got)
	})
}

// TestExecuteMulti_NestedShortCircuit covers the nested heterogeneous case
// `(g1#member or g2#member) and (g3#member and g4#member)`. The inner intersection's g3 leaf
// resolves false fast, which settles the whole tree to false; every other join blocks until
// cancelled, so the test only finishes if the root short-circuit cancels them all.
func TestExecuteMulti_NestedShortCircuit(t *testing.T) {
	model := `
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
		type document
			relations
				define u1: [g1#member]
				define u2: [g2#member]
				define i1: [g3#member]
				define i2: [g4#member]
				define viewer: (u1 or u2) and (i1 and i2)`

	withTimeout(t, func() {
		exec := shortCircuitExecutor{decide: func(_ string, args []any) *bool {
			if argsContain(args, "g3") {
				return boolPtr(false) // inner-intersection operand false => whole tree false
			}
			return nil // every other join blocks until cancelled
		}}
		p := planFor(t, exec, model, "document", "user:alice")
		require.Equal(t, unitMulti, p.unit.kind)
		got, err := p.Execute(context.Background(), nil)
		require.NoError(t, err)
		require.False(t, got)
	})
}

// TestExecuteMulti_SettledSubOperationCancelled is the user's per-operation-termination case,
// constructed so it can ONLY pass if a settled sub-operation's query is cancelled while the
// root is still undecided — whole-tree cancellation alone would deadlock it.
//
// The plan is `(u1 or u2) and (i1 and i2)`: u1 grants fast, settling the union to true while
// the root intersection stays undecided (it still needs the inner intersection). The leaves are
// wired into a dependency cycle that only a settled-union cancellation can break:
//   - u2 blocks until its context is cancelled, then signals u2Cancelled.
//   - i1 blocks until u2Cancelled fires, then returns false (settling the root to false).
//   - i2 grants fast (so the inner intersection waits only on i1).
//
// With per-unit cancellation, u1's true cancels u2 (the union no longer needs it) → u2 signals
// → i1 returns false → root is false → Execute returns. Without it, u2 is never cancelled until
// Execute returns, but Execute cannot return until i1 resolves, which waits on u2 — a deadlock
// the timeout would catch.
func TestExecuteMulti_SettledSubOperationCancelled(t *testing.T) {
	model := `
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
		type document
			relations
				define u1: [g1#member]
				define u2: [g2#member]
				define i1: [g3#member]
				define i2: [g4#member]
				define viewer: (u1 or u2) and (i1 and i2)`

	withTimeout(t, func() {
		u2Cancelled := make(chan struct{})
		queryFn := func(ctx context.Context, _ string, args []any) (adapter.Rows, error) {
			switch {
			case argsContain(args, "g1"): // u1: grant fast => union settles true
				return &boolRows{remaining: 1}, nil
			case argsContain(args, "g4"): // i2: grant fast => inner intersection waits only on i1
				return &boolRows{remaining: 1}, nil
			case argsContain(args, "g2"): // u2: must be cancelled once the union settles
				<-ctx.Done()
				close(u2Cancelled)
				return nil, ctx.Err()
			default: // g3 / i1: blocks until u2 is cancelled, then false => root decides false
				select {
				case <-u2Cancelled:
					return &boolRows{}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		}
		p := w2PlanFor(t, ctxExecutor{fn: queryFn}, model)
		got, err := p.Execute(context.Background(), nil)
		require.NoError(t, err)
		require.False(t, got)
	})
}

// TestExecuteMulti_ErrorSwallowedOnDecision verifies that once a definitive result is reached,
// a leaf error is swallowed: in an intersection where one leaf errors and another is false, the
// result is a clean false.
func TestExecuteMulti_ErrorSwallowedOnDecision(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type folder
			relations
				define admin: [user]
		type document
			relations
				define parent: [folder]
				define direct: [user]
				define viewer: direct and admin from parent`

	sentinel := errors.New("join blew up")
	exec := routedExecutor{fn: func(_ string, args []any) (adapter.Rows, error) {
		if argsContain(args, "folder") {
			return nil, sentinel // the join errors
		}
		return &boolRows{}, nil // the direct leaf: false => intersection decided false
	}}
	p := w2PlanFor(t, exec, model)
	got, err := p.Execute(context.Background(), nil)
	require.NoError(t, err, "a definitive false must swallow the sibling error")
	require.False(t, got)
}

// TestExecuteMulti_ErrorSurfacedWhenUndecided verifies that when no decision is reached, the
// first leaf error surfaces: in a union where one leaf errors and the rest are false, Execute
// returns the error.
func TestExecuteMulti_ErrorSurfacedWhenUndecided(t *testing.T) {
	sentinel := errors.New("one leaf failed")
	exec := routedExecutor{fn: func(_ string, args []any) (adapter.Rows, error) {
		if argsContain(args, "g1") {
			return nil, sentinel // one union operand errors
		}
		return &boolRows{}, nil // the rest are false => union stays undecided
	}}
	p := w2PlanFor(t, exec, fiveUsersetModel)
	_, err := p.Execute(context.Background(), nil)
	require.ErrorIs(t, err, sentinel)
}

// routedExecutor answers each query via fn, for tests that need per-query control including
// errors.
type routedExecutor struct {
	fn func(sql string, args []any) (adapter.Rows, error)
}

func (e routedExecutor) Query(_ context.Context, sql string, args []any) (adapter.Rows, error) {
	return e.fn(sql, args)
}

// ctxExecutor answers each query via fn with access to the query's context, for tests that
// assert a specific leaf's query is cancelled.
type ctxExecutor struct {
	fn func(ctx context.Context, sql string, args []any) (adapter.Rows, error)
}

func (e ctxExecutor) Query(ctx context.Context, sql string, args []any) (adapter.Rows, error) {
	return e.fn(ctx, sql, args)
}

// withTimeout runs fn and fails the test if it does not finish within a fixed deadline,
// turning a deadlock (e.g. a failed short-circuit leaving a query blocked) into a test failure
// rather than a hung suite.
func withTimeout(t *testing.T, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out — possible deadlock")
	}
}
