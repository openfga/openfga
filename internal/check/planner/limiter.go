package planner

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// defaultConcurrencyLimit bounds the number of extra goroutines a single plan execution may
// spawn when no limit is configured. It mirrors pkg/server/config.DefaultResolveNodeBreadthLimit
// (the breadth limit applied to the graph LocalChecker); it is duplicated here rather than
// imported to keep internal/check/planner free of a dependency on pkg/server.
const defaultConcurrencyLimit = 10

// limiter bounds the number of additional goroutines a plan execution may spawn, while
// guaranteeing the execution can never deadlock — including the future case where executing
// a leaf itself fans out into more sub-plan executions that share this same limiter.
//
// It bounds extra goroutines, not progress: a task is scheduled on a new goroutine only if a
// slot is free; otherwise it runs inline on the caller's own goroutine. A goroutine
// that cannot acquire a slot therefore makes progress by doing the work itself, so a parent
// can never block forever waiting on a slot held by one of its own descendants. Every task
// always runs (concurrently when a slot is free, inline otherwise), so the call tree always
// completes regardless of nesting depth.
//
// A single limiter is shared across an entire Execute call tree; threading one limiter down
// (rather than creating a fresh one per fan-out) is what keeps the total goroutine count
// bounded by the limit instead of growing as limit^depth.
type limiter struct {
	eg     *errgroup.Group
	cancel context.CancelFunc
}

// newLimiter returns a limiter bounding concurrency to limit extra goroutines, together with
// the derived context that scheduled tasks must run under. A limit <= 0 means unbounded. The
// returned context is canceled when any task (concurrent or inline) returns an error, so
// sibling tasks stop early.
func newLimiter(ctx context.Context, limit int) (*limiter, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	eg, egCtx := errgroup.WithContext(ctx)
	if limit > 0 {
		eg.SetLimit(limit)
	}
	return &limiter{eg: eg, cancel: cancel}, egCtx
}

// run executes fn under the limit. If a goroutine slot is free, fn is scheduled to run
// concurrently and run returns nil immediately; its error (if any) surfaces later from wait.
// If no slot is free, fn runs inline on the caller's goroutine: this is the deadlock-free
// fallback. An inline error is returned directly and cancels the shared context so sibling
// tasks stop; the caller should stop scheduling and propagate it.
func (l *limiter) run(fn func() error) error {
	if l.eg.TryGo(fn) {
		return nil
	}
	if err := fn(); err != nil {
		l.cancel()
		return err
	}
	return nil
}

// wait blocks until all concurrently scheduled tasks complete and returns the first error
// reported by a concurrent task (nil if none). It must be called exactly once, after all
// scheduling is done, and releases the shared context. It does not observe inline errors —
// the caller holds those directly.
func (l *limiter) wait() error {
	defer l.cancel()
	return l.eg.Wait()
}
