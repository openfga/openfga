package track

import (
	"context"
	"sync"
	"sync/atomic"
)

// Reporter updates a single entry in a [StatusPool]. Not safe for concurrent use.
type Reporter struct {
	index  int
	parent *StatusPool
}

// Report marks this reporter's source as ready.
func (r *Reporter) Report() {
	r.parent.set(r.index)
}

// Inc increments the pool's in-flight count.
func (r *Reporter) Inc() {
	r.parent.inc()
}

// Dec decrements the pool's in-flight count.
func (r *Reporter) Dec() {
	r.parent.dec()
}

// Wait blocks until all sources in the pool have reported
// and the in-flight count has reached zero. It returns false
// if ctx is cancelled first.
func (r *Reporter) Wait(ctx context.Context) bool {
	return r.parent.Wait(ctx)
}

// StatusPool tracks readiness across multiple sources and waits for
// quiescence. Each registered source starts as pending; calling
// [Reporter.Report] marks it ready. [StatusPool.Wait] blocks until all
// sources are ready and the in-flight count has reached zero.
type StatusPool struct {
	mu         sync.Mutex
	pool       []bool
	inflight   atomic.Int64
	total      atomic.Int64
	zero       atomic.Bool
	ready      chan struct{}
	quiescence chan struct{}
}

// NewStatusPool returns a StatusPool with no registered sources.
func NewStatusPool() *StatusPool {
	var sp StatusPool
	sp.ready = make(chan struct{})
	sp.quiescence = make(chan struct{})
	return &sp
}

func (sp *StatusPool) inc() int64 {
	sp.total.Add(1)
	return sp.inflight.Add(1)
}

func (sp *StatusPool) dec() int64 {
	value := sp.inflight.Add(-1)
	if value == 0 {
		// Swap ensures the channel is closed exactly once even if
		// multiple goroutines race to decrement to zero.
		if !sp.zero.Swap(true) {
			close(sp.quiescence)
		}
	}
	return value
}

// Register adds a source and returns its [Reporter].
// All calls to Register must complete before any call to [StatusPool.Wait].
func (sp *StatusPool) Register() *Reporter {
	sp.pool = append(sp.pool, true)
	index := len(sp.pool) - 1

	return &Reporter{
		index:  index,
		parent: sp,
	}
}

func (sp *StatusPool) set(index int) {
	// Check-then-lock: the unguarded read is safe because pool[index]
	// transitions from true to false exactly once and is only written
	// under the lock. The early return avoids locking after the first call.
	if sp.pool[index] {
		sp.mu.Lock()
		defer sp.mu.Unlock()
		sp.pool[index] = false

		for _, value := range sp.pool {
			if value {
				return
			}
		}
		close(sp.ready)
	}
}

// Wait blocks until all registered sources have reported and the in-flight
// count has reached zero. It returns false if ctx is cancelled first.
func (sp *StatusPool) Wait(ctx context.Context) bool {
	if len(sp.pool) != 0 {
		select {
		case <-sp.ready:
		case <-ctx.Done():
			return false
		}
	}

	if sp.total.Load() > 0 {
		select {
		case <-sp.quiescence:
		case <-ctx.Done():
			return false
		}
	}
	return true
}
