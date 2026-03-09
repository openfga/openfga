package track

import (
	"sync"
)

// Tracker is a concurrency-safe counter with condition-based waiting.
type Tracker struct {
	value int64
	mu    sync.Mutex
	wait  sync.Cond
}

func NewTracker() *Tracker {
	t := &Tracker{}
	t.wait.L = &t.mu
	return t
}

func (t *Tracker) Inc() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.value++

	if t.value == 0 || t.value == 1 {
		t.wait.Broadcast()
	}
}

func (t *Tracker) Dec() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.value--

	if t.value <= 1 && t.value >= -1 {
		t.wait.Broadcast()
	}
}

func (t *Tracker) Load() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.value
}

// Wait blocks until fn returns true, re-evaluating on each count change.
func (t *Tracker) Wait(fn func(int64) bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for {
		if fn(t.value) {
			return
		}
		t.wait.Wait()
	}
}

// Reporter updates a single entry in a StatusPool. Not safe for concurrent use.
type Reporter struct {
	ndx    int
	parent *StatusPool
}

func (r *Reporter) Report(status bool) {
	r.parent.set(r.ndx, status)
}

// Wait blocks until fn returns true for the pool's aggregate status.
func (r *Reporter) Wait(fn func(status bool) bool) {
	r.parent.Wait(fn)
}

// StatusPool aggregates boolean statuses from multiple sources.
// Returns true if any source is true.
type StatusPool struct {
	mu   sync.Mutex
	wait *sync.Cond
	init sync.Once
	pool []bool
}

func (sp *StatusPool) initialize() {
	sp.wait = sync.NewCond(&sp.mu)
}

// Register adds a source and returns its Reporter. Thread safe.
func (sp *StatusPool) Register() *Reporter {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	sp.pool = append(sp.pool, false)
	ndx := len(sp.pool) - 1

	return &Reporter{
		ndx:    ndx,
		parent: sp,
	}
}

func (sp *StatusPool) set(ndx int, value bool) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	sp.init.Do(sp.initialize)

	sp.pool[ndx] = value
	sp.wait.Broadcast()
}

func (sp *StatusPool) get() bool {
	for _, s := range sp.pool {
		if s {
			return true
		}
	}
	return false
}

func (sp *StatusPool) Status() bool {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	return sp.get()
}

// Wait blocks until fn returns true, re-evaluating on each status change.
func (sp *StatusPool) Wait(fn func(status bool) bool) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	sp.init.Do(sp.initialize)

	for !fn(sp.get()) {
		sp.wait.Wait()
	}
}
