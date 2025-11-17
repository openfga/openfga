package reverseexpand

import (
	"sync"
	"sync/atomic"
)

type tracker interface {
	Add(int64) int64
	Load() int64
}

type echoTracker struct {
	local  atomic.Int64
	parent tracker
}

func (t *echoTracker) Add(i int64) int64 {
	value := t.local.Add(i)
	if t.parent != nil {
		t.parent.Add(i)
	}
	return value
}

func (t *echoTracker) Load() int64 {
	return t.local.Load()
}

func newEchoTracker(parent tracker) tracker {
	return &echoTracker{
		parent: parent,
	}
}

type Reporter struct {
	ndx    int
	parent *StatusPool
}

func (r *Reporter) Report(status bool) {
	r.parent.mu.RLock()
	defer r.parent.mu.RUnlock()

	r.parent.pool[r.ndx] = status
}

// StatusPool is a struct that aggregates status values, as booleans, from multiple sources
// into a single boolean status value. Each source must register itself using the `Register`
// method and supply the returned value in each call to `Set` when updating the source's status
// value. The default state of a StatusPool is `false` for all sources. All StatusPool methods
// are thread safe.
type StatusPool struct {
	mu   sync.RWMutex
	pool []bool
	top  int
}

// Register is a function that creates a new entry in the StatusPool for a source and returns
// an identifier that is unique within the context of the StatusPool instance. The returned
// integer identifier values are predictable incrementing values beginning at 0. The `Register`
// method is thread safe.
func (sp *StatusPool) Register() Reporter {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	sp.pool = append(sp.pool, false)
	ndx := len(sp.pool) - 1

	return Reporter{
		ndx:    ndx,
		parent: sp,
	}
}

// Set is a function that accepts a registered identifier and a boolean status. The caller must
// provide an integer identifier returned from an initial call to the `Register` function associated
// with the desired source. The `Set` function is thread safe.
func (sp *StatusPool) Set(id int, status bool) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	sp.pool[id] = status
}

// Status is a function that returns the cummulative status of all sources registered within the pool.
// If any registered source's status is set to `true`, the return value of the `Status` function will
// be `true`. The default value is `false`. The `Status` function is thread safe.
func (sp *StatusPool) Status() bool {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	for _, s := range sp.pool {
		if s {
			return true
		}
	}
	return false
}
