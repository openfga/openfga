package planner

import (
	"math/rand"
	"sync"
	"time"
)

// Planner is the top-level entry point for creating and managing plans for different keys.
// It is safe for concurrent use.
type Planner struct {
	keys         sync.Map // Stores *KeyPlan, ensuring fine-grained concurrency per key.
	initialGuess time.Duration
	// Use a pool of RNGs to reduce allocation overhead and initialization cost on the hot path.
	rngPool sync.Pool
}

// New creates a new Planner with a specified initial guess for strategy performance.
func New(initialGuess time.Duration) *Planner {
	p := &Planner{
		initialGuess: initialGuess,
	}
	p.rngPool.New = func() interface{} {
		// Each new RNG is seeded to ensure different sequences.
		return rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return p
}

// GetKeyPlan retrieves the plan for a specific key, creating it if it doesn't exist.
func (p *Planner) GetKeyPlan(key string) *KeyPlan {
	// LoadOrStore atomically handles the check-and-create logic.
	kp, _ := p.keys.LoadOrStore(key, &KeyPlan{
		initialGuess: p.initialGuess,
		planner:      p,
		// The stats map is now a sync.Map, so no pre-allocation is needed.
	})
	return kp.(*KeyPlan)
}

// KeyPlan manages the statistics for a single key and makes decisions about its resolvers.
// This struct is now entirely lock-free, using a sync.Map to manage its stats.
type KeyPlan struct {
	initialGuess time.Duration
	// By replacing the RWMutex and standard map with a sync.Map, we eliminate
	// a major contention point. Operations on different resolvers within the same
	// key (e.g., updating "fast-path" and "standard-path") can now run in parallel.
	stats   sync.Map // Stores map[string]*ThompsonStats
	planner *Planner
}

// SelectResolver implements the Thompson Sampling decision rule.
// It is the main entry point for getting a decision and is safe for concurrent use.
func (kp *KeyPlan) SelectResolver(resolvers []string) string {
	// No more RWMutex! The logic is simpler and faster.
	rng := kp.planner.rngPool.Get().(*rand.Rand)
	defer kp.planner.rngPool.Put(rng)

	bestResolver := ""
	var minSampledTime float64 = -1

	// Sample from each resolver's distribution.
	for _, name := range resolvers {
		// Atomically load the stats for the resolver. If it doesn't exist,
		// LoadOrStore creates and stores a new one. This replaces the entire
		// double-checked locking pattern from the previous version.
		ts, _ := kp.stats.LoadOrStore(name, NewThompsonStats(kp.initialGuess))

		sampledTime := ts.(*ThompsonStats).Sample(rng)
		if bestResolver == "" || sampledTime < minSampledTime {
			minSampledTime = sampledTime
			bestResolver = name
		}
	}

	return bestResolver
}

// UpdateStats performs the Bayesian update for the given resolver's statistics.
// This method is now lock-free and highly concurrent.
func (kp *KeyPlan) UpdateStats(resolver string, duration time.Duration) {
	// No more mutex lock!

	// Get or create the stats object for this resolver atomically.
	ts, _ := kp.stats.LoadOrStore(resolver, NewThompsonStats(kp.initialGuess))

	// Call the lock-free Update method on the ThompsonStats object.
	// Concurrent updates to different resolvers for the same key will not block each other.
	ts.(*ThompsonStats).Update(duration)
}

// GetStats returns a snapshot of the current statistics for all resolvers of this key.
func (kp *KeyPlan) GetStats() map[string]*ThompsonStats {
	// No more RLock. We iterate over the sync.Map to create a copy.
	result := make(map[string]*ThompsonStats)
	kp.stats.Range(func(key, value interface{}) bool {
		result[key.(string)] = value.(*ThompsonStats)
		return true // continue iteration
	})
	return result
}
