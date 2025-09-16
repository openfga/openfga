package planner

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Planner is the top-level entry point for creating and managing plans for different keys.
// It is safe for concurrent use and includes a background routine to evict old keys.
type Planner struct {
	keys              sync.Map // Stores *KeyPlan, ensuring fine-grained concurrency per key.
	evictionThreshold time.Duration
	// Use a pool of RNGs to reduce allocation overhead and initialization cost on the hot path.
	rngPool sync.Pool

	wg          sync.WaitGroup
	stopCleanup chan struct{}
}

type KeyPlanStrategy struct {
	Type         string
	InitialGuess time.Duration
	Lambda       float64
	Alpha        float64
	Beta         float64
}

// Config holds configuration for the planner.
type Config struct {
	InitialGuess      time.Duration // TODO: Remove
	EvictionThreshold time.Duration // How long a key can be unused before being evicted. (e.g., 30 * time.Minute)
	CleanupInterval   time.Duration // How often the planner checks for stale keys. (e.g., 5 * time.Minute)
}

// New creates a new Planner with the specified configuration and starts its cleanup routine.
func New(config *Config) *Planner {
	p := &Planner{
		evictionThreshold: config.EvictionThreshold,
		stopCleanup:       make(chan struct{}),
		wg:                sync.WaitGroup{},
	}
	p.rngPool.New = func() interface{} {
		// Each new RNG is seeded to ensure different sequences.
		return rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	if config.EvictionThreshold > 0 && config.CleanupInterval > 0 {
		p.startCleanupRoutine(config.CleanupInterval)
	}

	return p
}

func NewNoopPlanner() *Planner {
	p := &Planner{
		evictionThreshold: 0,
		stopCleanup:       make(chan struct{}),
		wg:                sync.WaitGroup{},
	}
	p.rngPool.New = func() interface{} {
		// Each new RNG is seeded to ensure different sequences.
		return rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return p
}

// GetKeyPlan retrieves the plan for a specific key, creating it if it doesn't exist.
func (p *Planner) GetKeyPlan(key string) *KeyPlan {
	kp, _ := p.keys.LoadOrStore(key, &KeyPlan{
		planner: p,
	})

	plan := kp.(*KeyPlan)
	plan.touch() // Mark as accessed.
	return plan
}

// KeyPlan manages the statistics for a single key and makes decisions about its resolvers.
// This struct is now entirely lock-free, using a sync.Map to manage its stats.
type KeyPlan struct {
	stats   sync.Map // Stores map[string]*ThompsonStats
	planner *Planner
	// lastAccessed stores the UnixNano timestamp of the last access.
	// Using atomic guarantees thread-safe updates without a mutex.
	lastAccessed atomic.Int64
}

// touch updates the lastAccessed timestamp to the current time.
func (kp *KeyPlan) touch() {
	kp.lastAccessed.Store(time.Now().UnixNano())
}

// getOrCreateStats atomically retrieves or creates the ThompsonStats for a given resolver name.
// This avoids the allocation overhead of calling LoadOrStore directly on the hot path.
func (kp *KeyPlan) getOrCreateStats(plan *KeyPlanStrategy) *ThompsonStats {
	// Fast path: Try a simple load first. This avoids the allocation in the common case.
	val, ok := kp.stats.Load(plan.Type)
	if ok {
		return val.(*ThompsonStats)
	}

	// Slow path: The stats don't exist. Create a new one.
	newTS := NewThompsonStats(plan.InitialGuess, plan.Lambda, plan.Alpha, plan.Beta)

	// Use LoadOrStore to handle the race where another goroutine might have created it
	// in the time between our Load and now. The newTs object is only stored if
	// no entry existed.
	actual, _ := kp.stats.LoadOrStore(plan.Type, newTS)
	return actual.(*ThompsonStats)
}

// SelectStrategy implements the Thompson Sampling decision rule.
func (kp *KeyPlan) SelectStrategy(resolvers map[string]*KeyPlanStrategy) *KeyPlanStrategy {
	kp.touch() // Mark this key as recently used.

	rng := kp.planner.rngPool.Get().(*rand.Rand)
	defer kp.planner.rngPool.Put(rng)

	bestResolver := ""
	var minSampledTime float64 = -1

	for k, plan := range resolvers {
		// Use the optimized helper method to get stats without unnecessary allocations.
		ts := kp.getOrCreateStats(plan)

		sampledTime := ts.Sample(rng)
		if bestResolver == "" || sampledTime < minSampledTime {
			minSampledTime = sampledTime
			bestResolver = k
		}
	}

	return resolvers[bestResolver]
}

// UpdateStats performs the Bayesian update for the given resolver's statistics.
func (kp *KeyPlan) UpdateStats(plan *KeyPlanStrategy, duration time.Duration) {
	kp.touch() // Mark this key as recently used.

	// Use the optimized helper method to avoid allocations.
	ts := kp.getOrCreateStats(plan)
	ts.Update(duration)
}

// UpdateStatsOverGuess is like UpdateStats but only updates if the duration is worse than the initial guess.
// This can help reduce noise from very fast responses that might not reflect true performance (or cancelled ones)..
func (kp *KeyPlan) UpdateStatsOverGuess(plan *KeyPlanStrategy, duration time.Duration) {
	if duration > plan.InitialGuess {
		kp.UpdateStats(plan, duration)
	}
}

// startCleanupRoutine runs a background goroutine that periodically evicts stale keys.
func (p *Planner) startCleanupRoutine(interval time.Duration) {
	ticker := time.NewTicker(interval)
	p.wg.Add(1)
	go func() {
		for {
			select {
			case <-ticker.C:
				p.evictStaleKeys()
			case <-p.stopCleanup:
				ticker.Stop()
				p.wg.Done()
				return
			}
		}
	}()
}

// evictStaleKeys iterates over all keys and removes any that haven't been accessed
// within the evictionThreshold.
func (p *Planner) evictStaleKeys() {
	evictionThresholdNano := p.evictionThreshold.Nanoseconds()
	nowNano := time.Now().UnixNano()

	// NOTE: Consider also bounding the total number of keys stored.

	p.keys.Range(func(key, value interface{}) bool {
		kp := value.(*KeyPlan)
		lastAccessed := kp.lastAccessed.Load()
		if (nowNano - lastAccessed) > evictionThresholdNano {
			p.keys.Delete(key)
		}
		return true // continue iteration
	})
}

// StopCleanup gracefully terminates the background cleanup goroutine.
func (p *Planner) StopCleanup() {
	close(p.stopCleanup)
	p.wg.Wait()
}
