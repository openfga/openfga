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
	Type string
	/*
		In Thompson Sampling, the algorithm models its belief about each strategy's performance as a probability distribution (a "belief curve").
		The InitialGuess is the value that the algorithm assumes is the most likely average latency for a strategy it knows nothing about.
		To choose a strategy, the algorithm doesn't just use the average; it takes a random sample (a "draw") from each strategy's full belief curve.
		Before any data is collected, these draws will be statistically centered around the InitialGuess.
		A strategy with a low InitialGuess will have its draws centered around a low number, making it more likely to be chosen initially.
		Once a strategy is chosen and its actual performance is measured, that new data point is used to update the belief curve via a Bayesian update.
		If a strategy with an InitialGuess of 50ms consistently performs at 10ms, the curve's center will quickly "move" from 50ms down to 10ms. This is how the algorithm learns and adapts.
	*/
	InitialGuess time.Duration
	/*
		Lambda represents our confidence in the InitialGuess. How many good runs do we believe we've effectively seen already?
		A low Lambda (e.g., 1) means we have very little confidence. The model is "open-minded" and will quickly change its beliefs based on the first few real results. This encourages exploration.
		A medium lambda: 5, means a modest trust.
		A high Lambda (e.g., 10) means we are very confident. The model is "stubborn" and will require a lot of contradictory data to move away from its initial guess. This encourages exploitation.
	*/
	Lambda float64
	/*
		Alpha and Beta work together to define our belief about the consistency (or variance) of the strategy's performance.
		They describe the shape of the curve, not just its center, like the initial guess.
		Alpha is related to the number of observations about consistency.
		Beta is related to the amount of variation seen in those observations.

		Higher α (for the same β) → higher expected precision → lower variance. For example, α = 20, β = 2 create a tall, narrow distribution.
		This means we are very confident that the performance will be extremely consistent and tightly clustered around the average.
		Low Alpha and Beta values (e.g., α ≤ 1,α = 0.5) create a wide, flat distribution. This means we are very uncertain and expect performance to be highly variable, and this causes more exploration

		Higher β (for the same α) → lower precision → higher variance.
		If we expect the strategy to have bursty behaviour, keep α small (2–3) and/or β larger.
		If we expect the strategy to have tight behaviour, push α up and/or β down.
		If we don't have an idea, α≤1 keeps you very agnostic (heavy-tailed).
	*/

	Alpha float64
	Beta  float64
}

// Config holds configuration for the planner.
type Config struct {
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
