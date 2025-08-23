package planner

import (
	"math/rand"
	"sync"
	"time"
)

type Planner struct {
	// ttu|storeID|objectType|relation|userType|tuplesetRelation|tuplesetComputedRelation
	// userset|storeID|objectType|relation|userType
	keys         sync.Map
	initialGuess time.Duration
	// Global RNG pool to avoid per-KeyPlan allocation
	rngPool sync.Pool
}

func New(initialGuess time.Duration) *Planner {
	p := &Planner{
		initialGuess: initialGuess,
	}
	p.rngPool.New = func() interface{} {
		return rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return p
}

func (p *Planner) GetKeyPlan(key string) *KeyPlan {
	// LoadOrStore is an atomic operation that returns the existing value for a key
	// or stores the new one if it doesn't exist.
	kp, _ := p.keys.LoadOrStore(key, &KeyPlan{
		initialGuess: p.initialGuess,
		stats:        make(map[string]*ThompsonStats),
		planner:      p,
	})
	return kp.(*KeyPlan)
}

type KeyPlan struct {
	initialGuess time.Duration
	// Use RWMutex for better read performance during SelectResolver
	mu      sync.RWMutex
	stats   map[string]*ThompsonStats
	planner *Planner
}

// SelectResolver implements the Thompson Sampling decision rule.
// It is now public and is the main entry point for getting a decision.
func (kp *KeyPlan) SelectResolver(resolvers []string) string {
	// Fast path: try read-only access first
	kp.mu.RLock()
	allStatsExist := true
	for _, name := range resolvers {
		if _, ok := kp.stats[name]; !ok {
			allStatsExist = false
			break
		}
	}

	if allStatsExist {
		// All stats exist, we can proceed with sampling without upgrading to write lock
		resolver := kp.selectResolverLocked(resolvers)
		kp.mu.RUnlock()
		return resolver
	}
	kp.mu.RUnlock()

	// Slow path: need to create missing stats
	kp.mu.Lock()
	defer kp.mu.Unlock()

	// Double-check after acquiring write lock
	for _, name := range resolvers {
		if _, ok := kp.stats[name]; !ok {
			kp.stats[name] = NewThompsonStats(kp.initialGuess)
		}
	}

	return kp.selectResolverLocked(resolvers)
}

// selectResolverLocked assumes mu is already held (either read or write lock).
func (kp *KeyPlan) selectResolverLocked(resolvers []string) string {
	// Get RNG from pool
	rng := kp.planner.rngPool.Get().(*rand.Rand)
	defer kp.planner.rngPool.Put(rng)

	resolver := ""
	var minSampledTime float64 = -1

	// Sample from each resolver's distribution and pick the one with the best (lowest) sample.
	for _, name := range resolvers {
		ts := kp.stats[name]
		// We use the pooled rng
		sampledTime := ts.Sample(rng)
		if resolver == "" || sampledTime < minSampledTime {
			minSampledTime = sampledTime
			resolver = name
		}
	}

	return resolver
}

// UpdateStats performs the Bayesian update for the given resolver.
func (kp *KeyPlan) UpdateStats(resolver string, duration time.Duration) {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	if _, ok := kp.stats[resolver]; !ok {
		kp.stats[resolver] = NewThompsonStats(kp.initialGuess)
	}

	kp.stats[resolver].Update(duration)
}

func (kp *KeyPlan) GetStats() map[string]*ThompsonStats {
	kp.mu.RLock()
	defer kp.mu.RUnlock()

	// Return a copy to avoid races
	result := make(map[string]*ThompsonStats, len(kp.stats))
	for k, v := range kp.stats {
		result[k] = v
	}
	return result
}
