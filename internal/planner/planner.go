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
}

func New(initialGuess time.Duration) *Planner {
	return &Planner{
		initialGuess: initialGuess,
	}
}

func (p *Planner) GetKeyPlan(key string) *KeyPlan {
	// LoadOrStore is an atomic operation that returns the existing value for a key
	// or stores the new one if it doesn't exist.
	kp, _ := p.keys.LoadOrStore(key, &KeyPlan{
		stats: make(map[string]*ThompsonStats),
		rng:   rand.New(rand.NewSource(time.Now().UnixNano())),
	})
	return kp.(*KeyPlan)
}

type KeyPlan struct {
	initialGuess time.Duration
	mu           sync.Mutex
	stats        map[string]*ThompsonStats
	rng          *rand.Rand
}

// SelectResolver implements the Thompson Sampling decision rule.
// It is now public and is the main entry point for getting a decision.
func (kp *KeyPlan) SelectResolver(resolvers []string) string {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	resolver := ""
	var minSampledTime float64 = -1

	// Sample from each resolver's distribution and pick the one with the best (lowest) sample.
	for _, name := range resolvers {
		// Ensure stats exist for this key for all resolvers.
		if _, ok := kp.stats[name]; !ok {
			kp.stats[name] = NewThompsonStats(kp.initialGuess)
		}
		ts := kp.stats[name]
		// We use the global rng, but the lock ensures that the read of the stats
		// and the subsequent update are consistent for a given key.
		sampledTime := ts.Sample(kp.rng)
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
	return kp.stats
}