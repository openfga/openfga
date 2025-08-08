package planner

import (
	"math/rand"
	"sync"
	"time"
)

type Planner struct {
	// ttu|userset_storeID_objectType_relation_userType
	stats map[string]map[string]*ThompsonStats
	mu    sync.RWMutex
	rng   *rand.Rand // Random number generator for sampling
}

func New() *Planner {
	p := &Planner{
		stats: make(map[string]map[string]*ThompsonStats),
		rng:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	return p
}

// SelectResolver implements the Thompson Sampling decision rule.
// It is now public and is the main entry point for getting a decision.
func (p *Planner) SelectResolver(key string, resolvers []string) string {
	p.mu.Lock() // Lock for the duration of sampling to ensure consistency
	defer p.mu.Unlock()

	// Ensure stats exist for this key for all strategies.
	if _, ok := p.stats[key]; !ok {
		p.stats[key] = make(map[string]*ThompsonStats)
	}
	for _, name := range resolvers {
		if _, ok := p.stats[key][name]; !ok {
			p.stats[key][name] = NewThompsonStats()
		}
	}

	bestStrategy := ""
	var minSampledTime float64 = -1

	// Sample from each strategy's distribution and pick the one with the best (lowest) sample.
	for name, ts := range p.stats[key] {
		sampledTime := ts.Sample(p.rng)
		if bestStrategy == "" || sampledTime < minSampledTime {
			minSampledTime = sampledTime
			bestStrategy = name
		}
	}

	return bestStrategy
}

// UpdateStats performs the Bayesian update for the given strategy.
func (p *Planner) UpdateStats(key, resolver string, duration time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// The selectStrategy call ensures this entry exists.
	statsForStrategy := p.stats[key][resolver]
	statsForStrategy.Update(duration)
}
