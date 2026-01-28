package planner

import (
	"math/rand"
	"testing"
	"time"
)

func BenchmarkPlanner_RecursiveScenario(b *testing.B) {
	// 1. Setup Planner
	// We use a Noop config or standard config.
	// If you want to avoid background eviction noise, ensure eviction is disabled or interval is high.
	p := New(&Config{
		EvictionThreshold: 1 * time.Hour,
		CleanupInterval:   1 * time.Hour,
	})

	key := "benchmark_key_recursive"
	kp := p.GetPlanSelector(key)

	// 2. Define the Strategies with your exact Priors
	const (
		defaultResolverName   = "defaultResolver"
		recursiveResolverName = "recursiveResolver"
	)

	resolvers := map[string]*PlanConfig{
		defaultResolverName: {
			Name:         defaultResolverName,
			InitialGuess: 500 * time.Millisecond, // Slow Prior
			Lambda:       1,
			Alpha:        3,
			Beta:         2,
		},
		recursiveResolverName: {
			Name:         recursiveResolverName,
			InitialGuess: 150 * time.Millisecond, // Fast Prior
			Lambda:       1,
			Alpha:        3,
			Beta:         2,
		},
	}

	// 3. Setup Randomness for Simulation
	// We use a local RNG to simulate the "Network Latency" of the strategies.
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Track selection counts to verify behavior (optional, minimal overhead)
	var fastCount, slowCount int

	b.ResetTimer()
	b.ReportAllocs()

	// 4. The Benchmark Loop
	for i := 0; i < b.N; i++ {
		// A. Select Strategy
		selected := kp.Select(resolvers)

		// B. Simulate Execution Time based on the rules provided
		var actualDuration time.Duration

		if selected.Name == defaultResolverName {
			slowCount++
			// Default: Random between 500ms and 3000ms
			// Range = 2500, Base = 500
			randomMs := 500 + rng.Int63n(2501)
			actualDuration = time.Duration(randomMs) * time.Millisecond
		} else {
			fastCount++
			// Recursive: Random between 20ms and 150ms
			// Range = 130, Base = 20
			randomMs := 20 + rng.Int63n(131)
			actualDuration = time.Duration(randomMs) * time.Millisecond
		}

		// C. Update Stats
		kp.UpdateStats(selected, actualDuration)
	}

	b.StopTimer()

	// Optional: Print the win rate to verify the algorithm is working
	total := fastCount + slowCount
	if total > 0 {
		b.Logf("Benchmark Results:")
		b.Logf("Total Iterations: %d", total)
		b.Logf("Default (Slow) Chosen: %d (%.2f%%)", slowCount, float64(slowCount)/float64(total)*100)
		b.Logf("Recursive (Fast) Chosen: %d (%.2f%%)", fastCount, float64(fastCount)/float64(total)*100)
		b.Logf("-------------------------")
	}
}
