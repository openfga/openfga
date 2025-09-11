package planner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPlanner_New(t *testing.T) {
	p := New(&Config{InitialGuess: 1 * time.Second})
	require.NotNil(t, p)
}

func TestPlanner_SelectResolver(t *testing.T) {
	p := New(&Config{InitialGuess: 1 * time.Second})
	key := "test_key"
	resolvers := []string{"fast", "slow"}

	kp := p.GetKeyPlan(key)
	choice := kp.SelectResolver(resolvers)
	require.Contains(t, resolvers, choice)

	require.NotNil(t, kp)
	_, ok := kp.stats.Load("fast")
	require.True(t, ok)
	_, ok = kp.stats.Load("slow")
	require.True(t, ok)
}

func TestProfiler_Update(t *testing.T) {
	p := New(&Config{InitialGuess: 10 * time.Millisecond})
	key := "test_convergence"
	resolvers := []string{"fast", "slow"}

	kp := p.GetKeyPlan(key)

	// Heavily reward the "fast" strategy
	for i := 0; i < 150; i++ {
		kp.UpdateStats("fast", 10*time.Millisecond)
	}
	// Heavily penalize the "slow" strategy
	for i := 0; i < 150; i++ {
		kp.UpdateStats("slow", 50*time.Millisecond)
	}

	// After sufficient updates, Thompson sampling should almost always choose the better option.
	// We test this by seeing if it's chosen a high percentage of the time.
	counts := make(map[string]int)
	for i := 0; i < 100; i++ {
		choice := kp.SelectResolver(resolvers)
		counts[choice]++
	}

	require.Greater(t, counts["fast"], 90)
	require.Less(t, counts["slow"], 10)
}
func TestPlanner_EvictStaleKeys(t *testing.T) {
	evictionThreshold := 50 * time.Millisecond
	p := New(&Config{
		InitialGuess:      10 * time.Millisecond,
		EvictionThreshold: evictionThreshold,
	})

	// Create multiple old keys
	oldKeys := []string{"old_key1", "old_key2", "old_key3"}
	for _, key := range oldKeys {
		kp := p.GetKeyPlan(key)
		oldTime := time.Now().Add(-evictionThreshold - 10*time.Millisecond).UnixNano()
		kp.lastAccessed.Store(oldTime)
	}

	// Create one fresh key
	freshKp := p.GetKeyPlan("fresh_key")
	freshKp.touch()

	// Call evictStaleKeys
	p.evictStaleKeys()

	// Check that all old keys were evicted
	for _, key := range oldKeys {
		_, exists := p.keys.Load(key)
		require.False(t, exists, "old key %s should have been evicted", key)
	}

	// Check that fresh key still exists
	_, exists := p.keys.Load("fresh_key")
	require.True(t, exists, "fresh key should not have been evicted")
}

func BenchmarkKeyPlan(b *testing.B) {
	p := New(&Config{InitialGuess: 10 * time.Millisecond})
	kp := p.GetKeyPlan("test|store123|objectType|relation|userType")
	resolvers := []string{"resolver1", "resolver2", "resolver3"}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				_ = kp.SelectResolver(resolvers)
			} else {
				resolver := resolvers[i%len(resolvers)]
				duration := time.Duration(10+i%50) * time.Millisecond
				kp.UpdateStats(resolver, duration)
			}
			i++
		}
	})
}
