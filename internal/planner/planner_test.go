package planner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPlanner_New(t *testing.T) {
	p := New(&Config{})
	require.NotNil(t, p)
}

func TestPlanner_SelectResolver(t *testing.T) {
	p := New(&Config{})
	key := "test_key"
	resolvers := map[string]*KeyPlanStrategy{
		"fast": {
			Type:         "fast",
			InitialGuess: 5 * time.Millisecond,
			Lambda:       1,
			Alpha:        1,
			Beta:         1,
		},
		"slow": {
			Type:         "slow",
			InitialGuess: 10 * time.Millisecond,
			Lambda:       1,
			Alpha:        1,
			Beta:         1,
		}}
	kp := p.GetKeyPlan(key)
	choice := kp.SelectStrategy(resolvers)
	// It should favor "fast" since it has a better initial guess.
	require.Equal(t, "fast", choice)

	require.NotNil(t, kp)
	_, ok := kp.stats.Load("fast")
	require.True(t, ok)
	_, ok = kp.stats.Load("slow")
	require.True(t, ok)
}

func TestProfiler_Update(t *testing.T) {
	p := New(&Config{})
	key := "test_convergence"
	kp := p.GetKeyPlan(key)

	resolvers := map[string]*KeyPlanStrategy{
		"fast": {
			Type:         "fast",
			InitialGuess: 5 * time.Millisecond,
			Lambda:       1,
			Alpha:        1,
			Beta:         1,
		},
		"slow": {
			Type:         "slow",
			InitialGuess: 10 * time.Millisecond,
			Lambda:       1,
			Alpha:        1,
			Beta:         1,
		}}
	// Heavily reward the "fast" strategy
	for i := 0; i < 150; i++ {
		kp.UpdateStats(resolvers["fast"], 10*time.Millisecond)
	}
	// Heavily penalize the "slow" strategy
	for i := 0; i < 150; i++ {
		kp.UpdateStats(resolvers["slow"], 50*time.Millisecond)
	}

	// After sufficient updates, Thompson sampling should almost always choose the better option.
	// We test this by seeing if it's chosen a high percentage of the time.
	counts := make(map[string]int)
	for i := 0; i < 100; i++ {
		choice := kp.SelectStrategy(resolvers)
		counts[choice.Type]++
	}

	require.Greater(t, counts["fast"], 90)
	require.Less(t, counts["slow"], 10)
}
func TestPlanner_EvictStaleKeys(t *testing.T) {
	evictionThreshold := 50 * time.Millisecond
	p := New(&Config{
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
