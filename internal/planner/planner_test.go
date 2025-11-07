package planner

import (
	"fmt"
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
	resolvers := map[string]*PlanConfig{
		"fast": {
			Name:         "fast",
			InitialGuess: 5 * time.Millisecond,
			Lambda:       1,
			Alpha:        1,
			Beta:         1,
		},
		"slow": {
			Name:         "slow",
			InitialGuess: 10 * time.Millisecond,
			Lambda:       1,
			Alpha:        1,
			Beta:         1,
		}}

	// Test probabilistically over multiple runs instead of expecting deterministic behavior
	counts := make(map[string]int)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("test_key_%d", i)
		kp := p.GetPlanSelector(key)
		choice := kp.Select(resolvers)
		counts[choice.Name]++

		// Verify stats are created for both strategies
		kpi := kp.(*keyPlan)
		_, ok := kpi.stats.Load("fast")
		require.True(t, ok)
		_, ok = kpi.stats.Load("slow")
		require.True(t, ok)
	}

	// "fast" should be chosen more often due to better InitialGuess
	require.Greater(t, counts["fast"], 75, "fast should be chosen more than 75% of the time")
	require.Equal(t, 100, counts["fast"]+counts["slow"], "all selections should be accounted for")
}

func TestProfiler_Update(t *testing.T) {
	p := New(&Config{})
	key := "test_convergence"
	kp := p.GetPlanSelector(key)

	resolvers := map[string]*PlanConfig{
		"fast": {
			Name:         "fast",
			InitialGuess: 5 * time.Millisecond,
			Lambda:       1,
			Alpha:        1,
			Beta:         1,
		},
		"slow": {
			Name:         "slow",
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
		choice := kp.Select(resolvers)
		counts[choice.Name]++
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
		kp := p.GetPlanSelector(key)
		oldTime := time.Now().Add(-evictionThreshold - 10*time.Millisecond).UnixNano()
		kpi := kp.(*keyPlan)
		kpi.lastAccessed.Store(oldTime)
	}

	// Create one fresh key
	freshKp := p.GetPlanSelector("fresh_key")
	freshKpi := freshKp.(*keyPlan)
	freshKpi.touch()

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
