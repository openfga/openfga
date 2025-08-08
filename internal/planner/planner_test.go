package planner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPlanner_New(t *testing.T) {
	p := New()
	require.NotNil(t, p)
}

func TestPlanner_SelectResolver(t *testing.T) {
	p := New()
	key := "test_key"
	resolvers := []string{"fast", "slow"}

	choice := p.SelectResolver(key, resolvers)
	require.Contains(t, resolvers, choice)
	kp := p.getKeyPlan(key)

	require.NotNil(t, kp)
	require.NotNil(t, kp.stats["fast"])
	require.NotNil(t, kp.stats["slow"])
}

func TestProfiler_Update(t *testing.T) {
	p := New()
	key := "test_convergence"
	resolvers := []string{"fast", "slow"}

	// initialize internal maps
	_ = p.SelectResolver(key, resolvers)

	// Heavily reward the "fast" strategy
	for i := 0; i < 150; i++ {
		p.UpdateStats(key, "fast", 10*time.Millisecond)
	}
	// Heavily penalize the "slow" strategy
	for i := 0; i < 150; i++ {
		p.UpdateStats(key, "slow", 500*time.Millisecond)
	}

	// After sufficient updates, Thompson sampling should almost always choose the better option.
	// We test this by seeing if it's chosen a high percentage of the time.
	counts := make(map[string]int)
	for i := 0; i < 100; i++ {
		choice := p.SelectResolver(key, resolvers)
		counts[choice]++
	}

	require.Greater(t, counts["fast"], 90)
	require.Less(t, counts["slow"], 10)
}
