package planner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPlanner_New(t *testing.T) {
	p := New(1 * time.Second)
	require.NotNil(t, p)
}

func TestPlanner_SelectResolver(t *testing.T) {
	p := New(1 * time.Second)
	key := "test_key"
	resolvers := []string{"fast", "slow"}

	kp := p.GetKeyPlan(key)
	choice := kp.SelectResolver(resolvers)
	require.Contains(t, resolvers, choice)

	require.NotNil(t, kp)
	require.NotNil(t, kp.stats["fast"])
	require.NotNil(t, kp.stats["slow"])
}

func TestProfiler_Update(t *testing.T) {
	p := New(10 * time.Millisecond)
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
