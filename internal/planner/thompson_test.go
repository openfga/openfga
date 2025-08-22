package planner

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestThompsonStats_Update(t *testing.T) {
	stats := NewThompsonStats(1 * time.Second)
	initialMu := stats.Mu

	require.Zero(t, stats.Runs)

	stats.Update(100 * time.Millisecond)
	require.Equal(t, int64(1), stats.Runs)
	require.NotEqual(t, initialMu, stats.Mu)

	stats.Update(200 * time.Millisecond)
	require.Equal(t, int64(2), stats.Runs)
}

func TestThompsonStats_Sample(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		stats := NewThompsonStats(1 * time.Second)
		r := rand.New(rand.NewSource(1)) // Use a fixed seed for reproducibility

		sample1 := stats.Sample(r)
		require.Positive(t, sample1)

		sample2 := stats.Sample(r)
		require.NotEqual(t, sample1, sample2)
	})
	t.Run("complete", func(t *testing.T) {
		stats := NewThompsonStats(50 * time.Millisecond)
		r := rand.New(rand.NewSource(42))
		numSamples := 100

		// 1. Get the average of samples from the initial, diffuse prior.
		var totalBefore float64
		for i := 0; i < numSamples; i++ {
			totalBefore += stats.Sample(r)
		}
		avgBefore := totalBefore / float64(numSamples)

		// 2. Update the model with a series of high-latency results.
		// This should significantly shift the distribution's mean upwards.
		for i := 0; i < 20; i++ {
			stats.Update(500 * time.Millisecond)
		}

		// 3. Get the average of samples from the new, updated distribution.
		var totalAfter float64
		for i := 0; i < numSamples; i++ {
			totalAfter += stats.Sample(r)
		}
		avgAfter := totalAfter / float64(numSamples)

		// 4. Verify that the average of the samples has increased.
		require.Greater(t, avgAfter, avgBefore)
	})
}
