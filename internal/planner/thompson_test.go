package planner

import (
	"math/rand"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestThompsonStats_Update(t *testing.T) {
	stats := NewThompsonStats(1*time.Second, 1, 1, 1)
	params := (*samplingParams)(atomic.LoadPointer(&stats.params))
	initialMu := params.mu

	stats.Update(100 * time.Millisecond)
	params = (*samplingParams)(atomic.LoadPointer(&stats.params))
	require.NotEqual(t, initialMu, params.mu)
}

func TestThompsonStats_Sample(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		stats := NewThompsonStats(1*time.Second, 1, 1, 1)
		r := rand.New(rand.NewSource(1)) // Use a fixed seed for reproducibility

		sample1 := stats.Sample(r)
		require.Positive(t, sample1)

		sample2 := stats.Sample(r)
		require.NotEqual(t, sample1, sample2)
	})
	t.Run("complete", func(t *testing.T) {
		stats := NewThompsonStats(50*time.Millisecond, 1, 2, 1)
		r := rand.New(rand.NewSource(42))
		numSamples := 100

		// Helper to calculate Median
		getMedian := func() float64 {
			samples := make([]float64, numSamples)
			for i := 0; i < numSamples; i++ {
				samples[i] = stats.Sample(r)
			}
			sort.Float64s(samples) // Requires "sort" package
			return samples[numSamples/2]
		}

		// 1. Get Median Before (Should be near 50ms)
		medianBefore := getMedian()

		// 2. Update with high latency (500ms)
		for i := 0; i < 20; i++ {
			stats.Update(500 * time.Millisecond)
		}

		// 3. Get Median After (Should be near 500ms)
		medianAfter := getMedian()

		// 4. Verify
		// Use a margin of error or simple greater check
		t.Logf("Median Before: %.2f ms, Median After: %.2f ms", medianBefore, medianAfter)
		require.Greater(t, medianAfter, medianBefore)
	})
}

func BenchmarkThompsonStats_SampleParallel(b *testing.B) {
	ts := NewThompsonStats(10*time.Millisecond, 1, 1, 1)

	// Add some sample data
	for i := 0; i < 1000; i++ {
		ts.Update(time.Duration(5+i) * time.Millisecond)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			_ = ts.Sample(rng)
		}
	})
}
