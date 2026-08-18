package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBucketize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    uint
		buckets  []uint
		expected string
	}{
		{
			name:     "single_bucket_smaller",
			input:    uint(20),
			buckets:  []uint{40},
			expected: "40",
		},
		{
			name:     "single_bucket_equal",
			input:    uint(40),
			buckets:  []uint{40},
			expected: "40",
		},
		{
			name:     "single_bucket_larger",
			input:    uint(60),
			buckets:  []uint{40},
			expected: "+Inf",
		},
		{
			name:     "multiple_bucket_smaller",
			input:    uint(20),
			buckets:  []uint{40, 60},
			expected: "40",
		},
		{
			name:     "multiple_bucket_equal",
			input:    uint(40),
			buckets:  []uint{40, 60},
			expected: "40",
		},
		{
			name:     "multiple_bucket_between",
			input:    uint(50),
			buckets:  []uint{40, 60},
			expected: "60",
		},
		{
			name:     "multiple_bucket_larger",
			input:    uint(61),
			buckets:  []uint{40, 60},
			expected: "+Inf",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			output := Bucketize(test.input, test.buckets)
			require.Equal(t, test.expected, output)
		})
	}
}

func TestLinearBuckets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		minValue float64
		maxValue float64
		count    int
		expected []float64
	}{
		{
			name:     "integral_width",
			minValue: 1.0,
			maxValue: 100,
			count:    10,
			expected: []float64{1, 12, 23, 34, 45, 56, 67, 78, 89, 100},
		},
		{
			// The width here (100/9) is not representable in binary floating point, so
			// accumulating it drops the final bucket and stops short of maxValue.
			name:     "repeating_width_keeps_count_and_endpoint",
			minValue: 0,
			maxValue: 100,
			count:    10,
			expected: []float64{
				0,
				100.0 / 9 * 1,
				100.0 / 9 * 2,
				100.0 / 9 * 3,
				100.0 / 9 * 4,
				100.0 / 9 * 5,
				100.0 / 9 * 6,
				100.0 / 9 * 7,
				100.0 / 9 * 8,
				100,
			},
		},
		{
			name:     "fractional_width_keeps_count_and_endpoint",
			minValue: 0,
			maxValue: 0.3,
			count:    4,
			expected: []float64{0, 0.1, 0.2, 0.3},
		},
		{
			name:     "single_bucket",
			minValue: 5,
			maxValue: 100,
			count:    1,
			expected: []float64{5},
		},
		{
			name:     "two_buckets_are_the_endpoints",
			minValue: 5,
			maxValue: 100,
			count:    2,
			expected: []float64{5, 100},
		},
		{
			name:     "zero_count",
			minValue: 0,
			maxValue: 100,
			count:    0,
			expected: nil,
		},
		{
			name:     "negative_count",
			minValue: 0,
			maxValue: 100,
			count:    -1,
			expected: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			buckets := LinearBuckets(test.minValue, test.maxValue, test.count)

			require.Len(t, buckets, len(test.expected))
			require.InDeltaSlice(t, test.expected, buckets, 1e-9)

			if len(buckets) > 0 {
				// The interval is closed, so both endpoints must be exact.
				require.Equal(t, test.minValue, buckets[0])
				require.Equal(t, test.expected[len(test.expected)-1], buckets[len(buckets)-1])
			}

			if len(buckets) > 1 {
				require.IsIncreasing(t, buckets)
			}
		})
	}
}
