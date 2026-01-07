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
	buckets := LinearBuckets(1.0, 100, 10)
	require.Equal(t, []float64{1, 12, 23, 34, 45, 56, 67, 78, 89, 100}, buckets)
}
