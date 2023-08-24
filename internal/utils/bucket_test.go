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
			name:     "single_bucket_smaller",
			input:    uint(60),
			buckets:  []uint{40},
			expected: ">40",
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
			expected: ">60",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			output := Bucketize(test.input, test.buckets)
			require.Equal(t, test.expected, output)
		})
	}
}
