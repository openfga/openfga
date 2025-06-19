package utils

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilter(t *testing.T) {
	items := []int{
		1, 2, 3, 4, 5,
	}
	outcome := slices.Collect(Filter(items, func(item int) bool {
		return item%2 == 1
	}))
	require.Equal(t, []int{1, 3, 5}, outcome)

	// Test empty slice
	emptyOutcome := slices.Collect(Filter([]int{}, func(item int) bool {
		return item%2 == 1
	}))
	require.Empty(t, emptyOutcome)

	// Test filter none
	noneOutcome := slices.Collect(Filter(items, func(item int) bool {
		return item > 10
	}))
	require.Empty(t, noneOutcome)
}
