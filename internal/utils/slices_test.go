package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUniq(t *testing.T) {
	require.ElementsMatch(t, []string{"x", "y"}, Uniq([]string{"x", "x", "y", "y"}))
	require.ElementsMatch(t, []int{1, 2}, Uniq([]int{1, 1, 2, 2}))
}

func TestChunk(t *testing.T) {
	stringChunks := Chunk([]string{"1", "2"}, 1)
	require.ElementsMatch(t, [][]string{{"1"}, {"2"}}, stringChunks)

	intChunks := Chunk([]int{1, 2, 3}, 2)
	require.ElementsMatch(t, [][]int{{1, 2}, {3}}, intChunks)
}
