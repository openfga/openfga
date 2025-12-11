package containers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBag(t *testing.T) {
	var b Bag[int]

	for i := range 1000 {
		b.Add(i + 1)
	}

	values := b.Seq()

	var count int

	cmp := 1000
	for i := range values {
		require.Equal(t, cmp, i)
		cmp--
		count++
	}
	require.Equal(t, 1000, count)
}

func BenchmarkBag(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var b Bag[int]
		for i := range 1000 {
			b.Add(i)
		}
	}
}
