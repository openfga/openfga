package reverseexpand

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkStatusPool(b *testing.B) {
	var concurrency int = 1000

	var sp StatusPool

	reporters := make([]Reporter, concurrency)

	for i := range concurrency {
		reporters[i] = sp.Register()
	}

	var on bool

	for b.Loop() {
		var wg sync.WaitGroup

		for i := range concurrency {
			on = false
			wg.Add(1)
			go func(on bool) {
				defer wg.Done()
				reporters[i].Report(on)
			}(on)
		}
		wg.Wait()
		value := sp.Status()
		require.True(b, !value)
	}
}
