package track

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkStatusPool(b *testing.B) {
	const concurrency = 1000

	var sp StatusPool

	reporters := make([]Reporter, concurrency)

	for i := range concurrency {
		reporters[i] = sp.Register()
	}

	for b.Loop() {
		var wg sync.WaitGroup

		for i := range concurrency {
			wg.Add(1)
			go func() {
				defer wg.Done()
				reporters[i].Report(false)
			}()
		}
		wg.Wait()
		value := sp.Status()
		require.False(b, value)
	}
}
