package utils

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test the resolution metadata by ensuring whether forking keeps new db counter and allows using of the same counter
func TestResolutionMetadata(t *testing.T) {
	resolutionCounter := NewResolutionMetadata()
	const numResolve = 5
	for i := 0; i < numResolve; i++ {
		resolutionCounter.AddResolve()
	}

	var wg sync.WaitGroup
	const numForLoop = 20
	wg.Add(numForLoop)

	for i := 0; i < numForLoop; i++ {
		forkedResolutionCounter := resolutionCounter.Fork()
		go func() {
			defer wg.Done()
			forkedResolutionCounter.AddResolve()
		}()
	}

	wg.Wait()

	require.EqualValues(t, numResolve, resolutionCounter.GetResolve())
}
