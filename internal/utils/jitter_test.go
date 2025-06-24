package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJitterDuration(t *testing.T) {
	t.Parallel()
	t.Run("NoJitter", func(t *testing.T) {
		base := 2 * time.Second
		maxJitter := 0 * time.Millisecond

		result := JitterDuration(base, maxJitter)
		require.Equal(t, base, result)
	})
	t.Run("WithinBounds", func(t *testing.T) {
		base := 1 * time.Second
		maxJitter := 500 * time.Millisecond

		for i := 0; i < 100; i++ {
			result := JitterDuration(base, maxJitter)

			require.GreaterOrEqual(t, result, base)
			require.LessOrEqual(t, result, base+maxJitter)
		}
	})
}
