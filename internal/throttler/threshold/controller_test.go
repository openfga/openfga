package threshold

import (
	"context"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"testing"
)

func TestShouldThrottle(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("expect_should_throttle_logic_to_work", func(t *testing.T) {
		ctx := context.Background()
		require.False(t, ShouldThrottle(ctx, 190, 200, 200))
		require.True(t, ShouldThrottle(ctx, 201, 200, 200))
		require.False(t, ShouldThrottle(ctx, 190, 200, 0))
	})

	t.Run("should_respect_max_threshold", func(t *testing.T) {
		ctx := context.Background()
		ContextWithDispatchThrottlingThreshold(ctx, 200)
		require.True(t, ShouldThrottle(ctx, 190, 0, 210))
	})

	t.Run("should_respect_max_threshold", func(t *testing.T) {
		ctx := context.Background()
		ContextWithDispatchThrottlingThreshold(ctx, 300)
		require.True(t, ShouldThrottle(ctx, 190, 100, 200))
	})
}
