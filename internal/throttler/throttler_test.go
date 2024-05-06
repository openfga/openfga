package throttler

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/openfga/openfga/pkg/telemetry"

	"github.com/stretchr/testify/require"
)

func mockThrottlerTest(ctx context.Context, throttler Throttler, counter *int) {
	throttler.Throttle(ctx)
	*counter++
}

func TestDispatchThrottler(t *testing.T) {
	testThrottler := newConstantRateThrottler(1*time.Hour, "test")
	t.Cleanup(func() {
		goleak.VerifyNone(t)
		testThrottler.Close()
	})

	t.Run("throttler_will_release_only_when_ticked", func(t *testing.T) {

		counter := 0
		var goFuncDone sync.WaitGroup
		goFuncDone.Add(1)
		var goFuncInitiated sync.WaitGroup
		goFuncInitiated.Add(1)

		ctx := context.Background()

		go func() {
			goFuncInitiated.Done()
			mockThrottlerTest(ctx, testThrottler, &counter)
			goFuncDone.Done()
		}()

		goFuncInitiated.Wait()
		require.Equal(t, 0, counter)

		testThrottler.nonBlockingSend(testThrottler.throttlingQueue)
		goFuncDone.Wait()
		require.Equal(t, 1, counter)
	})
}

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
		telemetry.ContextWithDispatchThrottlingThreshold(ctx, 200)
		require.True(t, ShouldThrottle(ctx, 190, 0, 210))
	})

	t.Run("should_respect_max_threshold", func(t *testing.T) {
		ctx := context.Background()
		telemetry.ContextWithDispatchThrottlingThreshold(ctx, 300)
		require.True(t, ShouldThrottle(ctx, 190, 100, 200))
	})
}
