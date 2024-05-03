package throttler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func mockThrottlerTest(ctx context.Context, throttler Throttler, r *int) {
	throttler.Throttle(ctx)
	*r++
}

func TestDispatchThrottler(t *testing.T) {
	t.Run("throttler_will_release_only_when_ticked", func(t *testing.T) {
		testThrottler := newConstantRateThrottler(1*time.Hour, "test")

		i := 0
		var goFuncDone sync.WaitGroup
		goFuncDone.Add(1)
		var goFuncInitiated sync.WaitGroup
		goFuncInitiated.Add(1)

		ctx := context.Background()

		go func() {
			goFuncInitiated.Done()
			mockThrottlerTest(ctx, testThrottler, &i)
			goFuncDone.Done()
		}()

		goFuncInitiated.Wait()
		require.Equal(t, 0, i)

		testThrottler.nonBlockingSend(testThrottler.throttlingQueue)
		goFuncDone.Wait()
		require.Equal(t, 1, i)
	})
}
