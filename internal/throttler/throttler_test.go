package throttler

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/stretchr/testify/require"
)

func mockThrottlerTest(ctx context.Context, throttler Throttler, counter *int) {
	throttler.Throttle(ctx)
	*counter++
}

func TestDispatchThrottler(t *testing.T) {
	testThrottler := newConstantRateThrottler(1*time.Hour, "test")
	t.Cleanup(func() {
		testThrottler.Close()
		goleak.VerifyNone(t)
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
