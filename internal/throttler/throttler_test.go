package throttler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestConstantRateThrottler(t *testing.T) {
	testThrottler := newConstantRateThrottler(1*time.Hour, "test")
	t.Cleanup(func() {
		testThrottler.Close()
		goleak.VerifyNone(t)
	})

	t.Run("throttler_will_release_only_when_ticked", func(t *testing.T) {
		counter := 0

		ctx := context.Background()

		var wg sync.WaitGroup

		wg.Add(1)
		go func(counter *int) {
			defer wg.Done()
			testThrottler.Throttle(ctx)
			*counter++
		}(&counter)

		time.Sleep(100 * time.Millisecond) // Wait for the goroutine to attempt to throttle
		require.Equal(t, 0, counter)
		testThrottler.throttlingQueue <- struct{}{}
		wg.Wait()
		require.Equal(t, 1, counter)
	})
}
