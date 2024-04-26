package throttler

import (
	"context"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func mockThrottlerTest(throttler Throttler, ctx context.Context, r *int) {
	throttler.Throttle(ctx)
	*r++
}

func TestDispatchThrottler(t *testing.T) {
	t.Run("throttler_will_release_only_when_ticked", func(t *testing.T) {
		testThrottler := newDispatchThrottler(1 * time.Hour)

		i := 0
		var goFuncDone sync.WaitGroup
		goFuncDone.Add(1)
		var goFuncInitiated sync.WaitGroup
		goFuncInitiated.Add(1)

		ctx := context.Background()

		go func() {
			goFuncInitiated.Done()
			ctx = grpc_ctxtags.SetInContext(ctx, grpc_ctxtags.NewTags())
			mockThrottlerTest(testThrottler, ctx, &i)
			goFuncDone.Done()
		}()

		goFuncInitiated.Wait()
		require.Equal(t, 0, i)

		testThrottler.nonBlockingSend(testThrottler.throttlingQueue)
		goFuncDone.Wait()
		require.Equal(t, 1, i)
		require.True(t, grpc_ctxtags.Extract(ctx).Has(telemetry.Throttled))
	})
}
