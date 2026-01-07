package threshold

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/openfga/openfga/pkg/dispatch"
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

	type Tests struct {
		name             string
		thresholdInCtx   uint32
		currentCount     uint32
		defaultThreshold uint32
		maxThreshold     uint32
		expectedResult   bool
	}

	tests := []Tests{
		{
			name:             "override_default_threshold_and_not_throttle",
			thresholdInCtx:   200,
			currentCount:     190,
			defaultThreshold: 100,
			maxThreshold:     210,
			expectedResult:   false,
		},
		{
			name:             "override_default_threshold_and_throttle",
			thresholdInCtx:   200,
			currentCount:     205,
			defaultThreshold: 100,
			maxThreshold:     210,
			expectedResult:   true,
		},
		{
			name:             "use_max_threshold_when_higher_than_set_in_ctx",
			thresholdInCtx:   1000,
			currentCount:     301,
			defaultThreshold: 100,
			maxThreshold:     300,
			expectedResult:   true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			ctx = dispatch.ContextWithThrottlingThreshold(ctx, test.thresholdInCtx)
			require.Equal(t, test.expectedResult, ShouldThrottle(ctx, test.currentCount, test.defaultThreshold, test.maxThreshold))
		})
	}
}
