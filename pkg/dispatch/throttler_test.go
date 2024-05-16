package dispatch

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestContextWithThrottlingThreshold(t *testing.T) {
	ctx := ContextWithThrottlingThreshold(context.Background(), 20)
	thresholdInContext := ThrottlingThresholdFromContext(ctx)
	require.Equal(t, uint32(20), thresholdInContext)
}

func TestThrottlingThresholdFromContext(t *testing.T) {
	ctx := context.Background()
	thresholdInContext := ThrottlingThresholdFromContext(ctx)
	require.Equal(t, uint32(0), thresholdInContext)

	ctx = ContextWithThrottlingThreshold(ctx, 20)
	thresholdInContext = ThrottlingThresholdFromContext(ctx)
	require.Equal(t, uint32(20), thresholdInContext)
}
