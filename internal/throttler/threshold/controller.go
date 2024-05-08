package threshold

import (
	"context"

	"github.com/openfga/openfga/internal/throttler"
)

type dispatchThrottlingThresholdType uint32

const (
	dispatchThrottlingThreshold dispatchThrottlingThresholdType = iota
)

type Config struct {
	Enabled      bool
	Throttler    throttler.Throttler
	Threshold    uint32
	MaxThreshold uint32
}

func ShouldThrottle(ctx context.Context, currentCount uint32, defaultThreshold uint32, maxThreshold uint32) bool {
	threshold := defaultThreshold

	if maxThreshold == 0 {
		maxThreshold = defaultThreshold
	}

	thresholdInCtx := ThrottlingThresholdFromContext(ctx)

	if thresholdInCtx > 0 {
		threshold = min(thresholdInCtx, maxThreshold)
	}

	return currentCount > threshold
}

// ContextWithThrottlingThreshold will save the dispatch throttling threshold in context.
func ContextWithThrottlingThreshold(ctx context.Context, threshold uint32) context.Context {
	return context.WithValue(ctx, dispatchThrottlingThreshold, threshold)
}

// ThrottlingThresholdFromContext returns the dispatch throttling threshold saved in context
// Return 0 if not found.
func ThrottlingThresholdFromContext(ctx context.Context) uint32 {
	thresholdInContext := ctx.Value(dispatchThrottlingThreshold)
	if thresholdInContext != nil {
		thresholdInInt, ok := thresholdInContext.(uint32)
		if ok {
			return thresholdInInt
		}
	}
	return 0
}
