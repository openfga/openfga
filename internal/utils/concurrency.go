package utils

import (
	"context"

	"github.com/sourcegraph/conc/pool"
)

// NewPool returns a new pool where each task respects context cancellation.
// Wait() will only return the first error seen.
func NewPool(ctx context.Context, maxGoroutines int) *pool.ContextPool {
	return pool.New().
		WithContext(ctx).
		WithCancelOnError().
		WithFirstError().
		WithMaxGoroutines(maxGoroutines)
}
