package condition

import (
	"context"
	"sync"

	"golang.org/x/sync/singleflight"
)

// inlineConditionCache is a request-scoped cache of compiled inline conditions.
// It is stored in a context.Context and shared across all goroutines within the
// same request via context propagation.
type inlineConditionCache struct {
	mu sync.RWMutex
	m  map[string]*EvaluableCondition
	sf singleflight.Group
}

type inlineConditionCacheCtxKey struct{}

// NewContextWithInlineConditionCache returns ctx with a fresh per-request
// compiled-condition cache attached. Call once at the start of each top-level
// server handler (Check, ListObjects, ListUsers). All sub-handlers and
// concurrent goroutines inherit it through context propagation.
func NewContextWithInlineConditionCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, inlineConditionCacheCtxKey{}, &inlineConditionCache{
		m: make(map[string]*EvaluableCondition),
	})
}
