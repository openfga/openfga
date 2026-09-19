package condition

import (
	"context"
	"sync"

	"golang.org/x/sync/singleflight"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage/cache/keys"
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

// inlineConditionCacheKey builds a deterministic string key from the full tuple
// including condition name and context payload. Two tuples can share the same
// object+relation+user but carry distinct inline expressions or parameter schemas
// (e.g. via CombinedTupleReader mixing stored and contextual tuples), so the key
// must cover the complete condition content, not just the base tuple identity.
func inlineConditionCacheKey(tk *openfgav1.TupleKey) string {
	b := keys.GetBuilder()
	defer b.Close()
	(*keys.Tuple)(tk).WriteTo(b.Builder)
	return string(b.Bytes())
}
