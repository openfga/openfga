package condition

import (
	"context"
	"sync"

	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/types/known/structpb"

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

// inlineConditionCacheKey builds a deterministic string key from the condition
// name and context payload only. compileInlineExpression depends solely on the
// condition payload, so keying by the full tuple (object+relation+user) would
// cause every distinct tuple carrying the same expression to compile a separate
// CEL program, defeating the request cache for ListObjects/ListUsers fan-outs.
func inlineConditionCacheKey(tk *openfgav1.TupleKey) string {
	b := keys.GetBuilder()
	defer b.Close()
	cond := tk.GetCondition()
	keys.String(cond.GetName()).WriteTo(b.Builder)
	(*keys.PbValue)(structpb.NewStructValue(cond.GetContext())).WriteTo(b.Builder)
	return b.Key().String()
}
