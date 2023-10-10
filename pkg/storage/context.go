package storage

import (
	"context"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

type ctxKey string

var (
	contextualTupleContextKey = ctxKey("contextual-tuples")
)

func ContextWithContextualTuples(ctx context.Context, tuples []*openfgav1.TupleKey) context.Context {
	return context.WithValue(ctx, contextualTupleContextKey, tuples)
}

func ContextualTuplesFromContext(ctx context.Context) ([]*openfgav1.TupleKey, bool) {
	contextualTuples, ok := ctx.Value(contextualTupleContextKey).([]*openfgav1.TupleKey)
	return contextualTuples, ok
}
