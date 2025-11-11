package check

import (
	"context"
	"slices"
	"sync"

	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/condition/eval"
	"github.com/openfga/openfga/internal/iterator"
	"github.com/openfga/openfga/internal/modelgraph"
)

func evaluateCondition(ctx context.Context, model *modelgraph.AuthorizationModelGraph, edge *authzGraph.WeightedAuthorizationModelEdge, t *openfgav1.TupleKey, reqCtx *structpb.Struct) (bool, error) {
	if !slices.Contains(edge.GetConditions(), t.GetCondition().GetName()) {
		return false, nil
	}

	return eval.EvaluateTupleCondition(ctx, t, model.GetConditions()[t.GetCondition().GetName()], reqCtx)
}

func BuildConditionTupleKeyFilter(ctx context.Context, model *modelgraph.AuthorizationModelGraph, edge *authzGraph.WeightedAuthorizationModelEdge, reqCtx *structpb.Struct) iterator.FilterFunc[*openfgav1.TupleKey] {
	return func(_ iterator.OperationType, t *openfgav1.TupleKey) (bool, error) {
		return evaluateCondition(ctx, model, edge, t, reqCtx)
	}
}

func BuildUniqueTupleKeyFilter(visited *sync.Map, keyFunc func(key *openfgav1.TupleKey) string) iterator.FilterFunc[*openfgav1.TupleKey] {
	return func(op iterator.OperationType, tk *openfgav1.TupleKey) (bool, error) {
		key := keyFunc(tk)
		var seen bool
		if op == iterator.OperationHead {
			_, seen = visited.Load(key)
		} else {
			_, seen = visited.LoadOrStore(key, struct{}{})
		}
		return !seen, nil
	}
}
