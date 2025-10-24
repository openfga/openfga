package check

import (
	"context"
	"slices"

	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/condition/eval"
)

func evaluateCondition(ctx context.Context, model *AuthorizationModelGraph, edge *authzGraph.WeightedAuthorizationModelEdge, t *openfgav1.TupleKey, reqCtx *structpb.Struct) (bool, error) {
	// TODO: language has to change NoCond to ""
	if !slices.Contains(edge.GetConditions(), t.GetCondition().GetName()) {
		return false, nil
	}

	if t.GetCondition().GetName() != "" {
		// if the tuple has a condition condition, then the condition needs to be evaluated
		return eval.EvaluateTupleCondition(ctx, t, model.conditions[t.GetCondition().GetName()], reqCtx)
	}

	return true, nil
}

func BuildTupleKeyConditionFilter(ctx context.Context, model *AuthorizationModelGraph, edge *authzGraph.WeightedAuthorizationModelEdge, reqCtx *structpb.Struct) func(*openfgav1.TupleKey) (bool, error) {
	return func(t *openfgav1.TupleKey) (bool, error) {
		return evaluateCondition(ctx, model, edge, t, reqCtx)
	}
}
