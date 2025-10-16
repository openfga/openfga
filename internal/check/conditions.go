package check

import (
	"context"
	"slices"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	authzGraph "github.com/openfga/language/pkg/go/graph"
	"github.com/openfga/openfga/internal/condition/eval"
	"google.golang.org/protobuf/types/known/structpb"
)

func evaluateCondition(ctx context.Context, model *AuthorizationModelGraph, edge *authzGraph.WeightedAuthorizationModelEdge, t *openfgav1.Tuple, reqCtx *structpb.Struct) (bool, error) {
	if !slices.Contains(edge.GetConditions(), t.GetKey().GetCondition().GetName()) { 
		return false, nil
	}

	// if the tuple doesn't have a condition, the function exits early
	return eval.EvaluateTupleCondition(ctx, t.GetKey(), model.conditions[t.GetKey().GetCondition().GetName()], reqCtx)
}
