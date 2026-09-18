package check

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/condition/eval"
	"github.com/openfga/openfga/internal/iterator"
	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/pkg/tuple"
)

func evaluateCondition(ctx context.Context, model *modelgraph.AuthorizationModelGraph, conditions []string, t *openfgav1.TupleKey, reqCtx *structpb.Struct) (bool, error) {
	name := t.GetCondition().GetName()

	if !slices.Contains(conditions, name) {
		return false, nil
	}

	if condition.IsInlineExpression(name) {
		return evalInlineCondition(ctx, t, reqCtx)
	}

	// consider converting slice to map for faster lookup once we see a large adoption in conditions
	return eval.EvaluateTupleCondition(ctx, t, model.GetConditions()[name], reqCtx)
}

func evalInlineCondition(ctx context.Context, t *openfgav1.TupleKey, reqCtx *structpb.Struct) (bool, error) {
	dynCond, err := condition.FromInlineExpression(t.GetCondition().GetContext())
	if err != nil {
		return false, err
	}

	var reqFields map[string]*structpb.Value
	if reqCtx != nil {
		reqFields = reqCtx.GetFields()
	}

	result, err := dynCond.Evaluate(ctx, reqFields)
	if err != nil {
		return false, err
	}

	if len(result.MissingParameters) > 0 {
		return false, condition.NewEvaluationError(
			condition.InlineExpressionName,
			fmt.Errorf("missing required parameters: %s", strings.Join(result.MissingParameters, ", ")),
		)
	}

	return result.ConditionMet, nil
}

func BuildConditionTupleKeyFilter(ctx context.Context, model *modelgraph.AuthorizationModelGraph, conditions []string, reqCtx *structpb.Struct) iterator.FilterFunc[*openfgav1.TupleKey] {
	return func(t *openfgav1.TupleKey) (bool, error) {
		return evaluateCondition(ctx, model, conditions, t, reqCtx)
	}
}

func BuildUniqueTupleKeyFilter(visited *sync.Map, keyFunc func(key *openfgav1.TupleKey) string) iterator.FilterFunc[*openfgav1.TupleKey] {
	return func(tk *openfgav1.TupleKey) (bool, error) {
		_, seen := visited.LoadOrStore(keyFunc(tk), struct{}{})
		return !seen, nil
	}
}

// usersetDedupKey keys a tuple by the userset (object#relation) it points to.
// Used by userset iterators, where the tuple's user is already an object#relation.
func usersetDedupKey(key *openfgav1.TupleKey) string {
	return key.GetUser() // this is a userset (object#relation)
}

// ttuDedupKey keys a TTU tuple by the tupleset user object joined with the
// tupleset relation, suffixed with the computed relation the TTU will dispatch
// to. For a tuple like `org:o1#parent@org:o2` resolving `billing_user from
// parent`, this yields `org:o2#parent@billing_user`.
//
// Including both the tupleset and computed relation keeps TTU-origin entries in
// a namespace distinct from userset entries (which are bare object#relation)
// and distinct per (tupleset, computed relation) pair, so recursive relations
// that share a single `visited` map (for example, two distinct `... From parent`
// relations reached from the same node) no longer collide on the bare parent
// object and wrongly drop each other's tuples.
func ttuDedupKey(tuplesetRelation, computedRelation string) func(key *openfgav1.TupleKey) string {
	return func(key *openfgav1.TupleKey) string {
		return tuple.ToObjectRelationString(key.GetUser(), tuplesetRelation) + "@" + computedRelation
	}
}
