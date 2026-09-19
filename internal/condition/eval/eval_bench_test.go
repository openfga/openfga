package eval_test

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/condition/eval"
)

// makeTupleKeyWithExpr returns a TupleKey whose $expression condition uses exprStr
// with one string parameter named "channel".
func makeTupleKeyWithExpr(object, user, exprStr string) *openfgav1.TupleKey {
	ctx, _ := structpb.NewStruct(map[string]interface{}{
		"expression": exprStr,
		"parameters": map[string]interface{}{"channel": "string"},
	})
	return &openfgav1.TupleKey{
		Object:   object,
		Relation: "viewer",
		User:     user,
		Condition: &openfgav1.RelationshipCondition{
			Name:    condition.InlineExpressionName,
			Context: ctx,
		},
	}
}

// BenchmarkEvaluateInlineExpression_CacheHit simulates a tuple that is evaluated
// many times within the same request (e.g., the same userset tuple dispatched across
// multiple concurrent resolver paths, or the validation pass populating the cache
// before the evaluation pass). The first evaluation compiles; the remaining 999
// calls return the cached compiled condition.
func BenchmarkEvaluateInlineExpression_CacheHit(b *testing.B) {
	const repeats = 1000

	tk := makeTupleKeyWithExpr("document:1", "user:alice", "channel == 'X123'")
	reqCtx, _ := structpb.NewStruct(map[string]interface{}{"channel": "X123"})

	for b.Loop() {
		ctx := condition.NewContextWithInlineConditionCache(context.Background())
		for range repeats {
			_, _ = eval.EvaluateInlineExpression(ctx, tk, reqCtx)
		}
	}
}

// BenchmarkEvaluateInlineExpression_CacheMiss establishes the worst-case baseline:
// every tuple in the scan has a distinct (object, user) pair and a unique expression,
// so every evaluation is a cache miss and triggers a full CEL compilation.
func BenchmarkEvaluateInlineExpression_CacheMiss(b *testing.B) {
	const n = 100

	tuples := make([]*openfgav1.TupleKey, n)
	for i := range n {
		tuples[i] = makeTupleKeyWithExpr(
			fmt.Sprintf("document:%d", i),
			fmt.Sprintf("user:%d", i),
			fmt.Sprintf("channel == 'X%d'", i),
		)
	}

	reqCtx, _ := structpb.NewStruct(map[string]interface{}{"channel": "X0"})

	for b.Loop() {
		ctx := condition.NewContextWithInlineConditionCache(context.Background())
		for _, tk := range tuples {
			_, _ = eval.EvaluateInlineExpression(ctx, tk, reqCtx)
		}
	}
}
