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

// BenchmarkEvaluateInlineExpression_Repeated measures evaluating the same expression
// tuple 1000 times. Each call compiles the CEL expression on demand.
func BenchmarkEvaluateInlineExpression_Repeated(b *testing.B) {
	const repeats = 1000

	tk := makeTupleKeyWithExpr("document:1", "user:alice", "channel == 'X123'")
	reqCtx, _ := structpb.NewStruct(map[string]interface{}{"channel": "X123"})
	ctx := context.Background()

	for b.Loop() {
		for range repeats {
			_, _ = eval.EvaluateInlineExpression(ctx, tk, reqCtx)
		}
	}
}

// BenchmarkEvaluateInlineExpression_Distinct measures evaluating 100 tuples
// each with a unique expression, exercising full compile-per-tuple cost.
func BenchmarkEvaluateInlineExpression_Distinct(b *testing.B) {
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
	ctx := context.Background()

	for b.Loop() {
		for _, tk := range tuples {
			_, _ = eval.EvaluateInlineExpression(ctx, tk, reqCtx)
		}
	}
}
