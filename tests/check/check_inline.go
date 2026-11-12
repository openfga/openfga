package check

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

// The matrix model has one named condition:
//
//	condition xcond(x: string) {
//	  x == '1'
//	}
//
// The inline form of the matrix replaces it with $expression tuples that carry
// the same expression and the same parameter type. No tuple in the matrix
// stores a value for x, so the rewrite needs nothing else.
const (
	matrixNamedCondition   = "xcond"
	matrixInlineCondition  = "$expression"
	matrixInlineExpression = "x == '1'"
)

// InlineMatrixModel turns the matrix model into its $expression form: every
// type restriction that used the named condition uses $expression, and the
// named condition is removed.
func InlineMatrixModel(model string) string {
	start := strings.Index(model, "condition "+matrixNamedCondition)
	if start < 0 {
		panic("matrix model has no condition " + matrixNamedCondition)
	}
	model = model[:start]
	if strings.Contains(model, "condition ") {
		panic("matrix model has a condition the inline rewrite does not know")
	}
	return strings.ReplaceAll(model, " with "+matrixNamedCondition, " with "+matrixInlineCondition)
}

// inlineMatrixStages returns a copy of the stages whose tuples use
// $expression instead of the named condition. The stages passed in are shared
// by tests that run in parallel and are not modified.
func inlineMatrixStages(stages []*stage) []*stage {
	out := make([]*stage, 0, len(stages))
	for _, s := range stages {
		c := &stage{Name: s.Name, Model: s.Model, Tuples: InlineMatrixTuples(s.Tuples)}
		for _, a := range s.CheckAssertions {
			copied := *a
			copied.ContextualTuples = InlineMatrixTuples(a.ContextualTuples)
			c.CheckAssertions = append(c.CheckAssertions, &copied)
		}
		out = append(out, c)
	}
	return out
}

// InlineMatrixTuples returns a copy of the tuples in which every use of the
// named condition is a $expression tuple with the same expression.
func InlineMatrixTuples(tuples []*openfgav1.TupleKey) []*openfgav1.TupleKey {
	if tuples == nil {
		return nil
	}
	out := make([]*openfgav1.TupleKey, 0, len(tuples))
	for _, tk := range tuples {
		copied := proto.Clone(tk).(*openfgav1.TupleKey)
		if cond := copied.GetCondition(); cond != nil {
			if cond.GetName() != matrixNamedCondition || len(cond.GetContext().GetFields()) > 0 {
				panic(fmt.Sprintf("matrix tuple %s has a condition the inline rewrite does not know", tk))
			}
			copied.Condition = &openfgav1.RelationshipCondition{
				Name: matrixInlineCondition,
				Context: &structpb.Struct{Fields: map[string]*structpb.Value{
					"expression": structpb.NewStringValue(matrixInlineExpression),
					"parameters": structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
						"x": structpb.NewStringValue("string"),
					}}),
				}},
			}
		}
		out = append(out, copied)
	}
	return out
}
