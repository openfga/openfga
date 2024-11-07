package eval

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestEvaluateTupleCondition(t *testing.T) {
	tests := []struct {
		name         string
		tupleKey     *openfgav1.TupleKey
		model        *openfgav1.AuthorizationModel
		context      map[string]interface{}
		conditionMet bool
		expectedErr  string
	}{
		{
			name:     "condition_in_tuple_key_not_found_in_model",
			tupleKey: tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:maria", "unknown", nil),
			model: parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user

				type document
					relations
						define can_view: [user with correct_ip]

				condition correct_ip(ip: string) {
					ip == "192.168.0.1"
				}`),
			context:      map[string]interface{}{"ip": "192.168.0.1"},
			conditionMet: false,
			expectedErr:  "'unknown' - condition was not found",
		},
		{
			name:     "condition_not_met",
			tupleKey: tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:maria", "correct_ip", nil),
			model: parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user

				type document
					relations
						define can_view: [user with correct_ip]

				condition correct_ip(ip: string) {
					ip == "192.168.0.1"
				}`),
			context:      map[string]interface{}{"ip": "not_met"},
			conditionMet: false,
			expectedErr:  "",
		},
		{
			name:     "condition_met",
			tupleKey: tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:maria", "correct_ip", nil),
			model: parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user

				type document
					relations
						define can_view: [user with correct_ip]

				condition correct_ip(ip: string) {
					ip == "192.168.0.1"
				}`),
			context:      map[string]interface{}{"ip": "192.168.0.1"},
			conditionMet: true,
			expectedErr:  "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()

			ts, err := typesystem.NewAndValidate(context.Background(), test.model)
			require.NoError(t, err)

			contextStruct, err := structpb.NewStruct(test.context)
			require.NoError(t, err)

			condEvalResult, err := EvaluateTupleCondition(ctx, test.tupleKey, ts, contextStruct)
			if err != nil {
				var evalError *condition.EvaluationError
				require.ErrorAs(t, err, &evalError)
				require.EqualError(t, evalError, test.expectedErr)
			} else {
				require.Empty(t, test.expectedErr)
				require.Equal(t, test.conditionMet, condEvalResult.ConditionMet)
			}
		})
	}
}

// TestDefaultCELEvaluationCost is used to ensure we don't decreasee the default evaluation cost
// of CEL expressions, which would break API compatibility.
//
// Critical paths involving ABAC Condition evaluations use the EvaluateTupleCondition function,
// and so we test that directly to give us higher confidence we're not introducing a compatibility
// issue.
func TestDefaultCELEvaluationCost(t *testing.T) {
	tests := []struct {
		name     string
		model    *openfgav1.AuthorizationModel
		tupleKey *openfgav1.TupleKey
		context  map[string]any
		result   *condition.EvaluationResult
	}{
		{
			name: "list_comprehension",
			model: parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user

				type document
					relations
						define can_view: [user with str_cond]

				condition str_cond(s: list<string>) {
					"98" in s
				}`),
			tupleKey: tuple.NewTupleKeyWithCondition("document:1", "can_view", "user:jon", "str_cond", nil),
			context: map[string]any{
				"s": testutils.MakeSliceWithGenerator[any](99, testutils.NumericalStringGenerator),
			},
			result: &condition.EvaluationResult{
				Cost:         100,
				ConditionMet: true,
			},
		},
		{
			name: "string_contains",
			model: parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user

				type document
					relations
						define can_view: [user with str_cond]

				condition str_cond(s: string) {
					s.contains("b")
				}`),
			tupleKey: tuple.NewTupleKeyWithCondition("document:1", "can_view", "user:jon", "str_cond", nil),
			context: map[string]any{
				// see https://github.com/google/cel-go/blob/cfbf821f1b458533051306305a39b743db7c4bdb/checker/cost.go#L604-L609
				"s": testutils.MakeStringWithRuneset(990, []rune{'a'}), // string has cost factor of 0.1, so 990*0.1 < 100
			},
			result: &condition.EvaluationResult{
				Cost:         100,
				ConditionMet: false,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			ts, err := typesystem.NewAndValidate(context.Background(), test.model)
			require.NoError(t, err)

			contextStruct, err := structpb.NewStruct(test.context)
			require.NoError(t, err)

			condEvalResult, err := EvaluateTupleCondition(ctx, test.tupleKey, ts, contextStruct)
			require.NoError(t, err)

			require.Equal(t, test.result, condEvalResult)
		})
	}
}
