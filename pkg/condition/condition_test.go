package condition_test

import (
	"fmt"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/condition"
	"github.com/stretchr/testify/require"
)

func TestNewCompiled(t *testing.T) {
	var tests = []struct {
		name      string
		condition *openfgav1.Condition
		err       error
	}{
		{
			name: "valid",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "param1 == 'ok'",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"param1": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
					},
				},
			},
			err: nil,
		},
		{
			name: "invalid_parameter_type",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "param1 == 'ok'",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"param1": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_UNSPECIFIED,
					},
				},
			},
			err: fmt.Errorf("failed to decode parameter type for parameter 'param1': unknown condition parameter type `TYPE_NAME_UNSPECIFIED`"),
		},
		{
			name: "invalid_expression",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "invalid",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"param1": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
					},
				},
			},
			err: &condition.CompilationError{
				Expression: "invalid",
				Cause:      fmt.Errorf("ERROR: condition1:1:1: undeclared reference to 'invalid' (in container '')\n | invalid\n | ^"),
			},
		},
		{
			name: "invalid_output_type",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "param1",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"param1": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
					},
				},
			},
			err: fmt.Errorf("expected a bool condition expression output, but got 'string'"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := condition.NewCompiled(test.condition)

			if test.err != nil {
				require.Equal(t, err, test.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestEvaluate(t *testing.T) {
	var tests = []struct {
		name      string
		condition *openfgav1.Condition
		context   map[string]interface{}
		result    condition.EvaluationResult
		err       error
	}{
		{
			name: "success_condition_met",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "param1 == 'ok'",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"param1": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
					},
				},
			},
			context: map[string]interface{}{
				"param1": "ok",
			},
			result: condition.EvaluationResult{ConditionMet: true},
			err:    nil,
		},
		{
			name: "success_condition_unmet",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "param1 == 'ok'",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"param1": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
					},
				},
			},
			context: map[string]interface{}{"param1": "notok"},
			result:  condition.EvaluationResult{ConditionMet: false},
			err:     nil,
		},
		{
			name: "fail_no_such_attribute",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "param1 == 'ok'",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"param1": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
					},
				},
			},
			context: map[string]interface{}{
				"param2": "ok",
			},
			result: condition.EvaluationResult{ConditionMet: false},
			err:    fmt.Errorf("failed to evaluate condition expression: no such attribute(s): param1"),
		},
		{
			name: "fail_unexpected_type",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "param1 == 'ok'",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"param1": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
					},
				},
			},
			context: map[string]interface{}{
				"param1": true,
			},
			result: condition.EvaluationResult{ConditionMet: false},
			err:    fmt.Errorf("failed to convert context to typed parameter values: failed to convert context parameter 'param1': for string: unexpected type value '\"bool\"', expected 'string'"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			compiledCondition, err := condition.NewCompiled(test.condition)
			require.NoError(t, err)

			result, err := compiledCondition.Evaluate(test.context)

			require.Equal(t, test.result, result)
			if test.err != nil {
				require.Equal(t, test.err, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
