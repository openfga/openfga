package condition_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/condition/types"
)

func TestNewCompiled(t *testing.T) {
	var tests = []struct {
		name      string
		condition *openfgav1.Condition
		err       *condition.CompilationError
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
			err: &condition.CompilationError{
				Condition: "condition1",
				Cause:     fmt.Errorf("failed to decode parameter type for parameter 'param1': unknown condition parameter type `TYPE_NAME_UNSPECIFIED`"),
			},
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
				Condition: "condition1",
				Cause:     fmt.Errorf("ERROR: condition1:1:1: undeclared reference to 'invalid' (in container '')\n | invalid\n | ^"),
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
			err: &condition.CompilationError{
				Condition: "condition1",
				Cause:     fmt.Errorf("expected a bool condition expression output, but got 'string'"),
			},
		},
		{
			name: "ipaddress_literal_malformed_bool",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: `ipaddress(true).in_cidr("192.168.0.0/24")`,
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{},
			},
			err: &condition.CompilationError{
				Condition: "condition1",
				Cause:     fmt.Errorf("ERROR: condition1:1:10: found no matching overload for 'ipaddress' applied to '(bool)'\n | ipaddress(true).in_cidr(\"192.168.0.0/24\")\n | .........^"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := condition.NewCompiled(test.condition)

			if test.err != nil {
				require.EqualError(t, err, test.err.Error())
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
		err       *condition.EvaluationError
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
		},
		{
			name: "fail_no_such_attribute_nil_context",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "param1 == 'ok'",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"param1": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
					},
				},
			},
			context: nil,
			result: condition.EvaluationResult{
				ConditionMet:      false,
				MissingParameters: []string{"param1"},
			},
		},
		{
			name: "fail_no_such_attribute_empty_context",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "param1 == 'ok'",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"param1": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
					},
				},
			},
			context: map[string]interface{}{},
			result: condition.EvaluationResult{
				ConditionMet:      false,
				MissingParameters: []string{"param1"},
			},
		},
		{
			name: "fail_found_invalid_context_parameter",
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
			result: condition.EvaluationResult{
				ConditionMet:      false,
				MissingParameters: []string{"param1"},
			},
		},
		{
			name: "mix_of_missing_params_and_truthy_eval",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "param1 == 'ok' || param2 == 'ok'",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"param1": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
					},
					"param2": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
					},
				},
			},
			context: map[string]interface{}{
				"param1": "ok",
			},
			result: condition.EvaluationResult{
				ConditionMet:      true,
				MissingParameters: []string{"param2"},
			},
		},
		{
			name: "mix_of_missing_params_and_falsey_eval",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "param1 == 'ok' && param2 == 'ok'",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"param1": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
					},
					"param2": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
					},
				},
			},
			context: map[string]interface{}{
				"param1": "ok",
			},
			result: condition.EvaluationResult{
				ConditionMet:      false,
				MissingParameters: []string{"param2"},
			},
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
			err: &condition.EvaluationError{
				Condition: "condition1",
				Cause: &condition.ParameterTypeError{
					Condition: "condition1",
					Cause:     fmt.Errorf("failed to convert context parameter 'param1': expected type value 'string', but found 'bool'"),
				},
			},
		},
		{
			name: "ipaddress_literal",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: `ipaddress("192.168.0.1").in_cidr("192.168.0.0/24")`,
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{},
			},
			context: map[string]interface{}{},
			result:  condition.EvaluationResult{ConditionMet: true},
		},
		{
			name: "ipaddress_literal_malformed_addr",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: `ipaddress("192.168.0").in_cidr("192.168.0.0/24")`,
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{},
			},
			context: map[string]interface{}{},
			result:  condition.EvaluationResult{ConditionMet: false},
			err: &condition.EvaluationError{
				Condition: "condition1",
				Cause:     fmt.Errorf("failed to evaluate condition expression: ParseAddr(\"192.168.0\"): IPv4 address too short"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			compiledCondition, err := condition.NewCompiled(test.condition)
			require.NoError(t, err)

			contextStruct, err := structpb.NewStruct(test.context)
			require.NoError(t, err)

			result, err := compiledCondition.Evaluate(ctx, contextStruct.GetFields())

			require.Equal(t, test.result, result)
			if test.err != nil {
				var evalError *condition.EvaluationError
				require.ErrorAs(t, err, &evalError)
				require.EqualError(t, evalError, test.err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestEvaluateWithMaxCost(t *testing.T) {
	var tests = []struct {
		name      string
		condition *openfgav1.Condition
		context   map[string]any
		maxCost   uint64
		result    condition.EvaluationResult
		err       error
	}{
		{
			name: "cost_exceeded_int",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "x < y",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"x": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_INT,
					},
					"y": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_INT,
					},
				},
			},
			context: map[string]interface{}{
				"x": int64(1),
				"y": int64(2),
			},
			maxCost: 2,
			err:     fmt.Errorf("operation cancelled: actual cost limit exceeded"),
		},
		{
			name: "cost_not_exceeded_int",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "x < y",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"x": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_INT,
					},
					"y": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_INT,
					},
				},
			},
			context: map[string]interface{}{
				"x": int64(1),
				"y": int64(2),
			},
			maxCost: 3,
			result: condition.EvaluationResult{
				Cost:         3,
				ConditionMet: true,
			},
		},
		{
			name: "cost_exceeded_str",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "x == y",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"x": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
					},
					"y": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
					},
				},
			},
			context: map[string]interface{}{
				"x": "ab",
				"y": "ab",
			},
			maxCost: 2,
			err:     fmt.Errorf("operation cancelled: actual cost limit exceeded"),
		},
		{
			name: "cost_exceeded_list",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "'a' in strlist",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"strlist": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_LIST,
						GenericTypes: []*openfgav1.ConditionParamTypeRef{
							{
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
							},
						},
					},
				},
			},
			context: map[string]interface{}{
				"strlist": []interface{}{"c", "b", "a"},
			},
			maxCost: 3,
			err:     fmt.Errorf("operation cancelled: actual cost limit exceeded"),
		},
		{
			name: "cost_not_exceeded_list",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "'d' in strlist",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"strlist": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_LIST,
						GenericTypes: []*openfgav1.ConditionParamTypeRef{
							{
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
							},
						},
					},
				},
			},
			context: map[string]interface{}{
				"strlist": []interface{}{"a", "b", "c"},
			},
			maxCost: 4,
			result: condition.EvaluationResult{
				Cost:         4,
				ConditionMet: false,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			condition := condition.NewUncompiled(test.condition).WithMaxEvaluationCost(test.maxCost)

			err := condition.Compile()
			require.NoError(t, err)

			contextStruct, err := structpb.NewStruct(test.context)
			require.NoError(t, err)

			result, err := condition.Evaluate(ctx, contextStruct.GetFields())

			require.Equal(t, test.result, result)
			if test.err != nil {
				require.ErrorContains(t, err, test.err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCastContextToTypedParameters(t *testing.T) {
	tests := []struct {
		name                    string
		contextMap              map[string]any
		conditionParameterTypes map[string]*openfgav1.ConditionParamTypeRef
		expectedParams          map[string]any
		expectedError           *condition.ParameterTypeError
	}{
		{
			name: "valid",
			contextMap: map[string]any{
				"param1": "ok",
			},
			conditionParameterTypes: map[string]*openfgav1.ConditionParamTypeRef{
				"param1": {
					TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
				},
			},
			expectedParams: map[string]any{
				"param1": mustConvertValue(types.StringParamType, "ok"),
			},
		},
		{
			name:                    "empty_context_map",
			contextMap:              map[string]any{},
			conditionParameterTypes: map[string]*openfgav1.ConditionParamTypeRef{},
			expectedParams:          nil,
		},
		{
			name: "empty_parameter_types",
			contextMap: map[string]any{
				"param1": "ok",
			},
			conditionParameterTypes: map[string]*openfgav1.ConditionParamTypeRef{},
			expectedParams:          nil,
			expectedError: &condition.ParameterTypeError{
				Condition: "condition1",
				Cause:     fmt.Errorf("no parameters defined for the condition"),
			},
		},
		{
			name: "failed_to_decode_condition_parameter_type",
			contextMap: map[string]any{
				"param1": "ok",
			},
			conditionParameterTypes: map[string]*openfgav1.ConditionParamTypeRef{
				"param1": {
					TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_UNSPECIFIED,
				},
			},
			expectedParams: nil,
			expectedError: &condition.ParameterTypeError{
				Condition: "condition1",
				Cause:     fmt.Errorf("failed to decode condition parameter type 'TYPE_NAME_UNSPECIFIED': unknown condition parameter type `TYPE_NAME_UNSPECIFIED`"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := condition.NewUncompiled(&openfgav1.Condition{
				Name:       "condition1",
				Expression: "param1 == 'ok'",
				Parameters: test.conditionParameterTypes,
			})

			contextStruct, err := structpb.NewStruct(test.contextMap)
			require.NoError(t, err)

			typedParams, err := c.CastContextToTypedParameters(contextStruct.GetFields())

			if test.expectedError != nil {
				require.Error(t, err)
				require.EqualError(t, err, test.expectedError.Error())
			} else {
				require.NoError(t, err)
			}

			if !reflect.DeepEqual(typedParams, test.expectedParams) {
				t.Errorf("expected %v, got %v", test.expectedParams, typedParams)
			}
		})
	}
}

func TestEvaluateWithInterruptCheckFrequency(t *testing.T) {
	makeItems := func(size int) []interface{} {
		items := make([]interface{}, size)
		for i := int(0); i < size; i++ {
			items[i] = i
		}
		return items
	}

	// numLoops is the number of loops being evaluated by a CEL
	// expression. This number needs to be large enough to not
	// be resolved before the 1 microsecond context timeout.
	numLoops := 500

	var tests = []struct {
		name           string
		condition      *openfgav1.Condition
		context        map[string]any
		checkFrequency uint
		result         condition.EvaluationResult
		err            error
	}{
		{
			name: "operation_interrupted_one_comprehension",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "items.map(i, i * 2).size() > 0",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"items": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_LIST,
						GenericTypes: []*openfgav1.ConditionParamTypeRef{
							{
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_INT,
							},
						},
					},
				},
			},
			checkFrequency: uint(numLoops),
			context: map[string]interface{}{
				"items": makeItems(numLoops),
			},
			result: condition.EvaluationResult{
				ConditionMet: false,
			},
			err: fmt.Errorf("failed to evaluate relationship condition: 'condition1' - failed to evaluate condition expression: operation interrupted"),
		},
		{
			name: "operation_not_interrupted_one_comprehension",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "items.map(i, i * 2).size() > 0",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"items": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_LIST,
						GenericTypes: []*openfgav1.ConditionParamTypeRef{
							{
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_INT,
							},
						},
					},
				},
			},
			checkFrequency: uint(numLoops),
			context: map[string]interface{}{
				"items": makeItems(numLoops - 1),
			},
			result: condition.EvaluationResult{
				ConditionMet: true,
			},
			err: nil,
		},
		{
			name: "operation_interrupted_two_comprehensions",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "items.map(i, i * 2).map(i, i * i).size() > 0",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"items": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_LIST,
						GenericTypes: []*openfgav1.ConditionParamTypeRef{
							{
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_INT,
							},
						},
					},
				},
			},
			checkFrequency: uint(numLoops),
			context: map[string]interface{}{
				"items": makeItems(numLoops),
			},
			result: condition.EvaluationResult{
				ConditionMet: false,
			},
			err: fmt.Errorf("failed to evaluate relationship condition: 'condition1' - failed to evaluate condition expression: operation interrupted"),
		},
		{
			name: "operation_not_interrupted_two_comprehensions",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "items.map(i, i * 2).map(i, i * i).size() > 0",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"items": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_LIST,
						GenericTypes: []*openfgav1.ConditionParamTypeRef{
							{
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_INT,
							},
						},
					},
				},
			},
			checkFrequency: uint(numLoops),
			context: map[string]interface{}{
				"items": makeItems(numLoops - 1),
			},
			result: condition.EvaluationResult{
				ConditionMet: false,
			},
			err: fmt.Errorf("failed to evaluate relationship condition: 'condition1' - failed to evaluate condition expression: operation interrupted"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			condition := condition.NewUncompiled(test.condition).
				WithInterruptCheckFrequency(test.checkFrequency)

			err := condition.Compile()
			require.NoError(t, err)

			contextStruct, err := structpb.NewStruct(test.context)
			require.NoError(t, err)

			evalCtx, cancel := context.WithTimeout(ctx, time.Microsecond)
			defer cancel()

			result, err := condition.Evaluate(evalCtx, contextStruct.GetFields())

			require.Equal(t, test.result, result)
			if test.err != nil {
				require.ErrorContains(t, err, test.err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func mustConvertValue(varType types.ParameterType, value any) any {
	convertedParam, err := varType.ConvertValue(value)
	if err != nil {
		panic(err)
	}

	return convertedParam
}
