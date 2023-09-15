package condition

import (
	"fmt"
	"reflect"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/condition/types"
	"github.com/stretchr/testify/require"
)

func mustConvertValue(varType types.ParameterType, value any) any {
	convertedParam, err := varType.ConvertValue(value)
	if err != nil {
		panic(err)
	}

	return convertedParam
}
func TestCastContextToTypedParameters(t *testing.T) {
	tests := []struct {
		name                    string
		contextMap              map[string]any
		conditionParameterTypes map[string]*openfgav1.ConditionParamTypeRef
		expectedParams          map[string]any
		expectedError           error
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
			expectedError: nil,
		},
		{
			name:                    "empty_context_map",
			contextMap:              map[string]any{},
			conditionParameterTypes: map[string]*openfgav1.ConditionParamTypeRef{},
			expectedParams:          nil,
			expectedError:           nil,
		},
		{
			name: "empty_parameter_types",
			contextMap: map[string]any{
				"param1": "ok",
			},
			conditionParameterTypes: map[string]*openfgav1.ConditionParamTypeRef{},
			expectedParams:          nil,
			expectedError:           fmt.Errorf("no parameters defined for the condition"),
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
			expectedParams: map[string]any{
				"param1": mustConvertValue(types.StringParamType, "ok"),
			},
			expectedError: fmt.Errorf("failed to decode condition parameter type 'TYPE_NAME_UNSPECIFIED': unknown condition parameter type `TYPE_NAME_UNSPECIFIED`"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			typedParams, err := castContextToTypedParameters(test.contextMap, test.conditionParameterTypes)

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
