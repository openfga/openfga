package types

import (
	"testing"

	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

func TestDecodeParameterType(t *testing.T) {
	tests := []struct {
		name          string
		input         *openfgav1.ConditionParamTypeRef
		expectedError string
	}{
		{
			name: "unknown_type",
			input: &openfgav1.ConditionParamTypeRef{
				TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_UNSPECIFIED,
			},
			expectedError: "unknown condition parameter type `TYPE_NAME_UNSPECIFIED`",
		},
		{
			name: "valid_string_type",
			input: &openfgav1.ConditionParamTypeRef{
				TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
			},
		},
		{
			name: "valid_map_with_generic",
			input: &openfgav1.ConditionParamTypeRef{
				TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_MAP,
				GenericTypes: []*openfgav1.ConditionParamTypeRef{
					{TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING},
				},
			},
		},
		{
			name: "map_missing_generic_type",
			input: &openfgav1.ConditionParamTypeRef{
				TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_MAP,
			},
			expectedError: "condition parameter type `TYPE_NAME_MAP` requires 1 generic types; found 0",
		},
		{
			name: "map_too_many_generic_types",
			input: &openfgav1.ConditionParamTypeRef{
				TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_MAP,
				GenericTypes: []*openfgav1.ConditionParamTypeRef{
					{TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING},
					{TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_INT},
				},
			},
			expectedError: "condition parameter type `TYPE_NAME_MAP` requires 1 generic types; found 2",
		},
		{
			name: "list_missing_generic_type",
			input: &openfgav1.ConditionParamTypeRef{
				TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_LIST,
			},
			expectedError: "condition parameter type `TYPE_NAME_LIST` requires 1 generic types; found 0",
		},
		{
			name: "string_with_unexpected_generic",
			input: &openfgav1.ConditionParamTypeRef{
				TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
				GenericTypes: []*openfgav1.ConditionParamTypeRef{
					{TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_INT},
				},
			},
			expectedError: "condition parameter type `TYPE_NAME_STRING` requires 0 generic types; found 1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := DecodeParameterType(test.input)

			if test.expectedError != "" {
				require.Nil(t, result)
				require.EqualError(t, err, test.expectedError)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
			}
		})
	}
}
