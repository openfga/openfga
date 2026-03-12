package types

import (
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
)

func TestDecodeParameterType(t *testing.T) {
	tests := []struct {
		name          string
		input         *openfgav1.ConditionParamTypeRef
		expectedType  string
		expectedError string
	}{
		{
			name: "unknown_type_name",
			input: &openfgav1.ConditionParamTypeRef{
				TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_UNSPECIFIED,
			},
			expectedError: "unknown condition parameter type `TYPE_NAME_UNSPECIFIED`",
		},
		{
			name: "generic_count_mismatch",
			input: &openfgav1.ConditionParamTypeRef{
				TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_MAP,
				GenericTypes: []*openfgav1.ConditionParamTypeRef{
					{TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING},
					{TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_INT},
				},
			},
			expectedError: "condition parameter type `TYPE_NAME_MAP`",
		},
		{
			name: "generic_type_missing",
			input: &openfgav1.ConditionParamTypeRef{
				TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_LIST,
			},
			expectedError: "condition parameter type `TYPE_NAME_LIST`",
		},
		{
			name: "recursive_generic_type",
			input: &openfgav1.ConditionParamTypeRef{
				TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_LIST,
				GenericTypes: []*openfgav1.ConditionParamTypeRef{
					{
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_MAP,
						GenericTypes: []*openfgav1.ConditionParamTypeRef{
							{TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING},
						},
					},
				},
			},
			expectedType: "TYPE_NAME_LIST<TYPE_NAME_MAP<string>>",
		},
		{
			name: "valid_primitive_type",
			input: &openfgav1.ConditionParamTypeRef{
				TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
			},
			expectedType: "string",
		},
		{
			name: "valid_map_type",
			input: &openfgav1.ConditionParamTypeRef{
				TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_MAP,
				GenericTypes: []*openfgav1.ConditionParamTypeRef{
					{TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_INT},
				},
			},
			expectedType: "TYPE_NAME_MAP<int>",
		},
		{
			name: "valid_list_type",
			input: &openfgav1.ConditionParamTypeRef{
				TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_LIST,
				GenericTypes: []*openfgav1.ConditionParamTypeRef{
					{TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_BOOL},
				},
			},
			expectedType: "TYPE_NAME_LIST<bool>",
		},
		{
			name: "recursive_invalid_generic_type",
			input: &openfgav1.ConditionParamTypeRef{
				TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_LIST,
				GenericTypes: []*openfgav1.ConditionParamTypeRef{
					{
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_UNSPECIFIED,
					},
				},
			},
			expectedError: "unknown condition parameter type `TYPE_NAME_UNSPECIFIED`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := DecodeParameterType(tt.input)

			if tt.expectedError != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.expectedError)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Equal(t, tt.expectedType, result.String())
			}
		})
	}
}
