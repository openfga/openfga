package condition_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/openfga/openfga/internal/condition"
)

func TestIsInlineExpression(t *testing.T) {
	require.True(t, condition.IsInlineExpression("$expression"))
	require.False(t, condition.IsInlineExpression(""))
	require.False(t, condition.IsInlineExpression("expired"))
	require.False(t, condition.IsInlineExpression("$other"))
}

func TestNewCompiledFromInlineExpression(t *testing.T) {
	tests := []struct {
		name      string
		ctx       map[string]interface{}
		expectErr string
	}{
		{
			name: "valid_with_declared_params",
			ctx: map[string]interface{}{
				"expression": "channel_id == 'X123456'",
				"parameters": map[string]interface{}{
					"channel_id": "string",
				},
			},
		},
		{
			name: "valid_with_inferred_string_param",
			ctx: map[string]interface{}{
				"expression": "channel_id == 'X123456'",
				// no "parameters" key — channel_id inferred as string
			},
		},
		{
			name: "valid_no_params_constant_expression",
			ctx: map[string]interface{}{
				"expression": "true",
			},
		},
		{
			name: "valid_multiple_types",
			ctx: map[string]interface{}{
				"expression": "count > 5 && flag == true",
				"parameters": map[string]interface{}{
					"count": "int",
					"flag":  "bool",
				},
			},
		},
		{
			name: "valid_comprehension_local_var_not_inferred",
			// items is the outer var (should be inferred as string); x is comprehension-local
			ctx: map[string]interface{}{
				"expression": "[1, 2, 3].exists(x, x > 0)",
			},
		},
		{
			name:      "missing_expression_field",
			ctx:       map[string]interface{}{},
			expectErr: `missing required context field "expression"`,
		},
		{
			name: "empty_expression",
			ctx: map[string]interface{}{
				"expression": "",
			},
			expectErr: `must be a non-empty string`,
		},
		{
			name: "invalid_cel_syntax",
			ctx: map[string]interface{}{
				"expression": "channel_id ==",
			},
			expectErr: "invalid CEL expression",
		},
		{
			name: "unknown_parameter_type",
			ctx: map[string]interface{}{
				"expression": "x == 1",
				"parameters": map[string]interface{}{
					"x": "bigdecimal",
				},
			},
			expectErr: `unknown parameter type "bigdecimal"`,
		},
		{
			name: "reserved_param_name_expression",
			ctx: map[string]interface{}{
				"expression": "expression == 'foo'",
				"parameters": map[string]interface{}{
					"expression": "string",
				},
			},
			expectErr: `parameter name "expression" is reserved`,
		},
		{
			name: "reserved_param_name_parameters",
			ctx: map[string]interface{}{
				"expression": "parameters == 'foo'",
				"parameters": map[string]interface{}{
					"parameters": "string",
				},
			},
			expectErr: `parameter name "parameters" is reserved`,
		},
		{
			name: "type_mismatch_fails_compilation",
			ctx: map[string]interface{}{
				"expression": "count == 'notanint'",
				"parameters": map[string]interface{}{
					"count": "int",
				},
			},
			expectErr: "found no matching overload",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := structpb.NewStruct(tt.ctx)
			require.NoError(t, err)

			cond, err := condition.NewCompiledFromInlineExpression(s)
			if tt.expectErr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.expectErr)
				require.Nil(t, cond)
			} else {
				require.NoError(t, err)
				require.NotNil(t, cond)
			}
		})
	}
}
