package condition_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/condition"
)

func TestIsInlineExpression(t *testing.T) {
	require.True(t, condition.IsInlineExpression("$expression"))
	require.False(t, condition.IsInlineExpression(""))
	require.False(t, condition.IsInlineExpression("expired"))
	require.False(t, condition.IsInlineExpression("$other"))
}

// TestFromInlineExpression covers the parse/validation/compile phase of FromInlineExpression.
// FromInlineExpression pre-compiles the CEL program, so both structural parse errors and
// CEL type-check errors are returned directly from the function.
func TestFromInlineExpression(t *testing.T) {
	tests := []struct {
		name      string
		ctx       map[string]interface{}
		expectErr string // error from FromInlineExpression (parse/structural/compile)
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
			name: "param_named_expression_is_allowed",
			ctx: map[string]interface{}{
				"expression": "expression == 'foo'",
				"parameters": map[string]interface{}{
					"expression": "string",
				},
			},
		},
		{
			name: "param_named_parameters_is_allowed",
			ctx: map[string]interface{}{
				"expression": "parameters == 'foo'",
				"parameters": map[string]interface{}{
					"parameters": "string",
				},
			},
		},
		// Type mismatches are caught by CEL type-checking inside Compile(), which is
		// called inside FromInlineExpression.
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
		// Non-string scalar types
		{
			name: "valid_int_param",
			ctx: map[string]interface{}{
				"expression": "count > 5",
				"parameters": map[string]interface{}{
					"count": "int",
				},
			},
		},
		{
			name: "valid_uint_param",
			ctx: map[string]interface{}{
				"expression": "size > uint(0)",
				"parameters": map[string]interface{}{
					"size": "uint",
				},
			},
		},
		{
			name: "valid_double_param",
			ctx: map[string]interface{}{
				"expression": "ratio > 0.5",
				"parameters": map[string]interface{}{
					"ratio": "double",
				},
			},
		},
		{
			name: "valid_bool_param",
			ctx: map[string]interface{}{
				"expression": "flag == true",
				"parameters": map[string]interface{}{
					"flag": "bool",
				},
			},
		},
		{
			name: "valid_timestamp_param",
			ctx: map[string]interface{}{
				"expression": `ts < timestamp("2023-10-11T10:00:00.000Z")`,
				"parameters": map[string]interface{}{
					"ts": "timestamp",
				},
			},
		},
		{
			name: "valid_duration_param",
			ctx: map[string]interface{}{
				"expression": `elapsed > duration("1h")`,
				"parameters": map[string]interface{}{
					"elapsed": "duration",
				},
			},
		},
		// OpenFGA custom type
		{
			name: "valid_ipaddress_param",
			ctx: map[string]interface{}{
				"expression": `ip.in_cidr("10.0.0.0/8")`,
				"parameters": map[string]interface{}{
					"ip": "ipaddress",
				},
			},
		},
		{
			name: "type_mismatch_int_param_used_as_string",
			ctx: map[string]interface{}{
				"expression": "count == 'notanint'",
				"parameters": map[string]interface{}{
					"count": "int",
				},
			},
			expectErr: "found no matching overload",
		},
		{
			name: "type_mismatch_bool_param_used_as_int",
			ctx: map[string]interface{}{
				"expression": "flag > 0",
				"parameters": map[string]interface{}{
					"flag": "bool",
				},
			},
			expectErr: "found no matching overload",
		},
		{
			name: "type_mismatch_ipaddress_compared_as_string",
			ctx: map[string]interface{}{
				// ipaddress is not a string; direct string equality should fail type-checking
				"expression": `ip == "10.0.0.1"`,
				"parameters": map[string]interface{}{
					"ip": "ipaddress",
				},
			},
			expectErr: "found no matching overload",
		},
		// Container types with generic element
		{
			name: "valid_map_of_string_param",
			ctx: map[string]interface{}{
				"expression": `"admin" in roles`,
				"parameters": map[string]interface{}{
					"roles": "map<string>",
				},
			},
		},
		{
			name: "valid_list_of_string_param",
			ctx: map[string]interface{}{
				"expression": `"admin" in tags`,
				"parameters": map[string]interface{}{
					"tags": "list<string>",
				},
			},
		},
		{
			name: "map_param_missing_generic",
			ctx: map[string]interface{}{
				"expression": `m.size() > 0`,
				"parameters": map[string]interface{}{
					"m": "map",
				},
			},
			expectErr: `requires a generic element type`,
		},
		{
			name: "list_param_missing_generic",
			ctx: map[string]interface{}{
				"expression": `l.size() > 0`,
				"parameters": map[string]interface{}{
					"l": "list",
				},
			},
			expectErr: `requires a generic element type`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := structpb.NewStruct(tt.ctx)
			require.NoError(t, err)

			tk := &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:alice",
				Condition: &openfgav1.RelationshipCondition{
					Name:    condition.InlineExpressionName,
					Context: s,
				},
			}

			ec, err := condition.FromInlineExpression(context.Background(), tk)
			if tt.expectErr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.expectErr)
				require.Nil(t, ec)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, ec)
		})
	}
}
