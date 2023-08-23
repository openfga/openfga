package condition_test

import (
	"fmt"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/condition"
	"github.com/stretchr/testify/require"
)

func TestCompile(t *testing.T) {
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
						TypeName: "string",
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
						TypeName: "invalid",
					},
				},
			},
			err: fmt.Errorf("failed to decode parameter type for parameter 'param1': unknown condition parameter type `invalid`"),
		},
		{
			name: "invalid_expression",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "invalid",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"param1": {
						TypeName: "string",
					},
				},
			},
			err: fmt.Errorf("failed to compile condition expression: ERROR: <input>:1:1: undeclared reference to 'invalid' (in container '')\n | invalid\n | ^"),
		},
		{
			name: "invalid_output_type",
			condition: &openfgav1.Condition{
				Name:       "condition1",
				Expression: "param1",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"param1": {
						TypeName: "string",
					},
				},
			},
			err: fmt.Errorf("expected a bool condition expression output, but got 'string'"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := condition.Compile(test.condition)

			if test.err != nil {
				require.Equal(t, err, test.err)
			} else {
				require.NoError(t, err)
			}
		})
	}

}
