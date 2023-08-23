package condition

import (
	"fmt"
	"reflect"

	"github.com/google/cel-go/cel"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/condition/types"
)

type CompiledCondition struct {
	*openfgav1.Condition

	Program cel.Program
}

// Compile compiles a condition expression with a CEL environment
// constructed from the condition's parameter type defintions into a valid
// AST that can be evaluated at a later time.
func Compile(condition *openfgav1.Condition) (*CompiledCondition, error) {
	var envOpts []cel.EnvOption
	for _, customTypeOpts := range types.CustomParamTypes {
		envOpts = append(envOpts, customTypeOpts...)
	}

	conditionParamTypes := map[string]*types.ParameterType{}
	for paramName, paramTypeRef := range condition.GetParameters() {
		paramType, err := types.DecodeParameterType(paramTypeRef)
		if err != nil {
			return nil, fmt.Errorf("failed to decode parameter type for parameter '%s': %v", paramName, err)
		}

		conditionParamTypes[paramName] = paramType
	}

	for paramName, paramType := range conditionParamTypes {
		envOpts = append(envOpts, cel.Variable(paramName, paramType.CelType()))
	}

	env, err := cel.NewEnv(envOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to construct CEL env: %v", err)
	}

	ast, issues := env.Compile(condition.Expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("failed to compile condition expression: %v", issues.Err())
	}

	prg, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("condition expression construction error: %s", err)
	}

	if !reflect.DeepEqual(ast.OutputType(), cel.BoolType) {
		return nil, fmt.Errorf("expected a bool condition expression output, but got '%s'", ast.OutputType())
	}

	return &CompiledCondition{Condition: condition, Program: prg}, nil
}
