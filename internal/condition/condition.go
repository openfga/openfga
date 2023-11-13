package condition

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common"
	celtypes "github.com/google/cel-go/common/types"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/condition/types"
	"github.com/openfga/openfga/internal/errors"
	"golang.org/x/exp/maps"
)

var celBaseEnv *cel.Env

func init() {
	var envOpts []cel.EnvOption
	for _, customTypeOpts := range types.CustomParamTypes {
		envOpts = append(envOpts, customTypeOpts...)
	}

	envOpts = append(envOpts, types.IPAddressEnvOption(), cel.EagerlyValidateDeclarations(true))

	env, err := cel.NewEnv(envOpts...)
	if err != nil {
		panic(fmt.Sprintf("failed to construct CEL base env: %v", err))
	}

	celBaseEnv = env
}

var emptyEvaluationResult = EvaluationResult{}

type EvaluationResult struct {
	ConditionMet      bool
	MissingParameters []string
}

// EvaluableCondition represents a condition that can eventually be evaluated
// given a CEL expression and a set of parameters. Calling .Evaluate() will
// optionally call .Compile() which validates and compiles the expression and
// parameter type definitions if it hasn't been done already.
// Note: at the moment, this is not safe for concurrent use.
type EvaluableCondition struct {
	*openfgav1.Condition

	celEnv      *cel.Env
	celProgram  cel.Program
	compileOnce sync.Once
}

// Compile compiles a condition expression with a CEL environment
// constructed from the condition's parameter type definitions into a valid
// AST that can be evaluated at a later time.
func (c *EvaluableCondition) Compile() error {
	var compileErr error

	c.compileOnce.Do(func() {
		if err := c.compile(); err != nil {
			compileErr = err
			return
		}
	})

	return compileErr
}

func (c *EvaluableCondition) compile() error {
	var envOpts []cel.EnvOption
	conditionParamTypes := map[string]*types.ParameterType{}
	for paramName, paramTypeRef := range c.GetParameters() {
		paramType, err := types.DecodeParameterType(paramTypeRef)
		if err != nil {
			return &CompilationError{
				Condition: c.Name,
				Cause:     fmt.Errorf("failed to decode parameter type for parameter '%s': %v", paramName, err),
			}
		}

		conditionParamTypes[paramName] = paramType
	}

	for paramName, paramType := range conditionParamTypes {
		envOpts = append(envOpts, cel.Variable(paramName, paramType.CelType()))
	}

	env, err := celBaseEnv.Extend(envOpts...)
	if err != nil {
		return &CompilationError{
			Condition: c.Name,
			Cause:     err,
		}
	}

	source := common.NewStringSource(c.Expression, c.Name)
	ast, issues := env.CompileSource(source)
	if issues != nil {
		if err := issues.Err(); err != nil {
			return &CompilationError{
				Condition: c.Name,
				Cause:     err,
			}
		}
	}

	prgopts := []cel.ProgramOption{
		cel.EvalOptions(cel.OptPartialEval),
	}

	prg, err := env.Program(ast, prgopts...)
	if err != nil {
		return &CompilationError{
			Condition: c.Name,
			Cause:     fmt.Errorf("condition expression construction: %w", err),
		}
	}

	if !reflect.DeepEqual(ast.OutputType(), cel.BoolType) {
		return &CompilationError{
			Condition: c.Name,
			Cause:     fmt.Errorf("expected a bool condition expression output, but got '%s'", ast.OutputType()),
		}
	}

	c.celEnv = env
	c.celProgram = prg
	return nil
}

// CastContextToTypedParameters converts the provided context to typed condition
// parameters and returns an error if any additional context fields are provided
// that are not defined by the evaluable condition.
func (c *EvaluableCondition) CastContextToTypedParameters(contextMap map[string]any) (map[string]any, error) {
	if len(contextMap) == 0 {
		return nil, nil
	}

	parameterTypes := c.GetParameters()

	if len(parameterTypes) == 0 {
		return nil, &ParameterTypeError{
			Condition: c.Name,
			Cause:     fmt.Errorf("no parameters defined for the condition"),
		}
	}

	converted := make(map[string]any, len(contextMap))

	for key, value := range contextMap {
		paramTypeRef, ok := parameterTypes[key]
		if !ok {
			continue
		}

		varType, err := types.DecodeParameterType(paramTypeRef)
		if err != nil {
			return nil, &ParameterTypeError{
				Condition: c.Name,
				Cause:     fmt.Errorf("failed to decode condition parameter type '%s': %v", paramTypeRef.TypeName, err),
			}
		}

		convertedParam, err := varType.ConvertValue(value)
		if err != nil {
			return nil, &ParameterTypeError{
				Condition: c.Name,
				Cause:     fmt.Errorf("failed to convert context parameter '%s': %w", key, err),
			}
		}

		converted[key] = convertedParam
	}

	return converted, nil
}

// Evaluate evalutes the provided CEL condition expression with a CEL environment
// constructed from the condition's parameter type definitions and using the
// context provided. If more than one source of context is provided, and if the
// keys provided in those context(s) are overlapping, then the overlapping key
// for the last most context wins.
func (c *EvaluableCondition) Evaluate(contextMaps ...map[string]any) (EvaluationResult, error) {
	if err := c.Compile(); err != nil {
		return emptyEvaluationResult, errors.With(&EvaluationError{
			Condition: c.Name,
			Cause:     err,
		}, ErrEvaluationFailed)
	}

	// merge context maps
	clonedMap := maps.Clone(contextMaps[0])

	for _, contextMap := range contextMaps[1:] {
		maps.Copy(clonedMap, contextMap)
	}

	typedParams, err := c.CastContextToTypedParameters(clonedMap)
	if err != nil {
		return emptyEvaluationResult, errors.With(&EvaluationError{
			Condition: c.Name,
			Cause:     err,
		}, ErrEvaluationFailed)
	}

	activation, err := c.celEnv.PartialVars(typedParams)
	if err != nil {
		return emptyEvaluationResult, errors.With(&EvaluationError{
			Condition: c.Name,
			Cause:     fmt.Errorf("failed to construct condition partial vars: %v", err),
		}, ErrEvaluationFailed)
	}

	var missingParameters []string
	for key := range c.GetParameters() {
		if _, ok := activation.ResolveName(key); ok {
			continue
		}

		missingParameters = append(missingParameters, key)
	}

	out, _, err := c.celProgram.Eval(activation)
	if err != nil {
		return emptyEvaluationResult, NewEvaluationError(
			c.Name,
			fmt.Errorf("failed to evaluate condition expression: %v", err),
		)
	}

	if celtypes.IsUnknown(out) {
		return EvaluationResult{
			ConditionMet:      false,
			MissingParameters: missingParameters,
		}, nil
	}

	conditionMetVal, err := out.ConvertToNative(reflect.TypeOf(false))
	if err != nil {
		return emptyEvaluationResult, NewEvaluationError(
			c.Name,
			fmt.Errorf("failed to convert condition output to bool: %v", err),
		)
	}

	conditionMet, ok := conditionMetVal.(bool)
	if !ok {
		return emptyEvaluationResult, NewEvaluationError(
			c.Name,
			fmt.Errorf("expected CEL type conversion to return native Go bool"),
		)
	}

	return EvaluationResult{
		ConditionMet:      conditionMet,
		MissingParameters: missingParameters,
	}, nil
}

// NewUncompiled returns a new EvaluableCondition that has not
// validated and compiled its expression.
func NewUncompiled(condition *openfgav1.Condition) *EvaluableCondition {
	return &EvaluableCondition{Condition: condition}
}

// NewCompiled returns a new EvaluableCondition with a validated and
// compiled expression.
func NewCompiled(condition *openfgav1.Condition) (*EvaluableCondition, error) {
	compiled := NewUncompiled(condition)

	if err := compiled.Compile(); err != nil {
		return nil, err
	}

	return compiled, nil
}
