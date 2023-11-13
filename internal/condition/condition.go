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

var baseCelEnv *cel.Env

func init() {
	var envOpts []cel.EnvOption
	for _, customTypeOpts := range types.CustomParamTypes {
		envOpts = append(envOpts, customTypeOpts...)
	}

	envOpts = append(envOpts, types.IPAddressEnvOption())
	envOpts = append(envOpts, cel.EagerlyValidateDeclarations(true), cel.DefaultUTCTimeZone(true))

	env, err := cel.NewEnv(envOpts...)
	if err != nil {
		panic(fmt.Sprintf("failed to construct base CEL env: %v", err))
	}

	baseCelEnv = env
}

var emptyEvaluationResult = EvaluationResult{}

type EvaluationResult struct {
	Cost              uint64
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

	celProgramOpts []cel.ProgramOption
	celEnv         *cel.Env
	celProgram     cel.Program
	compileOnce    sync.Once
}

// Compile compiles a condition expression with a CEL environment
// constructed from the condition's parameter type definitions into a valid
// AST that can be evaluated at a later time.
func (e *EvaluableCondition) Compile() error {
	var compileErr error

	e.compileOnce.Do(func() {
		if err := e.compile(); err != nil {
			compileErr = err
			return
		}
	})

	return compileErr
}

func (e *EvaluableCondition) compile() error {
	var envOpts []cel.EnvOption
	conditionParamTypes := map[string]*types.ParameterType{}
	for paramName, paramTypeRef := range e.GetParameters() {
		paramType, err := types.DecodeParameterType(paramTypeRef)
		if err != nil {
			return &CompilationError{
				Condition: e.Name,
				Cause:     fmt.Errorf("failed to decode parameter type for parameter '%s': %v", paramName, err),
			}
		}

		conditionParamTypes[paramName] = paramType
	}

	for paramName, paramType := range conditionParamTypes {
		envOpts = append(envOpts, cel.Variable(paramName, paramType.CelType()))
	}

	env, err := baseCelEnv.Extend(envOpts...)
	if err != nil {
		return &CompilationError{
			Condition: e.Name,
			Cause:     err,
		}
	}

	source := common.NewStringSource(e.Expression, e.Name)
	ast, issues := env.CompileSource(source)
	if issues != nil {
		if err := issues.Err(); err != nil {
			return &CompilationError{
				Condition: e.Name,
				Cause:     err,
			}
		}
	}

	e.celProgramOpts = append(e.celProgramOpts, cel.EvalOptions(cel.OptPartialEval))

	prg, err := env.Program(ast, e.celProgramOpts...)
	if err != nil {
		return &CompilationError{
			Condition: e.Name,
			Cause:     fmt.Errorf("condition expression construction: %w", err),
		}
	}

	if !reflect.DeepEqual(ast.OutputType(), cel.BoolType) {
		return &CompilationError{
			Condition: e.Name,
			Cause:     fmt.Errorf("expected a bool condition expression output, but got '%s'", ast.OutputType()),
		}
	}

	e.celEnv = env
	e.celProgram = prg
	return nil
}

// CastContextToTypedParameters converts the provided context to typed condition
// parameters and returns an error if any additional context fields are provided
// that are not defined by the evaluable condition.
func (e *EvaluableCondition) CastContextToTypedParameters(contextMap map[string]any) (map[string]any, error) {
	if len(contextMap) == 0 {
		return nil, nil
	}

	parameterTypes := e.GetParameters()

	if len(parameterTypes) == 0 {
		return nil, &ParameterTypeError{
			Condition: e.Name,
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
				Condition: e.Name,
				Cause:     fmt.Errorf("failed to decode condition parameter type '%s': %v", paramTypeRef.TypeName, err),
			}
		}

		convertedParam, err := varType.ConvertValue(value)
		if err != nil {
			return nil, &ParameterTypeError{
				Condition: e.Name,
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
func (e *EvaluableCondition) Evaluate(contextMaps ...map[string]any) (EvaluationResult, error) {
	if err := e.Compile(); err != nil {
		return emptyEvaluationResult, errors.With(&EvaluationError{
			Condition: e.Name,
			Cause:     err,
		}, ErrEvaluationFailed)
	}

	// merge context maps
	clonedMap := maps.Clone(contextMaps[0])

	for _, contextMap := range contextMaps[1:] {
		maps.Copy(clonedMap, contextMap)
	}

	typedParams, err := e.CastContextToTypedParameters(clonedMap)
	if err != nil {
		return emptyEvaluationResult, errors.With(&EvaluationError{
			Condition: e.Name,
			Cause:     err,
		}, ErrEvaluationFailed)
	}

	activation, err := e.celEnv.PartialVars(typedParams)
	if err != nil {
		return emptyEvaluationResult, errors.With(&EvaluationError{
			Condition: e.Name,
			Cause:     fmt.Errorf("failed to construct condition partial vars: %v", err),
		}, ErrEvaluationFailed)
	}

	var missingParameters []string
	for key := range e.GetParameters() {
		if _, ok := activation.ResolveName(key); ok {
			continue
		}

		missingParameters = append(missingParameters, key)
	}

	out, details, err := e.celProgram.Eval(activation)
	if err != nil {
		return emptyEvaluationResult, NewEvaluationError(
			e.Name,
			fmt.Errorf("failed to evaluate condition expression: %v", err),
		)
	}

	var evaluationCost uint64
	if details != nil {
		cost := details.ActualCost()
		if cost != nil {
			evaluationCost = *cost
		}
	}

	if celtypes.IsUnknown(out) {
		return EvaluationResult{
			ConditionMet:      false,
			MissingParameters: missingParameters,
			Cost:              evaluationCost,
		}, nil
	}

	conditionMetVal, err := out.ConvertToNative(reflect.TypeOf(false))
	if err != nil {
		return emptyEvaluationResult, NewEvaluationError(
			e.Name,
			fmt.Errorf("failed to convert condition output to bool: %v", err),
		)
	}

	conditionMet, ok := conditionMetVal.(bool)
	if !ok {
		return emptyEvaluationResult, NewEvaluationError(
			e.Name,
			fmt.Errorf("expected CEL type conversion to return native Go bool"),
		)
	}

	return EvaluationResult{
		ConditionMet:      conditionMet,
		MissingParameters: missingParameters,
		Cost:              evaluationCost,
	}, nil
}

// WithTrackEvaluationCost enables CEL evaluation cost on the EvaluableCondition and returns the
// mutated EvaluableCondition. The expectation is that this is called on the Uncompiled condition
// because it modifies the behavior of the CEL program that is constructed after Compile.
func (e *EvaluableCondition) WithTrackEvaluationCost() *EvaluableCondition {
	e.celProgramOpts = append(e.celProgramOpts, cel.EvalOptions(cel.OptOptimize, cel.OptTrackCost))

	return e
}

// WithMaxEvaluationCost enables CEL evaluation cost enforcement on the EvaluableCondition and
// returns the mutated EvaluableCondition. The expectation is that this is called on the Uncompiled
// condition because it modifies the behavior of the CEL program that is constructed after Compile.
func (e *EvaluableCondition) WithMaxEvaluationCost(cost uint64) *EvaluableCondition {
	e.celProgramOpts = append(e.celProgramOpts, cel.CostLimit(cost))

	return e
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
