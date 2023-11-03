package condition

import (
	"fmt"
)

type CompilationError struct {
	Condition string
	Cause     error
}

func (e *CompilationError) Error() string {
	return fmt.Sprintf("failed to compile expression on condition '%s': %v", e.Condition, e.Cause)
}

func (e *CompilationError) Unwrap() error {
	return e.Cause
}

type EvaluationError struct {
	Condition string
	Cause     error
}

func (e *EvaluationError) Error() string {
	if _, ok := e.Cause.(*ParameterTypeError); ok {
		return e.Unwrap().Error()
	}

	return fmt.Sprintf("failed to evaluate relationship condition '%s': %v", e.Condition, e.Cause)
}

func (e *EvaluationError) Unwrap() error {
	return e.Cause
}

type ParameterTypeError struct {
	Condition string
	Cause     error
}

func (e *ParameterTypeError) Error() string {
	return fmt.Sprintf("parameter type error on condition '%s': %v", e.Condition, e.Cause)
}

func (e *ParameterTypeError) Unwrap() error {
	return e.Cause
}
