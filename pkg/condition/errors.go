package condition

import (
	"fmt"
)

type CompilationError struct {
	Cause error
}

func (e *CompilationError) Error() string {
	return fmt.Sprintf("failed to compile condition expression: %v", e.Cause)
}

func (e *CompilationError) Unwrap() error {
	return e.Cause
}

type EvaluationError struct {
	Cause error
}

func (e *EvaluationError) Error() string {
	return fmt.Sprintf("failed to evaluate relationship condition: %v", e.Cause)
}

func (e *EvaluationError) Unwrap() error {
	return e.Cause
}

type ParameterTypeError struct {
	Cause error
}

func (e *ParameterTypeError) Error() string {
	return e.Cause.Error()
}

func (e *ParameterTypeError) Unwrap() error {
	return e.Cause
}
