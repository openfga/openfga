package condition

import (
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

type CompilationError struct {
	Condition *openfgav1.Condition
	Cause     error
}

func (e *CompilationError) Error() string {
	return fmt.Sprintf("failed to compile expression on condition '%s': %v", e.Condition.Name, e.Cause)
}

func (e *CompilationError) Unwrap() error {
	return e.Cause
}

type EvaluationError struct {
	Condition *openfgav1.Condition
	Cause     error
}

func (e *EvaluationError) Error() string {
	return fmt.Sprintf("failed to evaluate relationship condition '%s': %v", e.Condition.Name, e.Cause)
}

func (e *EvaluationError) Unwrap() error {
	return e.Cause
}

type ParameterTypeError struct {
	Condition *openfgav1.Condition
	Cause     error
}

func (e *ParameterTypeError) Error() string {
	return fmt.Sprintf("parameter type error on condition '%s': %v", e.Condition.Name, e.Cause)
}

func (e *ParameterTypeError) Unwrap() error {
	return e.Cause
}
