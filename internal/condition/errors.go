package condition

import (
	"fmt"

	"github.com/openfga/openfga/internal/errors"
)

var ErrEvaluationFailed = fmt.Errorf("failed to evaluate relationship condition")

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

func NewEvaluationError(condition string, cause error) error {
	return errors.With(&EvaluationError{
		Condition: condition,
		Cause:     cause,
	}, ErrEvaluationFailed)
}

func (e *EvaluationError) Error() string {
	if _, ok := e.Cause.(*ParameterTypeError); ok {
		return e.Unwrap().Error()
	}

	return fmt.Sprintf("%s '%s': %v", ErrEvaluationFailed.Error(), e.Condition, e.Cause)
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
