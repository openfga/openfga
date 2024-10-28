package commands

import (
	"context"
	"errors"

	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/graph"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/tuple"
)

type InvalidTupleError struct {
	Cause error
}

func (e *InvalidTupleError) Unwrap() error {
	return e.Cause
}

func (e *InvalidTupleError) Error() string {
	return e.Unwrap().Error()
}

type InvalidRelationError struct {
	Cause error
}

func (e *InvalidRelationError) Unwrap() error {
	return e.Cause
}

func (e *InvalidRelationError) Error() string {
	return e.Unwrap().Error()
}

type ThrottledError struct {
	Cause error
}

func (e *ThrottledError) Unwrap() error {
	return e.Cause
}

func (e *ThrottledError) Error() string {
	return e.Unwrap().Error()
}

func CheckCommandErrorToServerError(err error) error {
	var invalidRelationError *InvalidRelationError
	if errors.As(err, &invalidRelationError) {
		return serverErrors.ValidationError(err)
	}

	var invalidTupleError *InvalidTupleError
	if errors.As(err, &invalidTupleError) {
		tupleError := tuple.InvalidTupleError{Cause: err}
		return serverErrors.HandleTupleValidateError(&tupleError)
	}

	if errors.Is(err, graph.ErrResolutionDepthExceeded) {
		return serverErrors.AuthorizationModelResolutionTooComplex
	}

	if errors.Is(err, condition.ErrEvaluationFailed) {
		return serverErrors.ValidationError(err)
	}

	var throttledError *ThrottledError
	if errors.As(err, &throttledError) {
		return serverErrors.ThrottledTimeout
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return serverErrors.RequestDeadlineExceeded
	}

	return serverErrors.HandleError("", err)
}
