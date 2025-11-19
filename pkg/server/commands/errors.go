package commands

import (
	"context"
	"errors"

	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/graph"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/tuple"
)

var errInvalidTupleDeprecate *InvalidTupleError
var errInvalidRelationError *InvalidRelationError
var errInvalidTuple *tuple.InvalidTupleError
var errThrottled *ThrottledError

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

// CheckCommandErrorToServerError converts internal errors thrown during the
// check_command into consumer-facing errors to be sent over the wire.
func CheckCommandErrorToServerError(err error) error {
	if errors.As(err, &errInvalidRelationError) {
		return serverErrors.ValidationError(err)
	}

	if errors.As(err, &errInvalidTupleDeprecate) {
		tupleError := tuple.InvalidTupleError{Cause: err}
		return serverErrors.HandleTupleValidateError(&tupleError)
	}

	if errors.Is(err, errInvalidTuple) {
		return serverErrors.HandleTupleValidateError(err)
	}

	if errors.Is(err, graph.ErrResolutionDepthExceeded) {
		return serverErrors.ErrAuthorizationModelResolutionTooComplex
	}

	if errors.Is(err, condition.ErrEvaluationFailed) {
		return serverErrors.ValidationError(err)
	}

	if errors.As(err, &errThrottled) {
		return serverErrors.ErrThrottledTimeout
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return serverErrors.ErrRequestDeadlineExceeded
	}

	return serverErrors.HandleError("", err)
}
