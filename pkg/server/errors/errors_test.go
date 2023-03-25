package errors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInternalErrorDontLeakInternals(t *testing.T) {
	err := NewInternalError("public", errors.New("internal"))

	require.NotContains(t, err.Error(), "internal")
}

func TestInternalErrorsWithNoMessageReturnsInternalServiceError(t *testing.T) {
	err := NewInternalError("", errors.New("internal"))

	expected := InternalServerErrorMsg
	require.Contains(t, err.Error(), expected)
}
