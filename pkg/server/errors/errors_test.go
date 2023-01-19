package errors

import (
	"errors"
	"strings"
	"testing"
)

func TestInternalErrorDontLeakInternals(t *testing.T) {
	err := NewInternalError("public", errors.New("internal"))

	if strings.Contains(err.Error(), "internal") {
		t.Errorf("internal error is leaking internals: %v", err)
	}
}

func TestInternalErrorsWithNoMessageReturnsInternalServiceError(t *testing.T) {
	err := NewInternalError("", errors.New("internal"))

	expected := InternalServerErrorMsg
	if !strings.Contains(err.Error(), expected) {
		t.Errorf("expected error message to contain '%s', but got '%s'", expected, err)
	}
}
