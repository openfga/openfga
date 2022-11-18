package errors

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func TestContextualTupleValidationErrors(t *testing.T) {
	type errorTest struct {
		error   error
		code    codes.Code
		message string
	}

	tk := &openfgapb.TupleKey{Object: "doc:top-secret", Relation: "access", User: "mallory"}

	tests := []errorTest{
		{
			error:   InvalidContextualTuple(tk),
			code:    codes.Code(openfgapb.ErrorCode_invalid_contextual_tuple),
			message: fmt.Sprintf("Invalid contextual tuple: %s. Please provide a user, object and relation.", tk.String()),
		},
		{
			error:   DuplicateContextualTuple(tk),
			code:    codes.Code(openfgapb.ErrorCode_duplicate_contextual_tuple),
			message: fmt.Sprintf("Duplicate contextual tuple in request: %s.", tk.String()),
		},
	}

	for _, test := range tests {
		stat, ok := status.FromError(test.error)
		if !ok {
			t.Error("expected a status, but didn't get one")
		}
		if test.code != stat.Code() {
			t.Errorf("expected '%v' but got '%v'", test.code, stat.Code())
		}
		if test.message != stat.Message() {
			t.Errorf("expected '%v' but got '%v'", test.code, stat.Code())
		}
	}
}
