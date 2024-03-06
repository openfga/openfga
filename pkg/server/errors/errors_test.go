package errors

import (
	"errors"
	"fmt"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/pkg/storage"
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

func TestHandleStorageErrors(t *testing.T) {
	tests := map[string]struct {
		storageErr              error
		expectedTranslatedError error
	}{
		`invalid_token`: {
			storageErr:              storage.ErrInvalidContinuationToken,
			expectedTranslatedError: InvalidContinuationToken,
		},
		`invalid_token_for_read_changes_api`: {
			storageErr:              storage.ErrMismatchObjectType,
			expectedTranslatedError: MismatchObjectType,
		},
		`context_cancelled`: {
			storageErr:              storage.ErrCancelled,
			expectedTranslatedError: RequestCancelled,
		},
		`context_deadline_exceeeded`: {
			storageErr:              storage.ErrDeadlineExceeded,
			expectedTranslatedError: RequestDeadlineExceeded,
		},
		`invalid_write_input`: {
			storageErr:              storage.ErrInvalidWriteInput,
			expectedTranslatedError: WriteFailedDueToInvalidInput(storage.ErrInvalidWriteInput),
		},
		`transaction_failed`: {
			storageErr:              storage.ErrTransactionalWriteFailed,
			expectedTranslatedError: status.Error(codes.Aborted, storage.ErrTransactionalWriteFailed.Error()),
		},
	}
	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			require.ErrorIs(t, HandleError("", test.storageErr), test.expectedTranslatedError)
		})
	}
}

func TestHandleTupleValidateError(t *testing.T) {
	invalidConditionTupleError := tuple.InvalidConditionalTupleError{
		Cause:    fmt.Errorf("foo"),
		TupleKey: tuple.NewTupleKey("doc:x", "viewer", "user:z"),
	}

	tests := map[string]struct {
		validateError           error
		expectedTranslatedError error
	}{
		`invalid_tuple_error`: {
			validateError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("invalid tuple error"),
				TupleKey: tuple.NewCheckRequestTupleKey("object:x", "relation_y", "user:z"),
			},
			expectedTranslatedError: status.Error(
				codes.Code(openfgav1.ErrorCode_invalid_tuple),
				fmt.Sprintf("Invalid tuple '%s'. Reason: %s",
					tuple.NewCheckRequestTupleKey("object:x", "relation_y", "user:z"),
					fmt.Errorf("invalid tuple error")),
			),
		},
		`type_not_found`: {
			validateError: &tuple.TypeNotFoundError{
				TypeName: "doc",
			},
			expectedTranslatedError: TypeNotFound("doc"),
		},
		"relationship_not_found": {
			validateError: &tuple.RelationNotFoundError{
				TypeName: "doc",
				Relation: "viewer",
				TupleKey: tuple.NewTupleKey("doc:x", "viewer", "user:z"),
			},
			expectedTranslatedError: RelationNotFound(
				"viewer",
				"doc",
				tuple.NewTupleKey("doc:x", "viewer", "user:z"),
			),
		},
		"invalid_tuple_condition": {
			validateError: &invalidConditionTupleError,
			expectedTranslatedError: status.Error(
				codes.Code(openfgav1.ErrorCode_validation_error),
				invalidConditionTupleError.Error(),
			),
		},
		"undefined error": {
			validateError:           fmt.Errorf("unknown"),
			expectedTranslatedError: HandleError("", fmt.Errorf("unknown")),
		},
	}
	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			require.ErrorIs(t, HandleTupleValidateError(test.validateError), test.expectedTranslatedError)
		})
	}
}
