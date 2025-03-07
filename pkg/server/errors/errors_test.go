package errors

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	errors2 "github.com/openfga/openfga/internal/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestInternalError(t *testing.T) {
	t.Run("no_public_message_set", func(t *testing.T) {
		err := NewInternalError("", errors.New("internal"))
		require.Contains(t, err.Error(), InternalServerErrorMsg)
	})

	t.Run("public_message_set", func(t *testing.T) {
		err := NewInternalError("public", errors.New("internal"))
		require.Contains(t, err.Error(), "public")
	})

	t.Run("grpc_status", func(t *testing.T) {
		err := NewInternalError("", errors2.ErrUnknown)

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(openfgav1.InternalErrorCode_internal_error), st.Code())
		require.Equal(t, "rpc error: code = Code(4000) desc = Internal Server Error", st.String())
	})

	t.Run("error_is", func(t *testing.T) {
		err := NewInternalError("", errors2.ErrUnknown)
		require.ErrorIs(t, err, errors2.ErrUnknown)
		err = NewInternalError("", fmt.Errorf("%w", errors2.ErrUnknown))
		require.ErrorIs(t, err, errors2.ErrUnknown)
	})

	t.Run("unwrap", func(t *testing.T) {
		err := NewInternalError("", errors2.ErrUnknown)

		require.Equal(t, err.Unwrap(), errors2.ErrUnknown)
	})
}

func TestHandleErrors(t *testing.T) {
	tests := map[string]struct {
		storageErr              error
		expectedTranslatedError error
	}{
		`invalid_token`: {
			storageErr:              storage.ErrInvalidContinuationToken,
			expectedTranslatedError: ErrInvalidContinuationToken,
		},
		`context_cancelled`: {
			storageErr:              context.Canceled,
			expectedTranslatedError: ErrRequestCancelled,
		},
		`context_deadline_exceeded`: {
			storageErr:              context.DeadlineExceeded,
			expectedTranslatedError: ErrRequestDeadlineExceeded,
		},
		`throttling_error`: {
			storageErr:              storage.ErrTransactionThrottled,
			expectedTranslatedError: ErrTransactionThrottled,
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
			require.EqualError(t, HandleTupleValidateError(test.validateError), test.expectedTranslatedError.Error())
		})
	}
}
