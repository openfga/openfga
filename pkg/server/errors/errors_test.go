package errors

import (
	"errors"
	"testing"

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
