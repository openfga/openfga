package grpc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
)

func TestToGRPCError(t *testing.T) {
	tests := []struct {
		name         string
		storageErr   error
		expectedCode codes.Code
	}{
		{
			name:         "nil error",
			storageErr:   nil,
			expectedCode: codes.OK,
		},
		{
			name:         "ErrNotFound",
			storageErr:   storage.ErrNotFound,
			expectedCode: codes.NotFound,
		},
		{
			name:         "ErrCollision",
			storageErr:   storage.ErrCollision,
			expectedCode: codes.AlreadyExists,
		},
		{
			name:         "ErrInvalidContinuationToken",
			storageErr:   storage.ErrInvalidContinuationToken,
			expectedCode: codes.InvalidArgument,
		},
		{
			name:         "ErrInvalidStartTime",
			storageErr:   storage.ErrInvalidStartTime,
			expectedCode: codes.InvalidArgument,
		},
		{
			name:         "ErrInvalidWriteInput",
			storageErr:   storage.ErrInvalidWriteInput,
			expectedCode: codes.InvalidArgument,
		},
		{
			name:         "ErrTransactionalWriteFailed",
			storageErr:   storage.ErrTransactionalWriteFailed,
			expectedCode: codes.Aborted,
		},
		{
			name:         "ErrWriteConflictOnInsert",
			storageErr:   storage.ErrWriteConflictOnInsert,
			expectedCode: codes.Aborted,
		},
		{
			name:         "ErrWriteConflictOnDelete",
			storageErr:   storage.ErrWriteConflictOnDelete,
			expectedCode: codes.Aborted,
		},
		{
			name:         "ErrTransactionThrottled",
			storageErr:   storage.ErrTransactionThrottled,
			expectedCode: codes.ResourceExhausted,
		},
		{
			name:         "context.DeadlineExceeded",
			storageErr:   context.DeadlineExceeded,
			expectedCode: codes.DeadlineExceeded,
		},
		{
			name:         "context.Canceled",
			storageErr:   context.Canceled,
			expectedCode: codes.Canceled,
		},
		{
			name:         "unknown error",
			storageErr:   errors.New("some unknown error"),
			expectedCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			grpcErr := toGRPCError(tt.storageErr)

			if tt.storageErr == nil {
				if grpcErr != nil {
					t.Errorf("expected nil error, got %v", grpcErr)
				}
				return
			}

			st, ok := status.FromError(grpcErr)
			if !ok {
				t.Fatalf("expected gRPC status error, got %v", grpcErr)
			}

			if st.Code() != tt.expectedCode {
				t.Errorf("expected code %v, got %v", tt.expectedCode, st.Code())
			}
		})
	}
}

func TestFromGRPCError(t *testing.T) {
	t.Run("backward compatibility - without error details", func(t *testing.T) {
		// Test that we can handle errors from older servers that don't include error details
		tests := []struct {
			name        string
			grpcErr     error
			expectedErr error
		}{
			{
				name:        "NotFound fallback",
				grpcErr:     status.Error(codes.NotFound, "not found"),
				expectedErr: storage.ErrNotFound,
			},
			{
				name:        "AlreadyExists fallback",
				grpcErr:     status.Error(codes.AlreadyExists, "already exists"),
				expectedErr: storage.ErrCollision,
			},
			{
				name:        "InvalidArgument fallback",
				grpcErr:     status.Error(codes.InvalidArgument, "some validation error"),
				expectedErr: nil, // Returns wrapped error
			},
			{
				name:        "Aborted fallback",
				grpcErr:     status.Error(codes.Aborted, "conflict"),
				expectedErr: storage.ErrTransactionalWriteFailed,
			},
			{
				name:        "ResourceExhausted fallback",
				grpcErr:     status.Error(codes.ResourceExhausted, "throttled"),
				expectedErr: storage.ErrTransactionThrottled,
			},
			{
				name:        "DeadlineExceeded fallback",
				grpcErr:     status.Error(codes.DeadlineExceeded, "deadline exceeded"),
				expectedErr: context.DeadlineExceeded,
			},
			{
				name:        "Canceled fallback",
				grpcErr:     status.Error(codes.Canceled, "canceled"),
				expectedErr: context.Canceled,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := fromGRPCError(tt.grpcErr)

				if result == nil {
					t.Fatalf("expected error, got nil")
				}

				if tt.expectedErr != nil {
					if !errors.Is(result, tt.expectedErr) {
						t.Errorf("expected error %v, got %v", tt.expectedErr, result)
					}
				}
			})
		}
	})

	t.Run("non-gRPC error passes through", func(t *testing.T) {
		regularErr := errors.New("regular error")
		result := fromGRPCError(regularErr)
		if result.Error() != regularErr.Error() {
			t.Errorf("expected error to pass through as %v, got %v", regularErr, result)
		}
	})
}

func TestErrorRoundTrip(t *testing.T) {
	// Test that converting storage errors to gRPC and back preserves the error type
	storageErrors := []error{
		storage.ErrNotFound,
		storage.ErrCollision,
		storage.ErrInvalidContinuationToken,
		storage.ErrInvalidStartTime,
		storage.ErrInvalidWriteInput,
		storage.ErrTransactionalWriteFailed,
		storage.ErrWriteConflictOnInsert,
		storage.ErrWriteConflictOnDelete,
		storage.ErrTransactionThrottled,
		context.DeadlineExceeded,
		context.Canceled,
	}

	for _, originalErr := range storageErrors {
		t.Run(originalErr.Error(), func(t *testing.T) {
			// Convert to gRPC error
			grpcErr := toGRPCError(originalErr)

			// Convert back to storage error
			resultErr := fromGRPCError(grpcErr)

			// Check that we got the same error type back
			if !errors.Is(resultErr, originalErr) {
				t.Errorf("round trip failed: expected %v, got %v", originalErr, resultErr)
			}
		})
	}
}

func TestWrappedError(t *testing.T) {
	// Test that wrappedError correctly handles nested error chains
	t.Run("works with nested errors", func(t *testing.T) {
		// Test that wrappedError works with errors that themselves wrap other errors
		conflictErr := storage.ErrWriteConflictOnInsert
		msg := "transactional write failed due to conflict: one or more tuples to write were inserted by another transaction"

		wrapped := &wrappedError{
			msg: msg,
			err: conflictErr,
		}

		// Should find the direct wrapped error
		if !errors.Is(wrapped, storage.ErrWriteConflictOnInsert) {
			t.Errorf("expected errors.Is to find ErrWriteConflictOnInsert")
		}

		// Should also find the underlying error (ErrWriteConflictOnInsert wraps ErrTransactionalWriteFailed)
		if !errors.Is(wrapped, storage.ErrTransactionalWriteFailed) {
			t.Errorf("expected errors.Is to find ErrTransactionalWriteFailed in nested chain")
		}
	})
}

func TestDetailedErrorMessages(t *testing.T) {
	t.Run("InvalidWriteInputError preserves tuple details", func(t *testing.T) {
		tk := &openfgav1.TupleKey{
			Object:   "doc:test",
			Relation: "viewer",
			User:     "user:alice",
		}

		// Create a detailed error like SQL stores do
		detailedErr := storage.InvalidWriteInputError(tk, openfgav1.TupleOperation_TUPLE_OPERATION_WRITE)

		// Convert to gRPC and back
		grpcErr := toGRPCError(detailedErr)
		resultErr := fromGRPCError(grpcErr)

		// Should preserve the detailed message
		expectedMsg := "cannot write a tuple which already exists: user: 'user:alice', relation: 'viewer', object: 'doc:test': tuple to be written already existed or the tuple to be deleted did not exist"
		if resultErr.Error() != expectedMsg {
			t.Errorf("expected message %q, got %q", expectedMsg, resultErr.Error())
		}

		// Should still work with errors.Is
		if !errors.Is(resultErr, storage.ErrInvalidWriteInput) {
			t.Errorf("expected errors.Is to find ErrInvalidWriteInput")
		}
	})

	t.Run("InvalidWriteInputError for delete preserves tuple details", func(t *testing.T) {
		tk := &openfgav1.TupleKeyWithoutCondition{
			Object:   "doc:test",
			Relation: "viewer",
			User:     "user:alice",
		}

		// Create a detailed error for delete operation
		detailedErr := storage.InvalidWriteInputError(tk, openfgav1.TupleOperation_TUPLE_OPERATION_DELETE)

		// Convert to gRPC and back
		grpcErr := toGRPCError(detailedErr)
		resultErr := fromGRPCError(grpcErr)

		// Should preserve the detailed message
		expectedMsg := "cannot delete a tuple which does not exist: user: 'user:alice', relation: 'viewer', object: 'doc:test': tuple to be written already existed or the tuple to be deleted did not exist"
		if resultErr.Error() != expectedMsg {
			t.Errorf("expected message %q, got %q", expectedMsg, resultErr.Error())
		}

		// Should still work with errors.Is
		if !errors.Is(resultErr, storage.ErrInvalidWriteInput) {
			t.Errorf("expected errors.Is to find ErrInvalidWriteInput")
		}
	})
}

func TestContextErrorHandling(t *testing.T) {
	// Test that context errors wrapped in other errors are detected correctly
	t.Run("wrapped context errors", func(t *testing.T) {
		// Test that context errors wrapped in other errors are handled correctly
		wrappedErr := fmt.Errorf("operation failed: %w", context.DeadlineExceeded)

		grpcErr := toGRPCError(wrappedErr)
		st, ok := status.FromError(grpcErr)
		if !ok {
			t.Fatalf("expected gRPC status error")
		}

		// Should detect the context error in the chain
		if st.Code() != codes.DeadlineExceeded {
			t.Errorf("expected code %v, got %v", codes.DeadlineExceeded, st.Code())
		}
	})
}
