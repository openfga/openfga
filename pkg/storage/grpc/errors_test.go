package grpc

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	t.Run("with error details", func(t *testing.T) {
		// Test errors that have been properly converted with toGRPCError (includes error details)
		tests := []struct {
			name        string
			storageErr  error
			expectedErr error
		}{
			{
				name:        "nil error",
				storageErr:  nil,
				expectedErr: nil,
			},
			{
				name:        "NotFound",
				storageErr:  storage.ErrNotFound,
				expectedErr: storage.ErrNotFound,
			},
			{
				name:        "Collision",
				storageErr:  storage.ErrCollision,
				expectedErr: storage.ErrCollision,
			},
			{
				name:        "InvalidContinuationToken",
				storageErr:  storage.ErrInvalidContinuationToken,
				expectedErr: storage.ErrInvalidContinuationToken,
			},
			{
				name:        "InvalidStartTime",
				storageErr:  storage.ErrInvalidStartTime,
				expectedErr: storage.ErrInvalidStartTime,
			},
			{
				name:        "InvalidWriteInput",
				storageErr:  storage.ErrInvalidWriteInput,
				expectedErr: storage.ErrInvalidWriteInput,
			},
			{
				name:        "WriteConflictOnInsert",
				storageErr:  storage.ErrWriteConflictOnInsert,
				expectedErr: storage.ErrWriteConflictOnInsert,
			},
			{
				name:        "WriteConflictOnDelete",
				storageErr:  storage.ErrWriteConflictOnDelete,
				expectedErr: storage.ErrWriteConflictOnDelete,
			},
			{
				name:        "TransactionalWriteFailed",
				storageErr:  storage.ErrTransactionalWriteFailed,
				expectedErr: storage.ErrTransactionalWriteFailed,
			},
			{
				name:        "TransactionThrottled",
				storageErr:  storage.ErrTransactionThrottled,
				expectedErr: storage.ErrTransactionThrottled,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Convert using toGRPCError (which adds error details)
				grpcErr := toGRPCError(tt.storageErr)
				result := fromGRPCError(grpcErr)

				if tt.expectedErr == nil {
					if result != nil {
						t.Errorf("expected nil error, got %v", result)
					}
					return
				}

				if result == nil {
					t.Fatalf("expected error, got nil")
				}

				if !errors.Is(result, tt.expectedErr) {
					t.Errorf("expected error %v, got %v", tt.expectedErr, result)
				}
			})
		}
	})

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
