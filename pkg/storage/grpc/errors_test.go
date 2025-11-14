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
	tests := []struct {
		name        string
		grpcErr     error
		expectedErr error
	}{
		{
			name:        "nil error",
			grpcErr:     nil,
			expectedErr: nil,
		},
		{
			name:        "NotFound",
			grpcErr:     status.Error(codes.NotFound, "not found"),
			expectedErr: storage.ErrNotFound,
		},
		{
			name:        "AlreadyExists",
			grpcErr:     status.Error(codes.AlreadyExists, "already exists"),
			expectedErr: storage.ErrCollision,
		},
		{
			name:        "InvalidArgument - continuation token",
			grpcErr:     status.Error(codes.InvalidArgument, storage.ErrInvalidContinuationToken.Error()),
			expectedErr: storage.ErrInvalidContinuationToken,
		},
		{
			name:        "InvalidArgument - start time",
			grpcErr:     status.Error(codes.InvalidArgument, storage.ErrInvalidStartTime.Error()),
			expectedErr: storage.ErrInvalidStartTime,
		},
		{
			name:        "InvalidArgument - write input",
			grpcErr:     status.Error(codes.InvalidArgument, storage.ErrInvalidWriteInput.Error()),
			expectedErr: storage.ErrInvalidWriteInput,
		},
		{
			name:        "Aborted - write conflict on insert",
			grpcErr:     status.Error(codes.Aborted, storage.ErrWriteConflictOnInsert.Error()),
			expectedErr: storage.ErrWriteConflictOnInsert,
		},
		{
			name:        "Aborted - write conflict on delete",
			grpcErr:     status.Error(codes.Aborted, storage.ErrWriteConflictOnDelete.Error()),
			expectedErr: storage.ErrWriteConflictOnDelete,
		},
		{
			name:        "Aborted - transactional write failed",
			grpcErr:     status.Error(codes.Aborted, storage.ErrTransactionalWriteFailed.Error()),
			expectedErr: storage.ErrTransactionalWriteFailed,
		},
		{
			name:        "ResourceExhausted",
			grpcErr:     status.Error(codes.ResourceExhausted, "throttled"),
			expectedErr: storage.ErrTransactionThrottled,
		},
		{
			name:        "non-gRPC error passes through",
			grpcErr:     errors.New("regular error"),
			expectedErr: nil, // We just check that result equals grpcErr for this case
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fromGRPCError(tt.grpcErr)

			if tt.expectedErr == nil && tt.name == "nil error" {
				if result != nil {
					t.Errorf("expected nil error, got %v", result)
				}
				return
			}

			if tt.expectedErr == nil && tt.name == "non-gRPC error passes through" {
				// For non-gRPC errors, they should pass through unchanged
				if result.Error() != tt.grpcErr.Error() {
					t.Errorf("expected error to pass through as %v, got %v", tt.grpcErr, result)
				}
				return
			}

			if result == nil {
				t.Fatalf("expected error, got nil")
			}

			// Use errors.Is for all storage sentinel error comparisons
			if !errors.Is(result, tt.expectedErr) {
				t.Errorf("expected error %v, got %v", tt.expectedErr, result)
			}
		})
	}
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
