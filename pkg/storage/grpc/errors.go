package grpc

import (
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/pkg/storage"
)

// toGRPCError converts a storage error to a gRPC status error.
// This is used on the server side to convert storage errors to gRPC errors.
func toGRPCError(err error) error {
	if err == nil {
		return nil
	}

	// Check for known storage errors and map them to appropriate gRPC status codes
	switch {
	case errors.Is(err, storage.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, storage.ErrCollision):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, storage.ErrInvalidContinuationToken):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, storage.ErrInvalidStartTime):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, storage.ErrInvalidWriteInput):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, storage.ErrTransactionalWriteFailed):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, storage.ErrWriteConflictOnInsert):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, storage.ErrWriteConflictOnDelete):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, storage.ErrTransactionThrottled):
		return status.Error(codes.ResourceExhausted, err.Error())
	default:
		// For unknown errors, wrap them as internal errors to preserve the original message
		return status.Error(codes.Internal, fmt.Sprintf("storage error: %v", err))
	}
}

// fromGRPCError converts a gRPC status error to a storage error.
// This is used on the client side to convert gRPC errors back to storage errors.
func fromGRPCError(err error) error {
	if err == nil {
		return nil
	}

	st, ok := status.FromError(err)
	if !ok {
		// Not a gRPC status error, return as-is
		return err
	}

	// Map gRPC status codes back to storage errors
	switch st.Code() {
	case codes.NotFound:
		return storage.ErrNotFound
	case codes.AlreadyExists:
		return storage.ErrCollision
	case codes.InvalidArgument:
		// Try to determine which specific error based on message content
		msg := st.Message()
		if msg == storage.ErrInvalidContinuationToken.Error() {
			return storage.ErrInvalidContinuationToken
		}
		if msg == storage.ErrInvalidStartTime.Error() {
			return storage.ErrInvalidStartTime
		}
		// Check if message contains the base error message (handles wrapped errors)
		if msg == storage.ErrInvalidWriteInput.Error() || strings.Contains(msg, storage.ErrInvalidWriteInput.Error()) {
			return storage.ErrInvalidWriteInput
		}
		// Return a generic wrapped error for other invalid argument cases
		return fmt.Errorf("invalid argument: %s", msg)
	case codes.Aborted:
		// Try to determine which specific conflict error
		msg := st.Message()
		if msg == storage.ErrWriteConflictOnInsert.Error() || strings.Contains(msg, storage.ErrWriteConflictOnInsert.Error()) {
			return storage.ErrWriteConflictOnInsert
		}
		if msg == storage.ErrWriteConflictOnDelete.Error() || strings.Contains(msg, storage.ErrWriteConflictOnDelete.Error()) {
			return storage.ErrWriteConflictOnDelete
		}
		if msg == storage.ErrTransactionalWriteFailed.Error() || strings.Contains(msg, storage.ErrTransactionalWriteFailed.Error()) {
			return storage.ErrTransactionalWriteFailed
		}
		// Return the base transactional write failed error for other abort cases
		return storage.ErrTransactionalWriteFailed
	case codes.ResourceExhausted:
		return storage.ErrTransactionThrottled
	default:
		// For other codes, return a wrapped error
		return fmt.Errorf("grpc error (code=%s): %s", st.Code(), st.Message())
	}
}
