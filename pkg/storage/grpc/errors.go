package grpc

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/pkg/storage"
	storagev1 "github.com/openfga/openfga/pkg/storage/grpc/proto/storage/v1"
)

// wrappedError wraps a base error but returns a custom message.
// This allows errors.Is() to work while preserving the original error message.
type wrappedError struct {
	msg string
	err error
}

func (e *wrappedError) Error() string {
	return e.msg
}

func (e *wrappedError) Unwrap() error {
	return e.err
}

// toGRPCError converts a storage error to a gRPC status error with error details.
// This is used on the server side to convert storage errors to gRPC errors.
func toGRPCError(err error) error {
	if err == nil {
		return nil
	}

	var code codes.Code
	var reason storagev1.StorageErrorReason

	// Check for known storage errors and map them to appropriate gRPC status codes
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		// context errors are not storage errors, so we return them as-is
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		// context errors are not storage errors, so we return them as-is
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, storage.ErrNotFound):
		code = codes.NotFound
		reason = storagev1.StorageErrorReason_NOT_FOUND
	case errors.Is(err, storage.ErrCollision):
		code = codes.AlreadyExists
		reason = storagev1.StorageErrorReason_COLLISION
	case errors.Is(err, storage.ErrInvalidContinuationToken):
		code = codes.InvalidArgument
		reason = storagev1.StorageErrorReason_INVALID_CONTINUATION_TOKEN
	case errors.Is(err, storage.ErrInvalidStartTime):
		code = codes.InvalidArgument
		reason = storagev1.StorageErrorReason_INVALID_START_TIME
	case errors.Is(err, storage.ErrInvalidWriteInput):
		code = codes.InvalidArgument
		reason = storagev1.StorageErrorReason_INVALID_WRITE_INPUT
	case errors.Is(err, storage.ErrWriteConflictOnInsert):
		code = codes.Aborted
		reason = storagev1.StorageErrorReason_WRITE_CONFLICT_ON_INSERT
	case errors.Is(err, storage.ErrWriteConflictOnDelete):
		code = codes.Aborted
		reason = storagev1.StorageErrorReason_WRITE_CONFLICT_ON_DELETE
	case errors.Is(err, storage.ErrTransactionalWriteFailed):
		code = codes.Aborted
		reason = storagev1.StorageErrorReason_TRANSACTIONAL_WRITE_FAILED
	case errors.Is(err, storage.ErrTransactionThrottled):
		code = codes.ResourceExhausted
		reason = storagev1.StorageErrorReason_TRANSACTION_THROTTLED
	default:
		// For unknown errors, wrap them as internal errors to preserve the original message
		return status.Error(codes.Internal, fmt.Sprintf("storage error: %v", err))
	}

	// Create status with error details
	st := status.New(code, err.Error())
	stWithDetails, detailErr := st.WithDetails(&errdetails.ErrorInfo{
		Reason: reason.String(),
		Domain: "openfga.storage",
	})
	if detailErr != nil {
		// If adding details fails, fall back to simple status error
		return status.Error(code, err.Error())
	}

	return stWithDetails.Err()
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

	// Try to extract error details first
	for _, detail := range st.Details() {
		if errInfo, ok := detail.(*errdetails.ErrorInfo); ok {
			switch errInfo.GetReason() {
			case storagev1.StorageErrorReason_NOT_FOUND.String():
				return storage.ErrNotFound
			case storagev1.StorageErrorReason_COLLISION.String():
				return storage.ErrCollision
			case storagev1.StorageErrorReason_INVALID_CONTINUATION_TOKEN.String():
				return &wrappedError{msg: st.Message(), err: storage.ErrInvalidContinuationToken}
			case storagev1.StorageErrorReason_INVALID_START_TIME.String():
				return &wrappedError{msg: st.Message(), err: storage.ErrInvalidStartTime}
			case storagev1.StorageErrorReason_INVALID_WRITE_INPUT.String():
				return &wrappedError{msg: st.Message(), err: storage.ErrInvalidWriteInput}
			case storagev1.StorageErrorReason_WRITE_CONFLICT_ON_INSERT.String():
				return &wrappedError{msg: st.Message(), err: storage.ErrWriteConflictOnInsert}
			case storagev1.StorageErrorReason_WRITE_CONFLICT_ON_DELETE.String():
				return &wrappedError{msg: st.Message(), err: storage.ErrWriteConflictOnDelete}
			case storagev1.StorageErrorReason_TRANSACTIONAL_WRITE_FAILED.String():
				return &wrappedError{msg: st.Message(), err: storage.ErrTransactionalWriteFailed}
			case storagev1.StorageErrorReason_TRANSACTION_THROTTLED.String():
				return storage.ErrTransactionThrottled
			}
		}
	}

	// Fall back to code-based mapping if no error details found
	// This provides backward compatibility with older servers
	switch st.Code() {
	case codes.NotFound:
		return storage.ErrNotFound
	case codes.AlreadyExists:
		return storage.ErrCollision
	case codes.InvalidArgument:
		return fmt.Errorf("invalid argument: %s", st.Message())
	case codes.Aborted:
		return storage.ErrTransactionalWriteFailed
	case codes.ResourceExhausted:
		return storage.ErrTransactionThrottled
	case codes.DeadlineExceeded:
		return context.DeadlineExceeded
	case codes.Canceled:
		return context.Canceled
	default:
		return fmt.Errorf("grpc error (code=%s): %s", st.Code(), st.Message())
	}
}
