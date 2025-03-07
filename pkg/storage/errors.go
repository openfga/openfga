package storage

import (
	"errors"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
)

var (
	// ErrCollision is returned when an item already exists within the store.
	ErrCollision = errors.New("item already exists")

	// ErrInvalidContinuationToken is returned when the continuation token is invalid.
	ErrInvalidContinuationToken = errors.New("invalid continuation token")

	// ErrInvalidStartTime is returned when start time param for ReadChanges API is invalid.
	ErrInvalidStartTime = errors.New("invalid start time")

	// ErrInvalidWriteInput is returned when the tuple to be written
	// already existed or the tuple to be deleted did not exist.
	ErrInvalidWriteInput = errors.New("tuple to be written already existed or the tuple to be deleted did not exist")

	// ErrTransactionalWriteFailed is returned when two writes attempt to write the same tuple at the same time.
	ErrTransactionalWriteFailed = errors.New("transactional write failed due to conflict")

	// ErrTransactionThrottled is returned when throttling is applied at the datastore level.
	ErrTransactionThrottled = errors.New("transaction throttled")

	// ErrNotFound is returned when the object does not exist.
	ErrNotFound = errors.New("not found")
)

// InvalidWriteInputError generates an error for invalid operations in a tuple store.
// This function is invoked when an attempt is made to write or delete a tuple with invalid conditions.
// Specifically, it addresses two scenarios:
// 1. Attempting to delete a non-existent tuple.
// 2. Attempting to write a tuple that already exists.
func InvalidWriteInputError(tk tuple.TupleWithoutCondition, operation openfgav1.TupleOperation) error {
	switch operation {
	case openfgav1.TupleOperation_TUPLE_OPERATION_DELETE:
		return fmt.Errorf(
			"cannot delete a tuple which does not exist: user: '%s', relation: '%s', object: '%s': %w",
			tk.GetUser(),
			tk.GetRelation(),
			tk.GetObject(),
			ErrInvalidWriteInput,
		)
	case openfgav1.TupleOperation_TUPLE_OPERATION_WRITE:
		return fmt.Errorf(
			"cannot write a tuple which already exists: user: '%s', relation: '%s', object: '%s': %w",
			tk.GetUser(),
			tk.GetRelation(),
			tk.GetObject(),
			ErrInvalidWriteInput,
		)
	default:
		return nil
	}
}
