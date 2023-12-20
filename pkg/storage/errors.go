package storage

import (
	"errors"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
)

var (
	// Creation errors

	ErrCollision = errors.New("item already exists")

	// Read errors

	ErrInvalidContinuationToken = errors.New("invalid continuation token")
	ErrMismatchObjectType       = errors.New("mismatched types in request and continuation token")

	// Write errors

	// ErrInvalidWriteInput if the tuple to be written already existed or the tuple to be deleted didn't exist
	ErrInvalidWriteInput = errors.New("invalid write input")
	// ErrTransactionalWriteFailed if two writes attempt to write the same tuple at the same time
	ErrTransactionalWriteFailed = errors.New("transactional write failed due to conflict")
	// ErrExceededWriteBatchLimit if MaxTuplesPerWrite is exceeded
	ErrExceededWriteBatchLimit = errors.New("number of operations exceeded write batch limit")

	// All other errors

	ErrCancelled = errors.New("request has been cancelled")
	ErrNotFound  = errors.New("not found")
)

func ExceededMaxTypeDefinitionsLimitError(limit int) error {
	return fmt.Errorf("exceeded number of allowed type definitions: %d", limit)
}

func InvalidWriteInputError(tk tuple.TupleWithoutCondition, operation openfgav1.TupleOperation) error {
	switch operation {
	case openfgav1.TupleOperation_TUPLE_OPERATION_DELETE:
		return fmt.Errorf("cannot delete a tuple which does not exist: user: '%s', relation: '%s', object: '%s': %w", tk.GetUser(), tk.GetRelation(), tk.GetObject(), ErrInvalidWriteInput)
	case openfgav1.TupleOperation_TUPLE_OPERATION_WRITE:
		return fmt.Errorf("cannot write a tuple which already exists: user: '%s', relation: '%s', object: '%s': %w", tk.GetUser(), tk.GetRelation(), tk.GetObject(), ErrInvalidWriteInput)
	default:
		return nil
	}
}
