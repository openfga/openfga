package storage

import (
	"errors"
	"fmt"

	"go.buf.build/openfga/go/openfga/api/openfga"
)

// since these errors are allocated at init time, it is better to leave them as normal errors instead of
// errors that have stack encoded.
var (
	ErrCollision                = errors.New("item already exists")
	ErrInvalidContinuationToken = errors.New("invalid continuation token")
	ErrInvalidWriteInput        = errors.New("invalid write input")
	ErrNotFound                 = errors.New("not found")
	ErrTransactionalWriteFailed = errors.New("transactional write failed due to bad input")
	ErrMismatchObjectType       = errors.New("mismatched types in request and continuation token")
	ErrExceededWriteBatchLimit  = errors.New("number of operations exceeded write batch limit")
	ErrCancelled                = errors.New("request has been cancelled")
)

func ExceededMaxTypeDefinitionsLimitError(limit int) error {
	return fmt.Errorf("exceeded number of allowed type definitions: %d", limit)
}

func InvalidWriteInputError(tk *openfga.TupleKey, operation openfga.TupleOperation) error {
	switch operation {
	case openfga.TupleOperation_DELETE:
		return fmt.Errorf("cannot delete a tuple which does not exist: user: '%s', relation: '%s', object: '%s': %w", tk.GetUser(), tk.GetRelation(), tk.GetObject(), ErrInvalidWriteInput)
	case openfga.TupleOperation_WRITE:
		return fmt.Errorf("cannot write a tuple which already exists: user: '%s', relation: '%s', object: '%s': %w", tk.GetUser(), tk.GetRelation(), tk.GetObject(), ErrInvalidWriteInput)
	default:
		return nil
	}
}
