package storage

import (
	"fmt"

	"errors"

	"go.buf.build/openfga/go/openfga/api/openfga"
)

// since these errors are allocated at init time, it is better to leave them as normal errors instead of
// errors that have stack encoded.
var (
	InvalidContinuationToken                 = errors.New("invalid continuation token")
	InvalidWriteInput                        = errors.New("invalid write input")
	NonexistentDatabase                      = errors.New("database is nonexistent")
	NotFound                                 = errors.New("not found")
	Collision                                = errors.New("item already exists")
	CannotAllowMoreThanOneOperationOnOneItem = errors.New("cannot allow more than one operation on one item")
	TransactionalWriteFailed                 = errors.New("transactional write failed due to bad input")
	EntityLimitReached                       = errors.New("an entity in the operation is above specified limit")
	MismatchObjectType                       = errors.New("mismatched types in request and continuation token")
	ExceededWriteBatchLimit                  = errors.New("number of operations exceeded write batch limit")
	Cancelled                                = errors.New("request has been cancelled")
)

func ExceededMaxTypeDefinitionsLimitError(limit int) error {
	return fmt.Errorf("exceeded number of allowed type definitions: %d", limit)
}

func InvalidWriteInputError(tk *openfga.TupleKey, operation openfga.TupleOperation) error {
	switch operation {
	case openfga.TupleOperation_DELETE:
		return fmt.Errorf("cannot delete a tuple which does not exist: user: '%s', relation: '%s', object: '%s': %w", tk.GetUser(), tk.GetRelation(), tk.GetObject(), InvalidWriteInput)
	case openfga.TupleOperation_WRITE:
		return fmt.Errorf("cannot write a tuple which already exists: user: '%s', relation: '%s', object: '%s': %w", tk.GetUser(), tk.GetRelation(), tk.GetObject(), InvalidWriteInput)
	default:
		return nil
	}
}
