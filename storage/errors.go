package storage

import (
	"errors"
	"fmt"
)

// since these errors are allocated at init time, it is better to leave them as normal errors instead of
// errors that have stack encoded.
var (
	ErrCollision                = errors.New("item already exists")
	ErrInvalidContinuationToken = errors.New("invalid continuation token")
	ErrNotFound                 = errors.New("not found")
	ErrTransactionalWriteFailed = errors.New("transactional write failed due to bad input")
	ErrMismatchObjectType       = errors.New("mismatched types in request and continuation token")
	ErrExceededWriteBatchLimit  = errors.New("number of operations exceeded write batch limit")
	ErrCancelled                = errors.New("request has been cancelled")
)

func ExceededMaxTypeDefinitionsLimitError(limit int) error {
	return fmt.Errorf("exceeded number of allowed type definitions: %d", limit)
}
