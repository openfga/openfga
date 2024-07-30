package errors

import "errors"

// Idea for this package is to hold all errors that can be eventually mapped to a distinct HTTP status code,
// and that can be extended with more details if need be.

var ErrUnknown = errors.New("internal server error")
