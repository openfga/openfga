package errors

import "errors"

// Idea for this package is to hold sentinel errors that can be eventually mapped to a distinct HTTP status code,
// If you need to add more details to the error so it will surface to the customer,
// use error wrapping (https://github.com/tomarrell/wrapcheck#why)

var ErrUnknown = errors.New("internal server error")

// FatalError wraps an error that must be propagated immediately by any consumer,
// regardless of whether other results have already been produced. It is not tied
// to any specific layer: any producer (filter, evaluator, resolver) may wrap an
// error as fatal, and any consumer can detect it with errors.As.
type FatalError struct {
	Cause error
}

func (e *FatalError) Error() string { return e.Cause.Error() }
func (e *FatalError) Unwrap() error { return e.Cause }
