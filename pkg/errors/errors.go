package errors

import "github.com/go-errors/errors"

// ErrorWithStack wraps the error with stack if error is non nil.
// Otherwise, the wrap will create a new "error" that has nil in it.
func ErrorWithStack(err error) error {
	if err != nil {
		return errors.Wrap(err, 1)
	}
	return err
}
