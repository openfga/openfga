package utils

import (
	"context"
	"errors"
	"fmt"
)

var ErrPanic = errors.New("panic captured")

func RunAsync(ctx context.Context, fn func(context.Context) error) <-chan error {
	errorChan := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				errorChan <- fmt.Errorf("%w: %s", ErrPanic, r)
			}
			close(errorChan)
		}()

		err := fn(ctx)
		if err != nil {
			errorChan <- err
		}
	}()

	return errorChan
}
