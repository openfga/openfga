package graph

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunAsync(t *testing.T) {
	ctx := context.Background()

	t.Run("should_return_nil_when_no_error_occurred", func(t *testing.T) {
		errorChan := RunAsync(ctx, func(ctx context.Context) error {
			return nil
		})

		err := <-errorChan

		require.NoError(t, err)
	})

	t.Run("should_return_error_when_fn_errors", func(t *testing.T) {
		errorChan := RunAsync(ctx, func(ctx context.Context) error {
			return errors.New("test error")
		})

		err := <-errorChan

		require.EqualError(t, err, "test error")
	})

	t.Run("should_return_error_when_fn_panics", func(t *testing.T) {
		errorChan := RunAsync(ctx, func(ctx context.Context) error {
			panic("test panic")
		})

		err := <-errorChan

		require.Error(t, err)
		require.ErrorIs(t, err, ErrPanic)
		require.ErrorContains(t, err, "test panic")
	})
}
