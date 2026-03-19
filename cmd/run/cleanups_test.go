package run

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCleanupWithMessage(t *testing.T) {
	t.Run("returns nil when cleanup succeeds", func(t *testing.T) {
		wrapped := cleanupWithMessage(func(context.Context) error {
			return nil
		}, "cleanup failed")

		err := wrapped(context.Background())
		require.NoError(t, err)
	})

	t.Run("wraps the original error with the provided message", func(t *testing.T) {
		expectedErr := errors.New("boom")
		wrapped := cleanupWithMessage(func(context.Context) error {
			return expectedErr
		}, "cleanup failed")

		err := wrapped(context.Background())
		require.EqualError(t, err, "cleanup failed: boom")
	})
}

func TestCleanupFromPlainFunc(t *testing.T) {
	t.Run("returns nil when the function completes before the context is canceled", func(t *testing.T) {
		var called bool
		cleanupFn := cleanupFromPlainFunc(func() {
			called = true
		}, "plain cleanup")

		err := cleanupFn(context.Background())
		require.NoError(t, err)
		require.True(t, called)
	})

	t.Run("returns a wrapped context error when the context is canceled first", func(t *testing.T) {
		done := make(chan struct{})
		release := make(chan struct{})
		cleanupFn := cleanupFromPlainFunc(func() {
			close(done)
			<-release
		}, "plain cleanup")

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			errCh <- cleanupFn(ctx)
		}()

		<-done
		err := <-errCh
		close(release)

		require.EqualError(t, err, "plain cleanup: context deadline exceeded")
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}
