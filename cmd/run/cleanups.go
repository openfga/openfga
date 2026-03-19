package run

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
)

// cleanup runs resource cleanup with context and returns an error on failure.
type cleanup func(context.Context) error

// cleanupWithMessage wraps a cleanup function and adds msg to returned error.
func cleanupWithMessage(fn cleanup, msg string) cleanup {
	return func(ctx context.Context) error {
		if err := fn(ctx); err != nil {
			return fmt.Errorf("%s: %w", msg, err)
		}
		return nil
	}
}

// cleanupFromPlainFunc wraps a plain func (without context or error)
// into a context-aware cleanup.
func cleanupFromPlainFunc(fn func(), msg string) cleanup {
	return func(ctx context.Context) error {
		done := make(chan struct{})

		go func() {
			fn()
			close(done)
		}()

		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", msg, ctx.Err())
		}
	}
}

// cleanupGrpcServer returns a cleanup function that gracefully stops the gRPC server,
// falling back to a hard stop on context timeout.
func cleanupGrpcServer(grpcServer *grpc.Server) cleanup {
	return func(ctx context.Context) error {
		gracefulCleanup := cleanupFromPlainFunc(grpcServer.GracefulStop, "grpc server")

		if err := gracefulCleanup(ctx); err != nil {
			grpcServer.Stop()
			return err
		}

		return nil
	}
}
