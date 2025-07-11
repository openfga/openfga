package concurrency

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"github.com/sourcegraph/conc/pool"
	"go.uber.org/zap"

	"github.com/openfga/openfga/pkg/logger"
)

type Pool = pool.ContextPool

// NewPool returns a new pool where each task respects context cancellation.
// Wait() will only return the first error seen.
func NewPool(ctx context.Context, maxGoroutines int) *Pool {
	return pool.New().
		WithContext(ctx).
		WithCancelOnError().
		WithFirstError().
		WithMaxGoroutines(maxGoroutines)
}

// TrySendThroughChannel attempts to send an object through a channel.
// If the context is canceled, it will not send the object.
func TrySendThroughChannel[T any](ctx context.Context, msg T, channel chan<- T) bool {
	select {
	case <-ctx.Done():
		return false
	case channel <- msg:
		return true
	}
}

var ErrPanic = errors.New("panic captured")

// RecoverFromPanic will recover from a panic and assign the panic message to the given error.
func RecoverFromPanic(err *error) {
	if r := recover(); r != nil {
		*err = fmt.Errorf(
			"%w: %q (err=%w)",
			ErrPanic,
			r,
			*err,
		)
	}
}

var ReallyCrash = true

func HandleCrash(ctx context.Context) {
	if r := recover(); r != nil {
		l := logger.FromContext(ctx)
		if l != nil {
			logCrash(l, r)
		}

		if ReallyCrash {
			panic(r)
		}
	}
}

func logCrash(l logger.Logger, r any) {
	_, file, line, _ := runtime.Caller(2)

	if msg, ok := r.(string); ok {
		l.Panic(msg, zap.String("code.file", file), zap.Int("code.line", line))
	} else {
		l.Panic(fmt.Sprintf("%#v", r), zap.String("code.file", file), zap.Int("code.line", line))
	}
}
