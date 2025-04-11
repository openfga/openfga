package graph

import (
	"context"
	"fmt"
)

type runAsyncConfig struct {
	propagateKeys []interface{}
}

type RunAsyncOption func(*runAsyncConfig)

type async struct {
	config runAsyncConfig
}

func NewAsync(options ...RunAsyncOption) *async {
	var conf runAsyncConfig

	for _, op := range options {
		op(&conf)
	}

	return &async{
		config: conf,
	}
}

func WithPropagateContextKeys(keys ...interface{}) RunAsyncOption {
	return func(conf *runAsyncConfig) {
		conf.propagateKeys = append(conf.propagateKeys, keys...)
	}
}

func (a *async) RunAsync(ctx context.Context, fn func(context.Context)) chan error {
	// Start a new async context
	ctx = a.asyncContext(ctx)
	errorChan := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				errorChan <- fmt.Errorf("panic occurred: %v", r)
			}
		}()

		fn(ctx)
	}()

	return errorChan
}

func (a *async) asyncContext(ctx context.Context) context.Context {
	asyncCtx := context.Background()
	for _, key := range a.config.propagateKeys {
		val := ctx.Value(key)
		asyncCtx = context.WithValue(asyncCtx, key, val)
	}

	return asyncCtx
}
