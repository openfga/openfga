package validator

import (
	"context"

	"google.golang.org/grpc"
)

type ctxKey string

var (
	requestIsValidatedCtxKey = ctxKey("request-validated")
)

func ContextWithRequestIsValidated(ctx context.Context) context.Context {
	return context.WithValue(ctx, requestIsValidatedCtxKey, true)
}

func RequestIsValidatedFromContext(ctx context.Context) bool {
	validated, ok := ctx.Value(requestIsValidatedCtxKey).(bool)
	return validated && ok
}

// UnaryServerInterceptor returns a new unary server interceptor that injects a bool indicating if validation has been run.
//
// It is expected that this middleware is run after all request validation middleware. Failing to run this middleware after
// request validation middleware could lead to downstream issues.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return handler(ContextWithRequestIsValidated(ctx), req)
	}
}

// StreamServerInterceptor returns a new streaming server interceptor that injects a bool indicating if validation has been run.
//
// It is expected that this middleware is run after all request validation middleware. Failing to run this middleware after
// request validation middleware could lead to downstream issues.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &recvWrapper{
			ctx:          ContextWithRequestIsValidated(stream.Context()),
			ServerStream: stream,
		})
	}
}

type recvWrapper struct {
	ctx context.Context
	grpc.ServerStream
}

func (r *recvWrapper) Context() context.Context {
	return r.ctx
}
