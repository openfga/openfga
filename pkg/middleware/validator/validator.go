// Package validator contains middleware that validates API input parameters.
package validator

import (
	"context"

	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"google.golang.org/grpc"
)

type ctxKey string

var (
	requestIsValidatedCtxKey = ctxKey("request-validated")
)

func contextWithRequestIsValidated(ctx context.Context) context.Context {
	return context.WithValue(ctx, requestIsValidatedCtxKey, true)
}

func RequestIsValidatedFromContext(ctx context.Context) bool {
	validated, ok := ctx.Value(requestIsValidatedCtxKey).(bool)
	return validated && ok
}

// UnaryServerInterceptor returns a new unary server interceptor that runs request validations and injects a bool in the context indicating that validation has been run.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {

	validator := grpc_validator.UnaryServerInterceptor()

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return validator(ctx, req, info, func(ctx context.Context, req interface{}) (interface{}, error) {
			return handler(contextWithRequestIsValidated(ctx), req)
		})
	}
}

// StreamServerInterceptor returns a new streaming server interceptor that runs request validations and injects a bool in the context indicating that validation has been run.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	validator := grpc_validator.StreamServerInterceptor()

	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return validator(srv, stream, info, func(srv interface{}, ss grpc.ServerStream) error {
			return handler(srv, &recvWrapper{
				ctx:          contextWithRequestIsValidated(stream.Context()),
				ServerStream: ss,
			})
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
