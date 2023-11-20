// Package validator contains middleware that validates API input parameters.
package validator

import (
	"context"

	grpcvalidator "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
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
	requestValidator := grpcvalidator.UnaryServerInterceptor()

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return requestValidator(ctx, req, info, func(ctx context.Context, req interface{}) (interface{}, error) {
			if err := requestContextWithinLimits(req); err != nil {
				return nil, err
			}

			return handler(contextWithRequestIsValidated(ctx), req)
		})
	}
}

// StreamServerInterceptor returns a new streaming server interceptor that runs request validations and injects a bool in the context indicating that validation has been run.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	requestValidator := grpcvalidator.StreamServerInterceptor()

	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return requestValidator(srv, stream, info, func(srv interface{}, ss grpc.ServerStream) error {
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

func (r *recvWrapper) RecvMsg(m any) error {
	if err := r.ServerStream.RecvMsg(m); err != nil {
		return err
	}

	if err := requestContextWithinLimits(m); err != nil {
		return err
	}

	return nil
}

func (r *recvWrapper) Context() context.Context {
	return r.ctx
}
