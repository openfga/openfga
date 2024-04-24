package middleware

import (
	"context"
	"time"

	grpcvalidator "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"google.golang.org/grpc"

	"github.com/openfga/openfga/pkg/logger"
)

// TimeoutInterceptor sets the timeout in each request.
type TimeoutInterceptor struct {
	timeout time.Duration
	logger  logger.Logger
}

// NewTimeoutInterceptor returns new TimeoutInterceptor that timeouts request if it
// exceeds the timeout value.
func NewTimeoutInterceptor(timeout time.Duration, logger logger.Logger) *TimeoutInterceptor {
	return &TimeoutInterceptor{
		timeout: timeout,
		logger:  logger,
	}
}

// NewUnaryTimeoutInterceptor returns an interceptor that will timeout according to the configured timeout.
// We need to use this middleware instead of relying on runtime.DefaultContextTimeout to allow us
// to return proper error code.
func (h *TimeoutInterceptor) NewUnaryTimeoutInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ctx, cancel := context.WithTimeout(ctx, h.timeout)
		defer cancel()
		return handler(ctx, req)
	}
}

// NewStreamTimeoutInterceptor returns an interceptor that will timeout according to the configured timeout.
// We need to use this middleware instead of relying on runtime.DefaultContextTimeout to allow us
// to return proper error code.
func (h *TimeoutInterceptor) NewStreamTimeoutInterceptor() grpc.StreamServerInterceptor {
	validator := grpcvalidator.StreamServerInterceptor()
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return validator(srv, stream, info, func(srv interface{}, ss grpc.ServerStream) error {
			ctx, cancel := context.WithTimeout(stream.Context(), h.timeout)
			defer cancel()

			return handler(srv, &recvWrapper{
				ctx:          ctx,
				ServerStream: ss,
			})
		})
	}
}

type recvWrapper struct {
	ctx context.Context
	grpc.ServerStream
}

// Context returns the context associated with the recvWrapper.
func (r *recvWrapper) Context() context.Context {
	return r.ctx
}
