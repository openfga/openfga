package middleware

import (
	"context"
	"time"

	grpcvalidator "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"google.golang.org/grpc"

	"github.com/openfga/openfga/pkg/logger"
)

// TimeoutHandler sets the timeout in each request
type TimeoutHandler struct {
	timeout time.Duration
	logger  logger.Logger
}

// NewTimeoutHandler returns new TimeoutHandler that timeouts request if it
// exceeds the timeout value
func NewTimeoutHandler(timeout time.Duration, logger logger.Logger) *TimeoutHandler {
	return &TimeoutHandler{
		timeout: timeout,
		logger:  logger,
	}
}

// NewUnaryTimeoutInterceptor configures the timeout expected
// We need to use this middleware instead of relying on runtime.DefaultContextTimeout to allow us
// to return proper error code
func (h *TimeoutHandler) NewUnaryTimeoutInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ctx, cancel := context.WithTimeout(ctx, h.timeout)
		defer cancel()
		return handler(ctx, req)
	}
}

// NewStreamTimeoutInterceptor configures the timeout expected
// We need to use this middleware instead of relying on runtime.DefaultContextTimeout to allow us
// to return proper error code
func (h *TimeoutHandler) NewStreamTimeoutInterceptor() grpc.StreamServerInterceptor {
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
