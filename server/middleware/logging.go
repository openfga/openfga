package middleware

import (
	"context"

	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/server/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func NewErrorLoggingInterceptor(logger logger.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			var e error
			if internalError, ok := err.(serverErrors.InternalError); ok {
				e = internalError.Internal()
			}
			logger.Error("grpc_error", zap.Error(e), zap.String("public_error", err.Error()))

			return nil, err
		}

		return resp, nil
	}
}

func NewStreamingErrorLoggingInterceptor(logger logger.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, stream)
		if err != nil {
			var e error
			if internalError, ok := err.(serverErrors.InternalError); ok {
				e = internalError.Internal()
			}
			logger.Error("grpc_error", zap.Error(e), zap.String("public_error", err.Error()))
		}

		return err
	}
}
