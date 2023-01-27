package middleware

import (
	"context"

	"github.com/google/uuid"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/openfga/openfga/pkg/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	requestIDCtxKey ctxKey = "request-id-context-key"

	requestIDHeader string = "X-Request-Id"
)

func NewRequestIDInterceptor(logger logger.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		id, err := uuid.NewRandom()
		if err != nil {
			logger.Error("failed to generate uuid", zap.Error(err))
		}

		requestID := id.String()

		ctx = context.WithValue(ctx, requestIDCtxKey, requestID)

		err = grpc.SetHeader(ctx, metadata.Pairs(requestIDHeader, requestID))
		if err != nil {
			logger.Error("failed to set header", zap.Error(err))
		}

		return handler(ctx, req)
	}
}

func NewStreamingRequestIDInterceptor(logger logger.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ss := grpc_middleware.WrapServerStream(stream)

		id, err := uuid.NewRandom()
		if err != nil {
			logger.Error("failed to generate uuid", zap.Error(err))
		}

		requestID := id.String()

		ss.WrappedContext = context.WithValue(ss.Context(), requestIDCtxKey, requestID)

		err = grpc.SetHeader(stream.Context(), metadata.Pairs(requestIDHeader, requestID))
		if err != nil {
			logger.Error("failed to set header", zap.Error(err))
		}

		return handler(srv, ss)
	}
}
