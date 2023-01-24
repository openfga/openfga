package middleware

import (
	"context"
	"fmt"
	"time"

	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func NewLoggingInterceptor(logger logger.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		resp, err := handler(ctx, req)

		fields := []zap.Field{
			zap.String("method", info.FullMethod),
			zap.Duration("took", time.Since(start)),
		}

		spanCtx := trace.SpanContextFromContext(ctx)
		if spanCtx.HasSpanID() {
			fields = append(fields, zap.String("trace_id", spanCtx.TraceID().String()))
		}

		fields = append(fields, zap.String("raw_request", fmt.Sprintf("{%s}", req)))

		if err != nil {
			if internalError, ok := err.(serverErrors.InternalError); ok {
				fields = append(fields, zap.Error(internalError.Internal()))
			}

			fields = append(fields, zap.String("public_error", err.Error()))
			logger.Error("grpc_error", fields...)

			return nil, err
		}

		fields = append(fields, zap.String("raw_response", fmt.Sprintf("{%s}", resp)))

		logger.Info("grpc_complete", fields...)

		return resp, nil
	}
}

func NewStreamingLoggingInterceptor(logger logger.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()

		err := handler(srv, stream)

		fields := []zap.Field{
			zap.String("method", info.FullMethod),
			zap.Duration("took", time.Since(start)),
		}

		if err != nil {
			if internalError, ok := err.(serverErrors.InternalError); ok {
				fields = append(fields, zap.Error(internalError.Internal()))
			}

			fields = append(fields, zap.String("public_error", err.Error()))
			logger.Error("grpc_error", fields...)

			return err
		}

		logger.Info("grpc_complete", fields...)

		return nil
	}
}
