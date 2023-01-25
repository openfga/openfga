package middleware

import (
	"context"
	"encoding/json"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func NewLoggingInterceptor(logger logger.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		requestID := ulid.Make().String()

		ctx = metadata.AppendToOutgoingContext(ctx, "X-Request-Id", requestID)

		fields := []zap.Field{
			zap.String("method", info.FullMethod),
			zap.String("request_id", requestID),
		}

		spanCtx := trace.SpanContextFromContext(ctx)
		if spanCtx.HasTraceID() {
			fields = append(fields, zap.String("trace_id", spanCtx.TraceID().String()))
		}

		fields = append(fields, ctxzap.TagsToFields(ctx)...)

		jsonReq, err := json.Marshal(req)
		if err == nil {
			fields = append(fields, zap.Any("raw_request", json.RawMessage(jsonReq)))
		}

		resp, err := handler(ctx, req)

		fields = append(fields, zap.Duration("took", time.Since(start)))

		if err != nil {
			if internalError, ok := err.(serverErrors.InternalError); ok {
				fields = append(fields, zap.Error(internalError.Internal()))
			}

			fields = append(fields, zap.String("public_error", err.Error()))
			logger.Error("grpc_error", fields...)

			return nil, err
		}

		jsonResp, err := json.Marshal(resp)
		if err == nil {
			fields = append(fields, zap.Any("raw_response", json.RawMessage(jsonResp)))
		}

		logger.Info("grpc_complete", fields...)

		return resp, nil
	}
}

func NewStreamingLoggingInterceptor(logger logger.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()

		requestID := ulid.Make().String()

		fields := []zap.Field{
			zap.String("method", info.FullMethod),
			zap.String("request_id", requestID),
		}

		spanCtx := trace.SpanContextFromContext(stream.Context())
		if spanCtx.HasTraceID() {
			fields = append(fields, zap.String("trace_id", spanCtx.TraceID().String()))
		}

		fields = append(fields, ctxzap.TagsToFields(stream.Context())...)

		err := handler(srv, stream)

		fields = append(fields, zap.Duration("took", time.Since(start)))

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
