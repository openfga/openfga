package middleware

import (
	"context"

	"github.com/google/uuid"
	"github.com/openfga/openfga/pkg/logger"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	requestIDCtxKey   ctxKey = "request-id-context-key"
	requestIDTraceKey string = "request_id"
	requestIDHeader   string = "X-Request-Id"
)

func RequestIDFromContext(ctx context.Context) (string, bool) {
	requestID, ok := ctx.Value(requestIDCtxKey).(string)
	return requestID, ok
}

func NewRequestIDInterceptor(logger logger.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		id, err := uuid.NewRandom()
		if err != nil {
			logger.Error("failed to generate uuid", zap.Error(err))
		}

		requestID := id.String()

		// Add the requestID to the context
		ctx = context.WithValue(ctx, requestIDCtxKey, requestID)

		// Add the requestID to the span
		trace.SpanFromContext(ctx).SetAttributes(attribute.String(requestIDTraceKey, requestID))

		// Add the requestID to the response header
		err = grpc.SetHeader(ctx, metadata.Pairs(requestIDHeader, requestID))
		if err != nil {
			logger.Error("failed to set header", zap.Error(err))
		}

		return handler(ctx, req)
	}
}

func NewStreamingRequestIDInterceptor(logger logger.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ss := newWrapperServerStream(stream)

		id, err := uuid.NewRandom()
		if err != nil {
			logger.Error("failed to generate uuid", zap.Error(err))
		}

		requestID := id.String()

		// Add the requestID to the context
		ss.wrappedContext = context.WithValue(ss.Context(), requestIDCtxKey, requestID)

		// Add the requestID to the span
		trace.SpanFromContext(ss.Context()).SetAttributes(attribute.String(requestIDTraceKey, requestID))

		// Add the requestID to the response header
		err = ss.SetHeader(metadata.Pairs(requestIDHeader, requestID))
		if err != nil {
			logger.Error("failed to set header", zap.Error(err))
		}

		return handler(srv, ss)
	}
}
