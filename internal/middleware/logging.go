package middleware

import (
	"context"
	"encoding/json"

	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	methodKey       = "method"
	requestIDKey    = "request_id"
	traceIDKey      = "trace_id"
	rawRequestKey   = "raw_request"
	rawResponseKey  = "raw_response"
	publicErrorKey  = "public_error"
	grpcErrorKey    = "grpc_error"
	grpcCompleteKey = "grpc_complete"
)

func NewLoggingInterceptor(logger logger.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		fields := []zap.Field{zap.String(methodKey, info.FullMethod)}

		if requestID, ok := RequestIDFromContext(ctx); ok {
			fields = append(fields, zap.String(requestIDKey, requestID))
		}

		spanCtx := trace.SpanContextFromContext(ctx)
		if spanCtx.HasTraceID() {
			fields = append(fields, zap.String(traceIDKey, spanCtx.TraceID().String()))
		}

		jsonReq, err := json.Marshal(req)
		if err == nil {
			fields = append(fields, zap.Any(rawRequestKey, json.RawMessage(jsonReq)))
		}

		resp, err := handler(ctx, req)

		if err != nil {
			if internalError, ok := err.(serverErrors.InternalError); ok {
				fields = append(fields, zap.Error(internalError.Internal()))
			}

			fields = append(fields, zap.String(publicErrorKey, err.Error()))
			logger.Error(grpcErrorKey, fields...)

			return nil, err
		}

		jsonResp, err := json.Marshal(resp)
		if err == nil {
			fields = append(fields, zap.Any(rawResponseKey, json.RawMessage(jsonResp)))
		}

		logger.Info(grpcCompleteKey, fields...)

		return resp, nil
	}
}

func NewStreamingLoggingInterceptor(logger logger.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		fields := []zap.Field{zap.String(methodKey, info.FullMethod)}

		if requestID, ok := RequestIDFromContext(stream.Context()); ok {
			fields = append(fields, zap.String(requestIDKey, requestID))
		}

		spanCtx := trace.SpanContextFromContext(stream.Context())
		if spanCtx.HasTraceID() {
			fields = append(fields, zap.String(traceIDKey, spanCtx.TraceID().String()))
		}

		err := handler(srv, stream)

		if err != nil {
			if internalError, ok := err.(serverErrors.InternalError); ok {
				fields = append(fields, zap.Error(internalError.Internal()))
			}

			fields = append(fields, zap.String(publicErrorKey, err.Error()))
			logger.Error(grpcErrorKey, fields...)

			return err
		}

		logger.Info(grpcCompleteKey, fields...)

		return nil
	}
}
