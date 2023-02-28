package middleware

import (
	"context"
	"encoding/json"

	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

const (
	grpcServiceKey     = "grpc_service"
	grpcMethodKey      = "grpc_method"
	grpcTypeKey        = "grpc_type"
	grpcCodeKey        = "grpc_code"
	requestIDKey       = "request_id"
	traceIDKey         = "trace_id"
	storeIDKey         = "store_id"
	rawRequestKey      = "raw_request"
	rawResponseKey     = "raw_response"
	internalErrorKey   = "internal_error"
	grpcReqCompleteKey = "grpc_req_complete"
)

func NewLoggingInterceptor(logger logger.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		fields := []zap.Field{
			zap.String(grpcServiceKey, openfgapb.OpenFGAService_ServiceDesc.ServiceName),
			zap.String(grpcMethodKey, info.FullMethod),
			zap.String(grpcTypeKey, "unary"),
		}

		if requestID, ok := RequestIDFromContext(ctx); ok {
			fields = append(fields, zap.String(requestIDKey, requestID))
		}

		spanCtx := trace.SpanContextFromContext(ctx)
		if spanCtx.HasTraceID() {
			fields = append(fields, zap.String(traceIDKey, spanCtx.TraceID().String()))
		}

		if storeID, ok := StoreIDFromContext(ctx); ok {
			fields = append(fields, zap.String(storeIDKey, storeID))
		}

		if jsonReq, err := json.Marshal(req); err == nil {
			fields = append(fields, zap.Any(rawRequestKey, json.RawMessage(jsonReq)))
		}

		resp, err := handler(ctx, req)

		code := serverErrors.ConvertToEncodedErrorCode(status.Convert(err))
		fields = append(fields, zap.Int32(grpcCodeKey, code))

		if err != nil {
			if internalError, ok := err.(serverErrors.InternalError); ok {
				fields = append(fields, zap.String(internalErrorKey, internalError.Internal().Error()))
			}

			if isInternalError(code) {
				logger.Error(err.Error(), fields...)
			} else {
				fields = append(fields, zap.Error(err))
				logger.Info(grpcReqCompleteKey, fields...)
			}

			return nil, err
		}

		if jsonResp, err := json.Marshal(resp); err == nil {
			fields = append(fields, zap.Any(rawResponseKey, json.RawMessage(jsonResp)))
		}

		logger.Info(grpcReqCompleteKey, fields...)

		return resp, nil
	}
}

func NewStreamingLoggingInterceptor(logger logger.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		fields := []zap.Field{
			zap.String(grpcServiceKey, openfgapb.OpenFGAService_ServiceDesc.ServiceName),
			zap.String(grpcMethodKey, info.FullMethod),
			zap.String(grpcTypeKey, "server_stream"),
		}

		ss := newWrappedServerStream(stream)

		if requestID, ok := RequestIDFromContext(ss.Context()); ok {
			fields = append(fields, zap.String(requestIDKey, requestID))
		}

		spanCtx := trace.SpanContextFromContext(ss.Context())
		if spanCtx.HasTraceID() {
			fields = append(fields, zap.String(traceIDKey, spanCtx.TraceID().String()))
		}

		if storeID, ok := StoreIDFromContext(ss.Context()); ok {
			fields = append(fields, zap.String(storeIDKey, storeID))
		}

		err := handler(srv, ss)

		// Add any fields pulled from RecvMsg
		fields = append(fields, ss.fields...)

		code := serverErrors.ConvertToEncodedErrorCode(status.Convert(err))
		fields = append(fields, zap.Int32(grpcCodeKey, code))

		if err != nil {
			if internalError, ok := err.(serverErrors.InternalError); ok {
				fields = append(fields, zap.String(internalErrorKey, internalError.Internal().Error()))
			}

			if isInternalError(code) {
				logger.Error(err.Error(), fields...)
			} else {
				fields = append(fields, zap.Error(err))
				logger.Info(grpcReqCompleteKey, fields...)
			}

			return err
		}

		logger.Info(grpcReqCompleteKey, fields...)

		return nil
	}
}

func isInternalError(code int32) bool {
	if code >= 4000 && code < 5000 {
		return true
	}
	return false
}
