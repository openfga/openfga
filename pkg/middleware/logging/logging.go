package logging

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
)

const (
	grpcServiceKey     = "grpc_service"
	grpcMethodKey      = "grpc_method"
	grpcTypeKey        = "grpc_type"
	grpcCodeKey        = "grpc_code"
	requestIDKey       = "request_id"
	traceIDKey         = "trace_id"
	rawRequestKey      = "raw_request"
	rawResponseKey     = "raw_response"
	internalErrorKey   = "internal_error"
	grpcReqCompleteKey = "grpc_req_complete"
	userAgentKey       = "user_agent"
	queryDurationKey   = "query_duration_ms"

	gatewayUserAgentHeader string = "grpcgateway-user-agent"
	userAgentHeader        string = "user-agent"
	healthCheckService     string = "grpc.health.v1.Health"
)

// NewLoggingInterceptor creates a new logging interceptor for gRPC unary server requests.
// The requestCompleteLevel is the level used for the per-request "grpc_req_complete"
// completion log (e.g. "info" or "debug").
func NewLoggingInterceptor(logger logger.Logger, requestCompleteLevel string) grpc.UnaryServerInterceptor {
	return interceptors.UnaryServerInterceptor(reportable(logger, requestCompleteLevel))
}

// NewStreamingLoggingInterceptor creates a new streaming logging interceptor for gRPC stream server requests.
// The requestCompleteLevel is the level used for the per-request "grpc_req_complete"
// completion log (e.g. "info" or "debug").
func NewStreamingLoggingInterceptor(logger logger.Logger, requestCompleteLevel string) grpc.StreamServerInterceptor {
	return interceptors.StreamServerInterceptor(reportable(logger, requestCompleteLevel))
}

type reporter struct {
	ctx                  context.Context
	logger               logger.Logger
	fields               []zap.Field
	protomarshaler       protojson.MarshalOptions
	serviceName          string
	requestCompleteLevel zapcore.Level
}

// logRequestComplete writes the grpc_req_complete completion line at the configured level.
func (r *reporter) logRequestComplete() {
	switch r.requestCompleteLevel {
	case zapcore.DebugLevel:
		r.logger.Debug(grpcReqCompleteKey, r.fields...)
	case zapcore.WarnLevel:
		r.logger.Warn(grpcReqCompleteKey, r.fields...)
	case zapcore.ErrorLevel:
		r.logger.Error(grpcReqCompleteKey, r.fields...)
	default:
		r.logger.Info(grpcReqCompleteKey, r.fields...)
	}
}

// PostCall is invoked after all PostMsgSend operations.
func (r *reporter) PostCall(err error, rpcDuration time.Duration) {
	rpcDurationMs := strconv.FormatInt(rpcDuration.Milliseconds(), 10)

	r.fields = append(r.fields, zap.String(queryDurationKey, rpcDurationMs))
	r.fields = append(r.fields, ctxzap.TagsToFields(r.ctx)...)

	code := serverErrors.ConvertToEncodedErrorCode(status.Convert(err))
	r.fields = append(r.fields, zap.Int32(grpcCodeKey, code))

	if err != nil {
		var internalError serverErrors.InternalError
		if errors.As(err, &internalError) {
			r.fields = append(r.fields, zap.String(internalErrorKey, internalError.Unwrap().Error()))
			r.logger.Error(err.Error(), r.fields...)
		} else {
			r.fields = append(r.fields, zap.Error(err))
			r.logRequestComplete()
		}

		return
	}

	if r.serviceName == healthCheckService {
		r.logger.Debug(grpcReqCompleteKey, r.fields...)
	} else {
		r.logRequestComplete()
	}
}

// PostMsgSend is invoked once after a unary response or multiple times in
// streaming requests after each message has been sent.
func (r *reporter) PostMsgSend(msg interface{}, err error, _ time.Duration) {
	if err != nil {
		// This is the actual error that customers see.
		intCode := serverErrors.ConvertToEncodedErrorCode(status.Convert(err))
		encodedError := serverErrors.NewEncodedError(intCode, err.Error())
		protomsg := encodedError.ActualError
		if resp, err := json.Marshal(protomsg); err == nil {
			r.fields = append(r.fields, zap.Any(rawResponseKey, json.RawMessage(resp)))
		}
		return
	}
	protomsg, ok := msg.(protoreflect.ProtoMessage)
	if ok {
		if resp, err := r.protomarshaler.Marshal(protomsg); err == nil {
			r.fields = append(r.fields, zap.Any(rawResponseKey, json.RawMessage(resp)))
		}
	}
}

// PostMsgReceive is invoked after receiving a message in streaming requests.
func (r *reporter) PostMsgReceive(msg interface{}, _ error, _ time.Duration) {
	protomsg, ok := msg.(protoreflect.ProtoMessage)
	if ok {
		if req, err := r.protomarshaler.Marshal(protomsg); err == nil {
			r.fields = append(r.fields, zap.Any(rawRequestKey, json.RawMessage(req)))
		}
	}
}

// userAgentFromContext retrieves the user agent field from the provided context.
// If the user agent field is not present in the context, the function returns an empty string and false.
func userAgentFromContext(ctx context.Context) (string, bool) {
	if headers, ok := metadata.FromIncomingContext(ctx); ok {
		if header := headers.Get(gatewayUserAgentHeader); len(header) > 0 {
			return header[0], true
		}
		if header := headers.Get(userAgentHeader); len(header) > 0 {
			return header[0], true
		}
	}
	return "", false
}

func reportable(l logger.Logger, requestCompleteLevel string) interceptors.CommonReportableFunc {
	level := parseLevel(requestCompleteLevel)
	return func(ctx context.Context, c interceptors.CallMeta) (interceptors.Reporter, context.Context) {
		fields := []zap.Field{
			zap.String(grpcServiceKey, c.Service),
			zap.String(grpcMethodKey, c.Method),
			zap.String(grpcTypeKey, string(c.Typ)),
		}

		spanCtx := trace.SpanContextFromContext(ctx)
		if spanCtx.HasTraceID() {
			fields = append(fields, zap.String(traceIDKey, spanCtx.TraceID().String()))
		}

		if userAgent, ok := userAgentFromContext(ctx); ok {
			fields = append(fields, zap.String(userAgentKey, userAgent))
		}

		return &reporter{
			ctx:                  ctx,
			logger:               l,
			fields:               fields,
			protomarshaler:       protojson.MarshalOptions{EmitUnpopulated: true},
			serviceName:          c.Service,
			requestCompleteLevel: level,
		}, ctx
	}
}

// parseLevel maps a configured level string to a zap level, defaulting to info.
func parseLevel(level string) zapcore.Level {
	switch level {
	case "debug":
		return zapcore.DebugLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}
