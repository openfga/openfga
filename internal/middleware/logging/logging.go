package logging

import (
	"context"
	"encoding/json"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"github.com/openfga/openfga/internal/middleware/requestid"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
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
	modelIDKey         = "authorization_model_id"
	rawRequestKey      = "raw_request"
	rawResponseKey     = "raw_response"
	internalErrorKey   = "internal_error"
	grpcReqCompleteKey = "grpc_req_complete"
)

type hasGetStoreID interface {
	GetStoreId() string
}

type hasGetAuthorizationModelId interface {
	GetAuthorizationModelId() string
}

func NewLoggingInterceptor(logger logger.Logger) grpc.UnaryServerInterceptor {
	return interceptors.UnaryServerInterceptor(reportable(logger))
}

func NewStreamingLoggingInterceptor(logger logger.Logger) grpc.StreamServerInterceptor {
	return interceptors.StreamServerInterceptor(reportable(logger))
}

type reporter struct {
	ctx    context.Context
	logger logger.Logger
	fields []zap.Field
}

func (r *reporter) PostCall(err error, _ time.Duration) {
	code := serverErrors.ConvertToEncodedErrorCode(status.Convert(err))
	r.fields = append(r.fields, zap.Int32(grpcCodeKey, code))

	if err != nil {
		if internalError, ok := err.(serverErrors.InternalError); ok {
			r.fields = append(r.fields, zap.String(internalErrorKey, internalError.Internal().Error()))
		}

		if isInternalError(code) {
			r.logger.Error(err.Error(), r.fields...)
		} else {
			r.fields = append(r.fields, zap.Error(err))
			r.logger.Info(grpcReqCompleteKey, r.fields...)
		}

		return
	}

	r.logger.Info(grpcReqCompleteKey, r.fields...)
}

func (r *reporter) PostMsgSend(msg interface{}, err error, _ time.Duration) {
	if resp, err := json.Marshal(msg); err == nil {
		r.fields = append(r.fields, zap.Any(rawResponseKey, json.RawMessage(resp)))
	}
}

func (r *reporter) PostMsgReceive(msg interface{}, _ error, _ time.Duration) {
	if m, ok := msg.(hasGetStoreID); ok {
		r.fields = append(r.fields, zap.String(storeIDKey, m.GetStoreId()))
	}

	if m, ok := msg.(hasGetAuthorizationModelId); ok {
		r.fields = append(r.fields, zap.String(modelIDKey, m.GetAuthorizationModelId()))
	}

	if req, err := json.Marshal(msg); err == nil {
		r.fields = append(r.fields, zap.Any(rawRequestKey, json.RawMessage(req)))
	}
}

func reportable(logger logger.Logger) interceptors.CommonReportableFunc {
	return func(ctx context.Context, c interceptors.CallMeta, isClient bool) (interceptors.Reporter, context.Context) {
		fields := []zap.Field{
			zap.String(grpcServiceKey, c.Service),
			zap.String(grpcMethodKey, c.Method),
			zap.String(grpcTypeKey, string(c.Typ)),
		}

		spanCtx := trace.SpanContextFromContext(ctx)
		if spanCtx.HasTraceID() {
			fields = append(fields, zap.String(traceIDKey, spanCtx.TraceID().String()))
		}

		if requestID, ok := requestid.FromContext(ctx); ok {
			fields = append(fields, zap.String(requestIDKey, requestID))
		}

		return &reporter{
			ctx:    ctx,
			logger: logger,
			fields: fields,
		}, ctx
	}
}

func isInternalError(code int32) bool {
	if code >= 4000 && code < 5000 {
		return true
	}
	return false
}
