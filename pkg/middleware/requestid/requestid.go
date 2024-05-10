package requestid

import (
	"context"

	"github.com/google/uuid"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	requestIDKey      = "request_id"
	requestIDTraceKey = "request_id"

	// RequestIDHeader defines the HTTP header that is set in each HTTP response.
	// for a given request. The value of the header is unique per request.
	RequestIDHeader = "X-Request-Id"
)

// InitRequestID returns the ID to be used to identify the request.
// If tracing is enabled, returns trace ID, e.g. "1e20da43269fe07e3d2ac018c0aad2d1".
// Otherwise returns a new UUID, e.g. "38fee7ac-4bfe-4cf6-baa2-8b5ec296b485".
func InitRequestID(ctx context.Context) string {
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.TraceID().IsValid() {
		return spanCtx.TraceID().String()
	}
	id, _ := uuid.NewRandom()
	return id.String()
}

// NewUnaryInterceptor creates a grpc.UnaryServerInterceptor which must
// come after the trace interceptor and before the logging interceptor.
func NewUnaryInterceptor() grpc.UnaryServerInterceptor {
	return interceptors.UnaryServerInterceptor(reportable())
}

// NewStreamingInterceptor creates a grpc.StreamServerInterceptor which must
// come after the trace interceptor and before the logging interceptor.
func NewStreamingInterceptor() grpc.StreamServerInterceptor {
	return interceptors.StreamServerInterceptor(reportable())
}

func reportable() interceptors.CommonReportableFunc {
	return func(ctx context.Context, c interceptors.CallMeta) (interceptors.Reporter, context.Context) {
		requestID := InitRequestID(ctx)

		grpc_ctxtags.Extract(ctx).Set(requestIDKey, requestID) // CtxTags used by other middlewares

		_ = grpc.SetHeader(ctx, metadata.Pairs(RequestIDHeader, requestID))

		trace.SpanFromContext(ctx).SetAttributes(attribute.String(requestIDTraceKey, requestID))

		return interceptors.NoopReporter{}, ctx
	}
}
