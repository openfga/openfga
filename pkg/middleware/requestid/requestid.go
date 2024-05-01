package requestid

import (
	"context"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	requestIDKey      = "request_id"
	requestIDTraceKey = "request_id"

	// RequestIDHeader defines the HTTP header that is set in each HTTP response
	// for a given request. The value of the header is unique per request.
	RequestIDHeader = "X-Request-Id"
)

// InitID returns the ID to be used to identify the request.
// If trace is enabled, returns trace ID; otherwise returns a new ULID.
func InitID(ctx context.Context) string {
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.TraceID().IsValid() {
		return spanCtx.TraceID().String()
	}
	return ulid.Make().String()
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
		// TODO use ulid library?
		requestID := InitID(ctx)

		grpc_ctxtags.Extract(ctx).Set(requestIDKey, requestID) // CtxTags used by other middlewares

		_ = grpc.SetHeader(ctx, metadata.Pairs(RequestIDHeader, requestID))

		trace.SpanFromContext(ctx).SetAttributes(attribute.String(requestIDTraceKey, requestID))

		return interceptors.NoopReporter{}, ctx
	}
}
