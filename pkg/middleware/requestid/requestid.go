package requestid

import (
	"context"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	requestIDCtxKey   = "request-id-context-key"
	requestIDTraceKey = "request_id"

	// RequestIDHeader defines the HTTP header that is set in each HTTP response
	// for a given request. The value of the header is unique per request.
	RequestIDHeader = "X-Request-Id"
)

// FromContext extracts the request-id from the context, if it exists.
func FromContext(ctx context.Context) (string, bool) {
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		if vals := md.Get(requestIDCtxKey); len(vals) > 0 {
			return vals[0], true
		}
	}

	return "", false
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
		id, _ := uuid.NewRandom()
		requestID := id.String()

		ctx = metadata.AppendToOutgoingContext(ctx, requestIDCtxKey, requestID)

		trace.SpanFromContext(ctx).SetAttributes(attribute.String(requestIDTraceKey, requestID))

		_ = grpc.SetHeader(ctx, metadata.Pairs(RequestIDHeader, requestID))

		return interceptors.NoopReporter{}, ctx
	}
}
