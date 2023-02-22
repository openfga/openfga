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
	requestIDHeader   = "X-Request-Id"
)

func RequestIDFromContext(ctx context.Context) (string, bool) {
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		if vals := md.Get(requestIDCtxKey); len(vals) > 0 {
			return vals[0], true
		}
	}
	return "", false
}

// NewRequestIDInterceptor creates a grpc.UnaryServerInterceptor which must
// come after the trace interceptor and before the logging interceptor.
func NewRequestIDInterceptor() grpc.UnaryServerInterceptor {
	return interceptors.UnaryServerInterceptor(reportable())
}

// NewStreamingRequestIDInterceptor creates a grpc.StreamServerInterceptor
// which must come after the trace interceptor and before the logging
// interceptor.
func NewStreamingRequestIDInterceptor() grpc.StreamServerInterceptor {
	return interceptors.StreamServerInterceptor(reportable())
}

func reportable() interceptors.CommonReportableFunc {
	return func(ctx context.Context, c interceptors.CallMeta, isClient bool) (interceptors.Reporter, context.Context) {
		id, _ := uuid.NewRandom()
		requestID := id.String()

		// Add the requestID to the context
		ctx = metadata.AppendToOutgoingContext(ctx, requestIDCtxKey, requestID)

		// Add the requestID to the span
		trace.SpanFromContext(ctx).SetAttributes(attribute.String(requestIDTraceKey, requestID))

		// Add the requestID to the response headers
		_ = grpc.SetHeader(ctx, metadata.Pairs(requestIDHeader, requestID))

		return interceptors.NoopReporter{}, ctx
	}
}
