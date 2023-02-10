package middleware

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

const (
	storeIDCtxKey ctxKey = "store-id-context-key"

	storeIDTraceKey = "store_id"
)

func StoreIDFromContext(ctx context.Context) (string, bool) {
	storeID, ok := ctx.Value(storeIDCtxKey).(string)
	return storeID, ok
}

type hasGetStoreID interface {
	GetStoreId() string
}

// NewStoreIDInterceptor creates a grpc.UnaryServerInterceptor which must come
// after the trace interceptor and before the logger interceptor.
func NewStoreIDInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if r, ok := req.(hasGetStoreID); ok {
			storeID := r.GetStoreId()

			// Add the storeID to the context
			ctx = context.WithValue(ctx, storeIDCtxKey, storeID)

			// Add the storeID to the span
			trace.SpanFromContext(ctx).SetAttributes(attribute.String(storeIDTraceKey, storeID))
		}

		return handler(ctx, req)
	}
}

// NewStreamingStoreIDInterceptor must come after the trace interceptor
func NewStreamingStoreIDInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, newWrappedServerStream(stream))
	}
}
