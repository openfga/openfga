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

// NewStoreIDInterceptor must come after the trace interceptor
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

type wrappedStoreIDServerStream struct {
	grpc.ServerStream
	WrappedContext context.Context
}

func (w *wrappedStoreIDServerStream) Context() context.Context {
	return w.WrappedContext
}

func (w *wrappedStoreIDServerStream) RecvMsg(m interface{}) error {
	if err := w.ServerStream.RecvMsg(m); err != nil {
		return nil
	}

	if r, ok := m.(hasGetStoreID); ok {
		storeID := r.GetStoreId()

		// Add the storeID to the context
		w.WrappedContext = context.WithValue(w.Context(), storeIDCtxKey, storeID)

		// Add the storeID to the span
		trace.SpanFromContext(w.Context()).SetAttributes(attribute.String(storeIDTraceKey, storeID))
	}

	return nil
}

// NewStreamingStoreIDInterceptor must come after the trace interceptor
func NewStreamingStoreIDInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ss := &wrappedStoreIDServerStream{ServerStream: stream, WrappedContext: stream.Context()}

		return handler(srv, ss)
	}
}
