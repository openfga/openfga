package middleware

import (
	"context"

	"google.golang.org/grpc"
)

const storeIDCtxKey ctxKey = "store-id-context-key"

func StoreIDFromContext(ctx context.Context) (string, bool) {
	storeID, ok := ctx.Value(storeIDCtxKey).(string)
	return storeID, ok
}

type hasGetStoreId interface {
	GetStoreId() string
}

func NewStoreIDInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if r, ok := req.(hasGetStoreId); ok {
			ctx = context.WithValue(ctx, storeIDCtxKey, r.GetStoreId())
		}

		return handler(ctx, req)
	}
}

type WrappedServerStream struct {
	grpc.ServerStream
	WrappedContext context.Context
}

func WrapServerStream(stream grpc.ServerStream) *WrappedServerStream {
	if existing, ok := stream.(*WrappedServerStream); ok {
		return existing
	}
	return &WrappedServerStream{ServerStream: stream, WrappedContext: stream.Context()}
}

func (w *WrappedServerStream) Context() context.Context {
	return w.WrappedContext
}

func (w *WrappedServerStream) RecvMsg(m interface{}) error {
	if err := w.ServerStream.RecvMsg(m); err != nil {
		return nil
	}

	if r, ok := m.(hasGetStoreId); ok {
		w.WrappedContext = context.WithValue(w.Context(), storeIDCtxKey, r.GetStoreId())
	}

	return nil
}

func NewStreamingStoreIDInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ss := WrapServerStream(stream)

		return handler(srv, ss)
	}
}
