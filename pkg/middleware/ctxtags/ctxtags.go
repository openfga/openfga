package ctxtags

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// UnaryServerInterceptor returns a new unary server interceptors that sets the values for request tags.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		newCtx := newTagsForCtx(ctx)
		return handler(newCtx, req)
	}
}

// StreamServerInterceptor returns a new streaming server interceptor that sets the values for request tags.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		newCtx := newTagsForCtx(stream.Context())
		// Short-circuit, don't do the expensive bit of allocating a wrappedStream.
		wrappedStream := wrapServerStream(stream)
		wrappedStream.WrappedContext = newCtx
		return handler(srv, wrappedStream)
	}
}

func newTagsForCtx(ctx context.Context) context.Context {
	t := NewTags()
	if peer, ok := peer.FromContext(ctx); ok {
		t.Set("peer.address", peer.Addr.String())
	}
	return SetInContext(ctx, t)
}

// WrappedServerStream is a thin wrapper around grpc.ServerStream that allows modifying context.
type WrappedServerStream struct {
	grpc.ServerStream
	// WrappedContext is the wrapper's own Context. You can assign it.
	WrappedContext context.Context
}

// Context returns the wrapper's WrappedContext, overwriting the nested grpc.ServerStream.Context().
func (w *WrappedServerStream) Context() context.Context {
	return w.WrappedContext
}

// WrapServerStream returns a ServerStream that has the ability to overwrite context.
func wrapServerStream(stream grpc.ServerStream) *WrappedServerStream {
	if existing, ok := stream.(*WrappedServerStream); ok {
		return existing
	}
	return &WrappedServerStream{ServerStream: stream, WrappedContext: stream.Context()}
}
