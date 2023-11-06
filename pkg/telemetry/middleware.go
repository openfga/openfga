package telemetry

import (
	"context"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var _ grpc.UnaryClientInterceptor = GRPCUnaryTraceMetadataInjector

// GRPCUnaryTraceMetadataInjector is lightweight [grpc.UnaryClientInterceptor] that injects trace metadata into outgoing grpc unary call
// without wrapping the call in new span.
func GRPCUnaryTraceMetadataInjector(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.MD{}
	}
	otelgrpc.Inject(ctx, &md)
	return invoker(ctx, method, req, reply, cc, opts...)
}

var _ grpc.StreamClientInterceptor = GRPCStreamTraceMetadataInjector

// GRPCStreamTraceMetadataInjector is lightweight [grpc.StreamClientInterceptor] that injects trace metadata into outgoing grpc unary call
// without wrapping the call in new span.
func GRPCStreamTraceMetadataInjector(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.MD{}
	}
	otelgrpc.Inject(ctx, &md)
	return streamer(ctx, desc, cc, method, opts...)
}

// HTTPServerTraceExtractor is a custom middleware that extracts
// the tracing context and sets the corrent tracing context.
// It can be used in place of [otelhttp.NewHandler] to avoid creating
// additional span.
func HTTPServerTraceExtractor(h http.Handler) http.Handler {
	propagator := otel.GetTextMapPropagator()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}
