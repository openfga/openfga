package telemetry

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
)

func TestTracerOptions(t *testing.T) {
	tracer := &customTracer{}

	WithOTLPEndpoint("collector:4317")(tracer)
	require.Equal(t, "collector:4317", tracer.endpoint)

	WithOTLPInsecure()(tracer)
	require.True(t, tracer.insecure)

	WithSampler("always_on")(tracer)
	require.Equal(t, "always_on", tracer.sampler)

	WithSamplingRatio(0.42)(tracer)
	require.Equal(t, 0.42, tracer.samplingRatio)

	attrs := []attribute.KeyValue{attribute.String("service.name", "openfga")}
	WithAttributes(attrs...)(tracer)
	require.Equal(t, attrs, tracer.attributes)
}

func TestParseOTLPEndpoint_EdgeCases(t *testing.T) {
	t.Run("unknown_scheme_returned_unchanged", func(t *testing.T) {
		endpoint, secure := ParseOTLPEndpoint("unix://socket:1234")
		require.Equal(t, "unix://socket:1234", endpoint)
		require.False(t, secure)
	})

	t.Run("empty_string", func(t *testing.T) {
		endpoint, secure := ParseOTLPEndpoint("")
		require.Equal(t, "", endpoint)
		require.False(t, secure)
	})

	t.Run("control_char_unparseable_uri", func(t *testing.T) {
		// A URL with a control character fails url.Parse and is returned as-is.
		endpoint, secure := ParseOTLPEndpoint("http://\x7f-bad")
		require.Equal(t, "http://\x7f-bad", endpoint)
		require.False(t, secure)
	})
}

func TestMustNewTracerProvider(t *testing.T) {
	// Stand up a local gRPC listener so the blocking OTLP exporter dial succeeds.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	tp := MustNewTracerProvider(
		WithOTLPEndpoint(lis.Addr().String()),
		WithOTLPInsecure(),
		WithSampler("always_on"),
		WithSamplingRatio(1.0),
		WithAttributes(attribute.String("service.name", "test")),
	)
	require.NotNil(t, tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
	})

	var _ *sdktrace.TracerProvider = tp
}

func TestTraceError_ContextCanceled(t *testing.T) {
	// noop span: TraceError on a context.Canceled error should be a no-op and not panic.
	_, span := noop.NewTracerProvider().Tracer("test").Start(context.Background(), "op")
	TraceError(span, context.Canceled)
	TraceError(span, errors.New("boom"))
}
