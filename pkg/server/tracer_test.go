package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/openfga/openfga/pkg/storage/memory"
)

func TestTracerProviderDefaultsToGlobal(t *testing.T) {
	datastore := memory.New()
	t.Cleanup(datastore.Close)

	s := MustNewServerWithOpts(WithDatastore(datastore))
	t.Cleanup(s.Close)

	require.Equal(t, otel.GetTracerProvider(), s.tracerProvider)
}

func TestWithTracerProvider(t *testing.T) {
	spanRecorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	t.Cleanup(func() {
		require.NoError(t, tracerProvider.Shutdown(context.Background()))
	})

	s, req := setupCheckServer(t, "", nil, WithTracerProvider(tracerProvider))

	_, err := s.Check(context.Background(), req)
	require.NoError(t, err)

	instrumentationScopes := make(map[string]struct{})
	for _, span := range spanRecorder.Ended() {
		instrumentationScopes[span.InstrumentationScope().Name] = struct{}{}
	}

	require.Contains(t, instrumentationScopes, "openfga/pkg/server")
	require.Contains(t, instrumentationScopes, "openfga/pkg/typesystem")
	require.Contains(t, instrumentationScopes, "internal/graph/check")
}

func TestWithTracerProviderIsInstanceScoped(t *testing.T) {
	firstRecorder := tracetest.NewSpanRecorder()
	firstProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(firstRecorder))
	t.Cleanup(func() {
		require.NoError(t, firstProvider.Shutdown(context.Background()))
	})

	secondRecorder := tracetest.NewSpanRecorder()
	secondProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(secondRecorder))
	t.Cleanup(func() {
		require.NoError(t, secondProvider.Shutdown(context.Background()))
	})

	firstServer, firstRequest := setupCheckServer(t, "", nil, WithTracerProvider(firstProvider))
	secondServer, secondRequest := setupCheckServer(t, "", nil, WithTracerProvider(secondProvider))

	firstSpanCount := len(firstRecorder.Ended())
	secondSpanCount := len(secondRecorder.Ended())

	_, err := firstServer.Check(context.Background(), firstRequest)
	require.NoError(t, err)
	require.Greater(t, len(firstRecorder.Ended()), firstSpanCount)
	require.Len(t, secondRecorder.Ended(), secondSpanCount)
	firstSpanCountAfterFirst := len(firstRecorder.Ended())

	_, err = secondServer.Check(context.Background(), secondRequest)
	require.NoError(t, err)
	require.Greater(t, len(secondRecorder.Ended()), secondSpanCount)
	require.Len(t, firstRecorder.Ended(), firstSpanCountAfterFirst)
}

func TestWithTracerProviderRejectsNil(t *testing.T) {
	datastore := memory.New()
	t.Cleanup(datastore.Close)

	_, err := NewServerWithOpts(
		WithDatastore(datastore),
		WithTracerProvider(nil),
	)

	require.EqualError(t, err, "tracer provider must not be nil")
}
