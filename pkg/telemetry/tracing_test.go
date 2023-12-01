package telemetry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

func TestTracing(t *testing.T) {
	options := []TracerOption{
		WithAttributes(
			semconv.ServiceNameKey.String("servicename"),
			semconv.ServiceVersionKey.String("0.0.0"),
		),
		WithSamplingRatio(1),
	}

	tp := MustNewTracerProvider(options...)

	spanRecorder := tracetest.NewSpanRecorder()
	tp.RegisterSpanProcessor(spanRecorder)

	_, span := tp.Tracer("").Start(context.Background(), "test")
	span.End()

	spans := spanRecorder.Ended()
	require.Equal(t, 1, len(spans))
	require.Equal(t, "test", spans[0].Name())
}
