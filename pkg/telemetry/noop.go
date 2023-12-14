package telemetry

import (
	"context"
	"sync"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
	"go.opentelemetry.io/otel/trace/noop"
)

type noopTracerProvider struct {
	embedded.TracerProvider

	once sync.Once
	tp   trace.TracerProvider
}

func (t *noopTracerProvider) Tracer(name string, options ...trace.TracerOption) trace.Tracer {
	t.once.Do(func() {
		t.tp = noop.NewTracerProvider()
	})

	return t.tp.Tracer(name, options...)
}

func (t *noopTracerProvider) Close(_ context.Context) error {
	return nil
}

func (t *noopTracerProvider) RegisterSpanProcessor(_ sdktrace.SpanProcessor) {
}

func Noop() TracerProvider {
	return &noopTracerProvider{}
}
