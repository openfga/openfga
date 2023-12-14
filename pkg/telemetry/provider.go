package telemetry

import (
	"context"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
)

type TracerProvider interface {
	trace.TracerProvider

	Close(context.Context) error
	RegisterSpanProcessor(sdktrace.SpanProcessor)
}

type tracerProvider struct {
	embedded.TracerProvider

	tp *sdktrace.TracerProvider
}

func (t *tracerProvider) Tracer(name string, options ...trace.TracerOption) trace.Tracer {
	return t.tp.Tracer(name, options...)
}

func (t *tracerProvider) Close(ctx context.Context) error {
	if t.tp != nil {
		if err := t.tp.ForceFlush(ctx); err != nil {
			return err
		}

		if err := t.tp.Shutdown(ctx); err != nil {
			return err
		}

		t.tp = nil
	}
	return nil
}

func (t *tracerProvider) RegisterSpanProcessor(spanProcessor sdktrace.SpanProcessor) {
	t.tp.RegisterSpanProcessor(spanProcessor)
}
