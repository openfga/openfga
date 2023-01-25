package telemetry

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func NewTracerProvider() (trace.TracerProvider, error) {
	// TODO: this still needs to be wired up and options set properly

	tp := sdktrace.NewTracerProvider()

	otel.SetTracerProvider(tp)

	return tp, nil
}

func NewNoopTracer() trace.Tracer {
	return trace.NewNoopTracerProvider().Tracer("noop")
}

func TraceError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
