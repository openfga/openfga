package telemetry

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func NewTracerProvider() (trace.TracerProvider, error) {
	// At some point this will be updated with a non-noop tracer provider.

	tp := trace.NewNoopTracerProvider()

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
