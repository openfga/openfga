package telemetry

import (
	"go.opentelemetry.io/otel/trace"
)

func NewNoopTracer() trace.Tracer {
	return trace.NewNoopTracerProvider().Tracer("noop")
}
