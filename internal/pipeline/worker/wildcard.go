package worker

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Wildcard is a leaf worker for wildcard type nodes (e.g. "user:*").
// It broadcasts its own label as the sole result, representing the
// wildcard match for the type.
type Wildcard struct {
	*Core
}

// Execute broadcasts the wildcard label and cleans up.
func (w *Wildcard) Execute(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "Wildcard.Execute", trace.WithAttributes(
		attribute.String("worker.label", w.String()),
	))
	defer span.End()

	defer w.instrument(span)
	defer w.Cleanup()

	results := NewValueReceiver(w.Label)
	defer results.Close()

	w.Broadcast(ctx, results)
}
