package worker

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/openfga/openfga/internal/concurrency"
)

// Terminal is a leaf worker for concrete type nodes (e.g. "user").
// It prepends the type label to each incoming identifier and broadcasts
// the fully-qualified object strings to listeners.
type Terminal struct {
	*Core
}

// ProcessMessage broadcasts each value in msg.
func (w *Terminal) ProcessMessage(ctx context.Context, index int, msg *Message) error {
	receiver := NewSliceReceiver(msg.Value)
	defer receiver.Close()
	w.Broadcast(ctx, receiver)
	return nil
}

// Execute starts a goroutine per sender to drain incoming messages.
func (w *Terminal) Execute(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "Terminal.Execute", trace.WithAttributes(
		attribute.String("worker.label", w.String()),
	))
	defer span.End()

	defer w.instrument(span)
	defer w.Cleanup()

	var wg sync.WaitGroup

	for index := range len(w.senders) {
		wg.Go(func() {
			var err error
			defer w.error(&err)
			defer concurrency.RecoverFromPanic(&err)

			w.ProcessSender(ctx, index, w)
		})
	}
	wg.Wait()
}
