package worker

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/pkg/tuple"
)

type DifferenceDirectSubtract struct {
	Subtracts         []*Edge
	SubjectType       string
	SubjectIdentifier string
	*Core
}

// ProcessMessage interprets the message values through the sender's edge and
// chunks the results to the worker's listeners.
func (w *DifferenceDirectSubtract) ProcessMessage(ctx context.Context, index int, msg *Message) error {
	sender := w.senders[index]
	senderEdge := sender.Key()

	results := w.Interpreter.Interpret(ctx, senderEdge, msg.Value)
	defer results.Close()

	var err error

	var objects []string

SubtractLoop:
	for {
		item, ok := results.Recv(ctx)
		if !ok {
			break
		}

		var value string
		value, err = item.Object()
		if err != nil {
			break
		}

		for _, subtractEdge := range w.Subtracts {
			subject := tuple.BuildObject(w.SubjectType, w.SubjectIdentifier)

			var exists bool

			exists, err = w.Interpreter.Exists(ctx, subtractEdge, value, subject)
			if err != nil {
				break SubtractLoop
			}

			if exists {
				// base object is in the subtract set; do not store the object.
				continue SubtractLoop
			}
		}

		// base object is not in the subtract set, store the object.
		objects = append(objects, value)
		if len(objects) == w.ChunkSize {
			// chunk limit reached, broadcast the collected objects to all
			// of this worker's listeners.
			w.send(ctx, objects)
			objects = objects[:0]
		}
	}

	if len(objects) > 0 {
		// objects remain unsent, broadcast the remaining objects to all
		// of this worker's listeners.
		w.send(ctx, objects)
	}
	return err
}

func (w *DifferenceDirectSubtract) Execute(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "DifferenceDirectSubtract.Execute", trace.WithAttributes(
		attribute.String("worker.label", w.String()),
		attribute.Int("worker.direct_edge_count", len(w.Subtracts)),
	))
	defer span.End()

	defer w.instrument(span)

	defer w.Cleanup()

	if len(w.senders) != 1 {
		panic("difference direct subtract worker requires a single sender")
	}

	var err error

	defer w.error(&err)
	defer concurrency.RecoverFromPanic(&err)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	w.ProcessSender(ctx, 0, w)
}
