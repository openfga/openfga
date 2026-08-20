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
	edge := sender.Key()

	results := w.Interpreter.Interpret(ctx, edge, msg.Value)
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

		for _, edge := range w.Subtracts {
			typeAndRelation := edge.GetRelationDefinition()

			objectType, relation := tuple.SplitObjectRelation(typeAndRelation)

			if objectType != tuple.GetType(value) {
				err = ErrUnexpectedObjectType
				break SubtractLoop
			}

			userType := edge.GetTo().GetUniqueLabel()

			subject := tuple.BuildObject(w.SubjectType, w.SubjectIdentifier)

			if tuple.IsTypedWildcard(userType) {
				subject = userType
				userType = tuple.GetType(userType)
			}

			if w.SubjectType != userType {
				err = ErrUnexpectedUserType
				break SubtractLoop
			}

			item := w.Interpreter.Get(ctx, value, relation, subject, edge.GetConditions())
			if item != nil {
				_, err = item.Object()
				if err != nil {
					break SubtractLoop
				}
				// base object is in the subtract set
				continue SubtractLoop
			}
		}

		// base object is not in the subtract set
		objects = append(objects, value)
		if len(objects) == w.ChunkSize {
			w.send(ctx, objects)
			clear(objects)
			objects = objects[:0]
		}
	}

	if len(objects) > 0 {
		w.send(ctx, objects)
		clear(objects)
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
