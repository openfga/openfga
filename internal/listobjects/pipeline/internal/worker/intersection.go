package worker

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/containers"
)

// Intersection is a worker that accumulates results from each sender into
// separate bags and then outputs only the items common to all of them.
// If any sender produces zero results, the context is cancelled early
// and the output is empty.
//
// Unlike [Basic], Intersection materializes the full result set from every
// sender into memory before computing the intersection. Memory usage is
// proportional to the total number of unique items across all senders.
type Intersection struct {
	bags []containers.AtomicMap[string, struct{}]
	*Core
}

// ProcessMessage interprets the message values through the sender's edge and
// accumulates the results into the bag corresponding to the sender index.
func (w *Intersection) ProcessMessage(ctx context.Context, index int, msg *Message) {
	sender := w.senders[index]
	bag := &w.bags[index]
	edge := sender.Key()

	results := w.Interpreter.Interpret(ctx, edge, msg.Value)
	defer results.Close()

	for {
		item, ok := results.Recv(ctx)
		if !ok {
			break
		}

		value, err := item.Object()
		if err != nil {
			w.error(&err)
			break
		}
		bag.Store(value, struct{}{})
	}
}

// Execute processes all senders concurrently, accumulating each sender's
// interpreted results into a separate bag. Once all senders complete, it
// computes the intersection of all bags and broadcasts the common items.
func (w *Intersection) Execute(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "Intersection.Execute", trace.WithAttributes(
		attribute.String("worker.label", w.String()),
	))
	defer span.End()

	defer w.instrument(span)

	defer func() {
		for i := range len(w.bags) {
			w.bags[i].Clear()
		}
		clear(w.bags)
	}()
	defer w.Cleanup()

	if len(w.senders) == 0 {
		return
	}

	var wg sync.WaitGroup

	w.bags = make([]containers.AtomicMap[string, struct{}], len(w.senders))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for index := range len(w.senders) {
		wg.Go(func() {
			var err error
			defer w.error(&err)
			defer concurrency.RecoverFromPanic(&err)

			w.ProcessSender(ctx, index, w)

			if w.bags[index].Len() == 0 {
				cancel()
			}
		})
	}
	wg.Wait()

	if ctx.Err() != nil {
		return
	}

	inputs := make([]map[string]struct{}, 0, len(w.bags)-1)

	objMin := len(w.bags[0].Unwrap())
	indexMin := 0
	for i := 1; i < len(w.bags); i++ {
		bag := w.bags[i].Unwrap()
		if len(bag) < objMin {
			inputs = append(inputs, w.bags[indexMin].Unwrap())
			indexMin = i
			objMin = len(bag)
		} else {
			inputs = append(inputs, bag)
		}
	}

	output := w.bags[indexMin].Unwrap()

	// Deleting from a map during range iteration is well-defined in Go:
	// deleted keys that have not yet been reached are skipped.
OutputLoop:
	for value := range output {
		for _, m := range inputs {
			if _, ok := m[value]; !ok {
				delete(output, value)
				continue OutputLoop
			}
		}
	}

	values := make([]string, 0, len(output))
	for value := range output {
		values = append(values, value)
	}

	results := NewSliceReceiver(values)
	defer results.Close()

	w.Broadcast(ctx, results)
}
