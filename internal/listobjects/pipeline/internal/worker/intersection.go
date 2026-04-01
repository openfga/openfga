package worker

import (
	"context"
	"maps"
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
func (w *Intersection) ProcessMessage(ctx context.Context, ndx int, msg *Message) {
	sender := w.senders[ndx]
	bag := &w.bags[ndx]
	edge := sender.Key()

	results := w.Interpreter.Interpret(ctx, edge, msg.Value)

	for item := range results {
		if ctx.Err() != nil {
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

	for ndx := range len(w.senders) {
		wg.Go(func() {
			var err error
			defer w.error(&err)
			defer concurrency.RecoverFromPanic(&err)

			w.ProcessSender(ctx, ndx, w)

			if w.stats[ndx].SumObjectsReceived == 0 {
				cancel()
			}
		})
	}
	wg.Wait()

	if ctx.Err() != nil {
		return
	}

	inputs := make([]map[string]struct{}, 0, len(w.bags)-1)

	objMin := w.stats[0].SumObjectsReceived
	ndxMin := 0
	for i, stats := range w.stats {
		if stats.SumObjectsReceived < objMin {
			inputs = append(inputs, w.bags[ndxMin].Unwrap())
			ndxMin = i
			objMin = stats.SumObjectsReceived
		} else {
			inputs = append(inputs, w.bags[i].Unwrap())
		}
	}

	output := w.bags[ndxMin].Unwrap()

OutputLoop:
	for value := range output {
		for _, m := range inputs {
			if _, ok := m[value]; !ok {
				delete(output, value)
				continue OutputLoop
			}
		}
	}

	w.Broadcast(ctx, maps.Keys(output))
}
