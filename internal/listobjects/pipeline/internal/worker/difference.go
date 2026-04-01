package worker

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/containers"
	"github.com/openfga/openfga/internal/containers/mpsc"
	"github.com/openfga/openfga/internal/seq"
)

// Adder is a generic interface for appending values to a collection.
type Adder[T any] interface {
	Add(...T)
}

// AccumulatorAdder adapts an [mpsc.Accumulator] to satisfy the [Adder] interface.
type AccumulatorAdder[T any] mpsc.Accumulator[T]

// Add sends each value to the underlying accumulator.
func (a *AccumulatorAdder[T]) Add(values ...T) {
	for _, value := range values {
		(*mpsc.Accumulator[T])(a).Send(value)
	}
}

// Difference is a worker that computes the set difference of its first
// sender (base) minus its second sender (subtract). It requires at least
// two registered senders.
//
// The subtract set is fully materialized into memory before filtering
// begins, so memory usage is proportional to the size of the subtract
// set. The base set is streamed and never fully materialized.
type Difference struct {
	adders []Adder[string]
	*Core
}

// ProcessMessage interprets the message values through the sender's edge and
// adds the results to the adder corresponding to the sender index.
func (w *Difference) ProcessMessage(ctx context.Context, index int, msg *Message) {
	sender := w.senders[index]
	edge := sender.Key()
	adder := w.adders[index]

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
		adder.Add(value)
	}
}

// Execute processes the base and subtract senders concurrently. Once both
// complete, it broadcasts the base items that are not present in the subtract
// set. If the base sender produces no results, the context is cancelled early.
func (w *Difference) Execute(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "Difference.Execute", trace.WithAttributes(
		attribute.String("worker.label", w.String()),
	))
	defer span.End()

	defer w.Cleanup()

	if len(w.senders) < 2 {
		return
	}

	w.adders = make([]Adder[string], len(w.senders))

	base := mpsc.NewAccumulator[string]()
	w.adders[0] = (*AccumulatorAdder[string])(base)

	var subtract containers.Bag[string]
	w.adders[1] = &subtract

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wgBase sync.WaitGroup
	defer wgBase.Wait()

	wgBase.Go(func() {
		var err error
		defer base.Close()
		defer w.error(&err)
		defer concurrency.RecoverFromPanic(&err)

		w.ProcessSender(ctx, 0, w)

		if w.stats[0].SumObjectsReceived == 0 {
			cancel()
		}
	})

	var wgSubtract sync.WaitGroup

	wgSubtract.Go(func() {
		var err error
		defer w.error(&err)
		defer concurrency.RecoverFromPanic(&err)

		w.ProcessSender(ctx, 1, w)
	})

	wgSubtract.Wait()

	if ctx.Err() != nil {
		return
	}

	subtractions := make(map[string]struct{})

	for value := range subtract.Seq() {
		subtractions[value] = struct{}{}
	}

	output := seq.Filter(base.Seq(ctx), func(value string) bool {
		_, ok := subtractions[value]
		return !ok
	})

	w.Broadcast(ctx, output)
}
