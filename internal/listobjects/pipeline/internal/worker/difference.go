package worker

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/containers"
	"github.com/openfga/openfga/internal/containers/mpsc"
)

// SubtractingReceiver wraps a Receiver[string] and filters out any
// values present in the subtraction set.
type SubtractingReceiver struct {
	inner        Receiver[string]
	subtractions map[string]struct{}
}

func newSubtractingReceiver(inner Receiver[string], subtractions map[string]struct{}) *SubtractingReceiver {
	return &SubtractingReceiver{
		inner:        inner,
		subtractions: subtractions,
	}
}

// Recv returns the next value not in the subtraction set.
func (r *SubtractingReceiver) Recv(ctx context.Context) (string, bool) {
	for {
		value, ok := r.inner.Recv(ctx)
		if !ok {
			return "", false
		}

		if _, subtract := r.subtractions[value]; !subtract {
			return value, true
		}
	}
}

// Close releases the underlying receiver.
func (r *SubtractingReceiver) Close() {
	r.inner.Close()
}

// Difference is a worker that computes the set difference of its first
// sender (base) minus its second sender (subtract). It requires at least
// two registered senders.
//
// The subtract set is fully materialized into memory before filtering
// begins, so memory usage is proportional to the size of the subtract
// set. The base set is streamed and never fully materialized.
type Difference struct {
	base     *mpsc.Accumulator[string]
	subtract containers.Bag[string]
	*Core
}

// ProcessMessage interprets the message values through the sender's edge and
// adds the results to the adder corresponding to the sender index.
func (w *Difference) ProcessMessage(ctx context.Context, index int, msg *Message) error {
	sender := w.senders[index]
	edge := sender.Key()

	results := w.Interpreter.Interpret(ctx, edge, msg.Value)
	defer results.Close()

	var err error

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

		switch index {
		case 0:
			w.base.Send(value)
		case 1:
			w.subtract.Add(value)
		}
	}
	return err
}

// Execute processes the base and subtract senders concurrently. Once both
// complete, it broadcasts the base items that are not present in the subtract
// set. If the base sender produces no results, the context is cancelled early.
func (w *Difference) Execute(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "Difference.Execute", trace.WithAttributes(
		attribute.String("worker.label", w.String()),
	))
	defer span.End()

	defer w.instrument(span)

	defer w.Cleanup()

	if len(w.senders) < 2 {
		return
	}

	w.base = mpsc.NewAccumulator[string]()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wgBase sync.WaitGroup
	defer wgBase.Wait()

	wgBase.Go(func() {
		var err error
		defer w.base.Close()
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

	for value := range w.subtract.Seq() {
		subtractions[value] = struct{}{}
	}
	results := newSubtractingReceiver(w.base, subtractions)
	defer results.Close()

	w.Broadcast(ctx, results)
}
