package worker

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/containers"
)

// Preprocessor filters or transforms a batch of values before they are
// passed to the [Interpreter].
type Preprocessor interface {
	Process([]string, []string) []string
}

// DeduplicatingProcessor tracks previously seen values and filters duplicates
// across multiple calls to [DeduplicatingProcessor.Process]. It is safe for
// concurrent use.
type DeduplicatingProcessor struct {
	m containers.AtomicMap[string, struct{}]
}

// Process returns only the values that have not been seen in any previous call.
func (d *DeduplicatingProcessor) Process(values []string, buffer []string) []string {
	for _, value := range values {
		if _, ok := d.m.LoadOrStore(value, struct{}{}); !ok {
			buffer = append(buffer, value)
		}
	}
	return buffer
}

// IdentityProcessor is a [Preprocessor] that returns values unmodified.
type IdentityProcessor struct{}

// Process returns values unchanged.
func (p *IdentityProcessor) Process(values []string, _ []string) []string {
	return values
}

// DefaultPreprocessor is an [IdentityProcessor] used when no deduplication
// is required.
var DefaultPreprocessor Preprocessor = &IdentityProcessor{}

// Basic is a worker that passes interpreted results through to its listeners,
// deduplicating output across all senders. It handles both standard (non-cyclical)
// and cyclical edges, coordinating cycle termination through its [Membership]
// when present.
type Basic struct {
	Membership    *Membership
	outputBuffer  containers.AtomicMap[string, struct{}]
	preprocessors []Preprocessor
	*Core
}

// DeduplicatingReceiver wraps a Receiver[Item] and suppresses values
// that have already been emitted by the owning Basic worker, ensuring
// each unique object identifier is broadcast at most once across all
// senders.
type DeduplicatingReceiver struct {
	inner  Receiver[Item]
	output *containers.AtomicMap[string, struct{}]
	err    error
}

func (w *Basic) newDeduplicatingReceiver(inner Receiver[Item], output *containers.AtomicMap[string, struct{}]) *DeduplicatingReceiver {
	return &DeduplicatingReceiver{
		inner:  inner,
		output: output,
	}
}

// Recv returns the next unique value, skipping duplicates and errors.
func (r *DeduplicatingReceiver) Recv(ctx context.Context) (string, bool) {
	for {
		item, ok := r.inner.Recv(ctx)
		if !ok {
			return "", false
		}

		value, err := item.Object()
		if err != nil {
			r.err = err
			return "", false
		}

		if _, loaded := r.output.LoadOrStore(value, struct{}{}); !loaded {
			return value, true
		}
	}
}

func (r *DeduplicatingReceiver) Err() error {
	return r.err
}

// Close releases the underlying receiver.
func (r *DeduplicatingReceiver) Close() {
	r.inner.Close()
}

// ProcessMessage interprets the message values through the sender's edge,
// deduplicates the results against all other senders, and broadcasts any
// new values to downstream listeners.
func (w *Basic) ProcessMessage(ctx context.Context, index int, msg *Message) error {
	sender := w.senders[index]
	edge := sender.Key()

	buffer, ok := ctx.Value(BufferKey).([]string)
	if !ok || buffer == nil {
		buffer = make([]string, w.ChunkSize)
	}
	buffer = buffer[:0]

	values := w.preprocessors[index].Process(msg.Value, buffer)

	results := w.Interpreter.Interpret(ctx, edge, values)
	defer results.Close()

	output := w.newDeduplicatingReceiver(results, &w.outputBuffer)

	w.Broadcast(ctx, output)

	return output.Err()
}

// Execute processes all registered senders concurrently. Standard senders run
// in parallel goroutines. Cyclical senders run in their own goroutines and
// are terminated once all standard senders have been exhausted and the
// in-flight message count across the cycle group reaches zero.
//
// After quiescence is detected, the leader initiates an ordered teardown:
// each member closes its listeners and wakes the next member in the ring.
// This ensures downstream channels are closed in dependency order so that
// cyclical ProcessSender goroutines drain and exit cleanly.
func (w *Basic) Execute(ctx context.Context) {
	membership := "nil"
	if w.Membership != nil {
		membership = w.Membership.String()
	}
	ctx, span := tracer.Start(ctx, "Basic.Execute", trace.WithAttributes(
		attribute.String("worker.label", w.String()),
		attribute.String("worker.membership", membership),
	))
	defer span.End()

	defer w.instrument(span)

	defer w.Cleanup()

	if len(w.senders) == 0 {
		return
	}

	w.preprocessors = make([]Preprocessor, len(w.senders))

	defer w.outputBuffer.Clear()

	var wgStandard sync.WaitGroup
	var wgRecursive sync.WaitGroup

	for index, sender := range w.senders {
		edge := sender.Key()
		cyclical := IsCyclical(edge)

		if cyclical {
			wgRecursive.Go(func() {
				var err error
				defer w.error(&err)
				defer concurrency.RecoverFromPanic(&err)

				var d DeduplicatingProcessor
				w.preprocessors[index] = &d
				w.ProcessSender(ctx, index, w)
			})
			continue
		}

		wgStandard.Go(func() {
			var err error
			defer w.error(&err)
			defer concurrency.RecoverFromPanic(&err)

			w.preprocessors[index] = DefaultPreprocessor
			w.ProcessSender(ctx, index, w)
		})
	}

	wgStandard.Wait()

	defer wgRecursive.Wait()

	if w.Membership != nil {
		w.Membership.SignalReady()

		// When the context is canceled, WaitForAllReady should still wait for
		// all members of the cycle group to enter a ready state. Therefore,
		// we use [context.Background] here.
		w.Membership.WaitForAllReady(context.Background())

		// Ordered teardown: the leader starts the cascade immediately;
		// non-leaders wait to be woken by their predecessor. Each member
		// closes its listeners (unblocking the next member's cyclical
		// Recv calls) and then wakes the next member.
		if w.Membership.IsLeader() {
			w.Cleanup()
			w.Membership.Next().Wake()
			return
		}

		// When the context is canceled, sleep should still wait for the upstream
		// worker to finish. Therefore, we use [context.Background] here.
		w.Membership.Sleep(context.Background())
		w.Cleanup()
		w.Membership.Next().Wake()
	}
}
