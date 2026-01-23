package pipeline

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/openfga/openfga/internal/containers"
	"github.com/openfga/openfga/internal/seq"
	"go.opentelemetry.io/otel/attribute"
)

// baseProcessor processes messages from a single sender for baseResolver.
// Handles deduplication at two levels: input (per-sender) and output (per-worker).
type baseProcessor struct {
	resolverCore
	listeners    []*listener
	inputBuffer  *containers.AtomicMap[string, struct{}]
	outputBuffer *containers.AtomicMap[string, struct{}]
	SentCount    int64
}

// process is a function that reads output from a single sender, processes the output through an
// interpreter, and then sends the interpreter's output to each listener. The sender's output is
// deduplicated across all process functions for the same edge when the sender is for a cyclical
// edge because values of a cycle may be reentrant. Output from the interpreter is always
// deduplicated across all process functions because input from two different senders may produce
// the same output value(s).
func (p *baseProcessor) process(ctx context.Context, edge *Edge, msg *message) {
	errs := make([]Item, 0, len(msg.Value))
	unseen := make([]string, 0, len(msg.Value))

	for _, item := range msg.Value {
		if item.Err != nil {
			errs = append(errs, item)
			continue
		}

		if p.inputBuffer != nil {
			// Deduplicate cyclical input to prevent infinite loops.
			// LoadOrStore returns loaded=true if value already existed; we only process
			// new values to avoid reprocessing items that cycle back to this sender.
			if _, loaded := p.inputBuffer.LoadOrStore(item.Value, struct{}{}); !loaded {
				unseen = append(unseen, item.Value)
			}
			continue
		}
		unseen = append(unseen, item.Value)
	}

	results := p.interpreter.Interpret(ctx, edge, unseen)

	// Combine the initial errors with the interpreted output.
	results = seq.Flatten(seq.Sequence(errs...), results)

	results = seq.Filter(results, func(item Item) bool {
		if item.Err != nil {
			return true
		}

		// Deduplicate the interpreted values using the buffer shared by all processors
		// of all senders.
		_, loaded := p.outputBuffer.LoadOrStore(item.Value, struct{}{})
		return !loaded
	})

	p.SentCount += int64(p.broadcast(results, p.listeners))

	// Release the received message's resources.
	msg.Done()
}

// baseResolver is a struct that implements the Resolver interface and acts as the standard
// resolver for most workers. A baseResolver handles both recursive and non-recursive edges
// concurrently.
type baseResolver struct {
	resolverCore
}

// Resolve is a function that orchestrates the processing of all sender output, broadcasting the
// result of that processing to all listeners. The Resolve function will block until all of its
// non-cyclical senders have been exhausted and the status of the instance's reporter is equal to
// `true` and the count of its tracker reaches `0`.
func (r *baseResolver) Resolve(
	ctx context.Context,
	senders []*sender,
	listeners []*listener,
) {
	ctx, span := pipelineTracer.Start(ctx, "baseResolver.Resolve")
	defer span.End()

	// This output buffer is shared across all processors of all senders, and is used
	// for output deduplication.
	var outputBuffer containers.AtomicMap[string, struct{}]
	defer outputBuffer.Clear()

	var sentCount atomic.Int64

	// Any senders with a non-recursive edge will be processed in the "standard" queue.
	var wgStandard sync.WaitGroup

	// Any senders with a recursive edge will be processed in the "recursive" queue.
	var wgRecursive sync.WaitGroup

	for _, snd := range senders {
		edge := snd.Key()
		isCyclical := isCyclical(edge)

		if isCyclical {
			// Cyclical edges can send the same value multiple times as it flows through the cycle.
			// Input buffer prevents reprocessing the same value from this specific sender.
			// Shared across all processors of this sender to deduplicate across goroutines.
			var inputBuffer containers.AtomicMap[string, struct{}]

			for range r.numProcs {
				processor := baseProcessor{
					resolverCore: r.resolverCore,
					listeners:    listeners,
					inputBuffer:  &inputBuffer,
					outputBuffer: &outputBuffer,
				}

				wgRecursive.Add(1)
				go func() {
					defer wgRecursive.Done()
					r.drain(ctx, snd, processor.process)
					sentCount.Add(processor.SentCount)
				}()
			}
			continue
		}

		// The sender's edge is not recursive or part of a tuple cycle.
		for range r.numProcs {
			processor := baseProcessor{
				resolverCore: r.resolverCore,
				listeners:    listeners,
				inputBuffer:  nil,
				outputBuffer: &outputBuffer,
			}

			wgStandard.Add(1)
			go func() {
				defer wgStandard.Done()
				r.drain(ctx, snd, processor.process)
				sentCount.Add(processor.SentCount)
			}()
		}
	}

	// All standard senders are guaranteed to end at some point.
	wgStandard.Wait()

	// Now that all standard senders have been exhausted, this resolver is ready
	// to end once all other resolvers that are part of the same cycle are ready.
	r.membership.SignalReady()

	// Wait for all related resolvers' status to be set to `false`.
	r.membership.WaitForAllReady()

	// Wait until all messages from related resolvers have finished processing.
	r.membership.WaitForDrain()

	// Close all listeners to release any processors that are stuck on a full
	// listener buffer. Without this, an early termination could cause a deadlock
	// when the listener's internal buffer remains full.
	for _, lst := range listeners {
		lst.Close()
	}

	// Ensure that all recursive processors have ended.
	wgRecursive.Wait()

	span.SetAttributes(attribute.Int64("items.count", sentCount.Load()))
}
