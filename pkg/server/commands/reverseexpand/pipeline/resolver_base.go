package pipeline

import (
	"context"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"

	"github.com/openfga/openfga/internal/containers"
	"github.com/openfga/openfga/internal/seq"
)

// baseProcessor handles two-level deduplication: input (per-sender) and output (per-worker).
type baseProcessor struct {
	resolverCore
	listeners    []*listener
	inputBuffer  *containers.AtomicMap[string, struct{}]
	outputBuffer *containers.AtomicMap[string, struct{}]
	SentCount    int64
}

func (p *baseProcessor) process(ctx context.Context, edge *Edge, msg *message) {
	errs := make([]Item, 0, len(msg.Value))
	unseen := make([]string, 0, len(msg.Value))

	for _, obj := range msg.Value {
		value, err := obj.Object()

		if err != nil {
			errs = append(errs, obj)
			continue
		}

		if p.inputBuffer != nil {
			// Deduplicate cyclical input to prevent infinite loops.
			// LoadOrStore returns loaded=true if value already existed; we only process
			// new values to avoid reprocessing items that cycle back to this sender.
			if _, loaded := p.inputBuffer.LoadOrStore(value, struct{}{}); !loaded {
				unseen = append(unseen, value)
			}
			continue
		}
		unseen = append(unseen, value)
	}

	results := p.interpreter.Interpret(ctx, edge, unseen)

	results = seq.Flatten(seq.Sequence(errs...), results)

	results = seq.Filter(results, func(obj Item) bool {
		value, err := obj.Object()
		if err != nil {
			return true
		}

		// Deduplicate the interpreted values using the buffer shared by all processors
		// of all senders.
		_, loaded := p.outputBuffer.LoadOrStore(value, struct{}{})
		return !loaded
	})

	p.SentCount += int64(p.broadcast(results, p.listeners))

	msg.Done()
}

// baseResolver handles both recursive and non-recursive edges concurrently.
type baseResolver struct {
	resolverCore
}

func (r *baseResolver) Resolve(
	ctx context.Context,
	senders []*sender,
	listeners []*listener,
) {
	ctx, span := pipelineTracer.Start(ctx, "baseResolver.Resolve")
	defer span.End()

	// Shared across all processors to deduplicate output.
	var outputBuffer containers.AtomicMap[string, struct{}]
	defer outputBuffer.Clear()

	var sentCount atomic.Int64
	var wgStandard sync.WaitGroup
	var wgRecursive sync.WaitGroup

	for _, snd := range senders {
		edge := snd.Key()
		isCyclical := isCyclical(edge)

		if isCyclical {
			// Input buffer prevents infinite loops from cyclical edges.
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

	wgStandard.Wait()

	// Coordinate shutdown with other resolvers in the same cycle.
	r.membership.SignalReady()
	r.membership.WaitForAllReady()
	r.membership.WaitForDrain()

	// Unblock processors stuck on full listener buffers.
	for _, lst := range listeners {
		lst.Close()
	}

	wgRecursive.Wait()

	span.SetAttributes(attribute.Int64("items.count", sentCount.Load()))
}
