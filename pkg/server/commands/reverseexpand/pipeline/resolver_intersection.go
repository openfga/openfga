package pipeline

import (
	"context"
	"maps"
	"sync"

	"go.opentelemetry.io/otel/attribute"

	"github.com/openfga/openfga/internal/containers"
)

type intersectionResolver struct {
	resolverCore
}

func (r *intersectionResolver) Resolve(
	ctx context.Context,
	senders []*sender,
	listeners []*listener,
) {
	ctx, span := pipelineTracer.Start(ctx, "intersectionResolver.Resolve")
	defer span.End()

	defer r.membership.SignalReady()

	if len(senders) == 0 {
		return
	}

	var wg sync.WaitGroup

	bags := make([]txBag[string], len(senders))

	var cleanup containers.Bag[func()]

	for i, snd := range senders {
		processor := operatorProcessor{
			resolverCore: r.resolverCore,
			items:        &bags[i],
			cleanup:      &cleanup,
		}

		for range r.numProcs {
			wg.Add(1)
			go func(p operatorProcessor) {
				defer wg.Done()
				r.drain(ctx, snd, p.process)
			}(processor)
		}
	}
	wg.Wait()

	output := make(map[string]struct{})

	for value := range bags[0].Seq() {
		output[value] = struct{}{}
	}

	// Each subsequent bag narrows the candidate set.
	for i := 1; i < len(bags); i++ {
		found := make(map[string]struct{}, len(output))
		for value := range bags[i].Seq() {
			if _, ok := output[value]; ok {
				found[value] = struct{}{}
			}
		}
		output = found
	}

	sentCount := r.broadcast(maps.Keys(output), listeners)

	span.SetAttributes(attribute.Int("items.count", sentCount))

	for fn := range cleanup.Seq() {
		fn()
	}

	for _, lst := range listeners {
		lst.Close()
	}
}
