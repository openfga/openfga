package pipeline

import (
	"context"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"

	"github.com/openfga/openfga/internal/containers"
	"github.com/openfga/openfga/internal/pipe"
	"github.com/openfga/openfga/internal/seq"
)

// exclusionResolver is a struct that resolves senders to an exclusion operation.
type exclusionResolver struct {
	resolverCore
}

func (r *exclusionResolver) Resolve(
	ctx context.Context,
	senders []*sender,
	listeners []*listener,
) {
	ctx, span := pipelineTracer.Start(ctx, "exclusionResolver.Resolve")
	defer span.End()

	defer r.membership.SignalReady()

	if len(senders) != 2 {
		panic("exclusion resolver requires two senders")
	}

	var excluded containers.Bag[Item]

	var cleanup containers.Bag[func()]

	var wgExclude sync.WaitGroup

	// Exclusion streams "include" side through a pipe while "exclude" side collects into a bag.
	// Pipe auto-extends to accommodate streaming include results without blocking.
	pipeInclude := pipe.Must[Item](pipe.Config{
		Capacity:      1 << 7,
		ExtendAfter:   0,  // Extend immediately when full to prevent blocking include side
		MaxExtensions: -1, // Unbounded growth adapts to result set size
	})

	// Track active goroutines processing include side to know when to close the pipe.
	var counter atomic.Int32
	counter.Store(int32(r.numProcs))

	processorInclude := operatorProcessor{
		resolverCore: r.resolverCore,
		items:        pipeInclude,
		cleanup:      &cleanup,
	}

	for range r.numProcs {
		go func(p operatorProcessor) {
			defer func() {
				// Last goroutine to finish closes the pipe to signal completion.
				// Atomic decrement ensures exactly one goroutine closes the pipe.
				if counter.Add(-1) < 1 {
					_ = pipeInclude.Close()
				}
			}()
			r.drain(ctx, senders[0], p.process)
		}(processorInclude)
	}

	processorExclude := operatorProcessor{
		resolverCore: r.resolverCore,
		items:        (*txBag[Item])(&excluded),
		cleanup:      &cleanup,
	}

	for range r.numProcs {
		wgExclude.Add(1)
		go func(p operatorProcessor) {
			defer wgExclude.Done()
			r.drain(ctx, senders[1], p.process)
		}(processorExclude)
	}

	wgExclude.Wait()

	var errs []Item

	exclusions := make(map[string]struct{})

	for item := range excluded.Seq() {
		if item.Err != nil {
			errs = append(errs, item)
			continue
		}
		exclusions[item.Value] = struct{}{}
	}

	results := seq.Filter(pipeInclude.Seq(), func(item Item) bool {
		if item.Err != nil {
			return true
		}

		_, ok := exclusions[item.Value]
		return !ok
	})

	results = seq.Flatten(seq.Sequence(errs...), results)

	sentCount := r.broadcast(results, listeners)

	span.SetAttributes(attribute.Int("items.count", sentCount))

	for fn := range cleanup.Seq() {
		fn()
	}

	for _, lst := range listeners {
		lst.Close()
	}
}
