package pipeline

import (
	"context"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"

	"github.com/openfga/openfga/internal/containers"
	"github.com/openfga/openfga/internal/containers/mpsc"
	"github.com/openfga/openfga/internal/seq"
)

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

	var excluded containers.Bag[string]

	var cleanup containers.Bag[func()]

	var wgExclude sync.WaitGroup

	// Include side streams through an Accumulator; exclude side collects into a bag.
	included := mpsc.NewAccumulator[string]()

	var counter atomic.Int32
	counter.Store(int32(r.numProcs))

	processorInclude := operatorProcessor{
		resolverCore: r.resolverCore,
		items:        included,
		cleanup:      &cleanup,
	}

	for range r.numProcs {
		go func() {
			defer func() {
				// Last goroutine closes the pipe.
				if counter.Add(-1) < 1 {
					included.Close()
				}
			}()
			r.drain(ctx, senders[0], processorInclude.process)
		}()
	}

	processorExclude := operatorProcessor{
		resolverCore: r.resolverCore,
		items:        &excluded,
		cleanup:      &cleanup,
	}

	for range r.numProcs {
		wgExclude.Add(1)
		go func() {
			defer wgExclude.Done()
			r.drain(ctx, senders[1], processorExclude.process)
		}()
	}

	wgExclude.Wait()

	exclusions := make(map[string]struct{})

	for value := range excluded.Seq() {
		exclusions[value] = struct{}{}
	}

	results := seq.Filter(included.Seq(), func(value string) bool {
		_, ok := exclusions[value]
		return !ok
	})

	sentCount := r.broadcast(ctx, results, listeners)

	span.SetAttributes(attribute.Int("items.count", sentCount))

	for fn := range cleanup.Seq() {
		fn()
	}

	for _, lst := range listeners {
		lst.Close()
	}
}
