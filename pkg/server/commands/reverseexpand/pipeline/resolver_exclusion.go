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

	// Include side streams through a pipe; exclude side collects into a bag.
	pipeInclude := pipe.Must[Item](pipe.Config{
		Capacity:      1 << 7,
		ExtendAfter:   0,
		MaxExtensions: -1,
	})

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
				// Last goroutine closes the pipe.
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

	for obj := range excluded.Seq() {
		value, err := obj.Object()
		if err != nil {
			errs = append(errs, obj)
			continue
		}
		exclusions[value] = struct{}{}
	}

	results := seq.Filter(pipeInclude.Seq(), func(obj Item) bool {
		value, err := obj.Object()
		if err != nil {
			return true
		}

		_, ok := exclusions[value]
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
