package pipeline

import (
	"context"
	"iter"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/openfga/openfga/internal/seq"
)

type resolver interface {
	Resolve(context.Context, []*sender, []*listener)
}

// resolverCore provides shared state for resolver implementations.
type resolverCore struct {
	interpreter interpreter
	membership  *membership
	bufferPool  *bufferPool
	errors      chan error
	numProcs    int
}

func (r *resolverCore) error(err *error) {
	if err != nil {
		r.errors <- *err
	}
}

// broadcast batches results to amortize message overhead.
func (r *resolverCore) broadcast(
	ctx context.Context,
	results iter.Seq[string],
	listeners []*listener,
) int {
	var sentCount int

	buffer := r.bufferPool.Get()
	defer r.bufferPool.Put(buffer)

	reader := seq.NewSeqReader(results)
	defer reader.Close()

	for ctx.Err() == nil {
		count := reader.Read(*buffer)

		if count == 0 {
			break
		}

		for i := range len(listeners) {
			// Each listener needs its own copy to avoid data races.
			values := r.bufferPool.Get()
			copy(*values, (*buffer)[:count])

			r.membership.Tracker().Inc()
			m := &message{
				Value:      (*values)[:count],
				buffer:     values,
				bufferPool: r.bufferPool,
				tracker:    r.membership.Tracker(),
			}

			listeners[i].C <- m
		}
		sentCount += count
	}
	return sentCount
}

func (r *resolverCore) drain(
	ctx context.Context,
	snd *sender,
	yield func(context.Context, *Edge, *message),
) {
	edge := snd.Key

	edgeTo := "nil"
	edgeFrom := "nil"

	if edge != nil {
		edgeTo = edge.GetTo().GetUniqueLabel()
		edgeFrom = edge.GetFrom().GetUniqueLabel()
	}

	attrs := []attribute.KeyValue{
		attribute.String("edge.to", edgeTo),
		attribute.String("edge.from", edgeFrom),
	}

	for msg := range snd.C {
		if ctx.Err() != nil {
			msg.Done()
			continue
		}

		var messageAttrs [3]attribute.KeyValue
		messageAttrs[0] = attribute.Int("items.count", len(msg.Value))
		copy(messageAttrs[1:], attrs)

		ctx, span := pipelineTracer.Start(
			ctx, "message.received",
			trace.WithAttributes(messageAttrs[:]...),
		)

		yield(ctx, edge, msg)
		span.End()
	}
}
