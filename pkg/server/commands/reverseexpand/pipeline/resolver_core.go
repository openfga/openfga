package pipeline

import (
	"context"
	"iter"
	"time"

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
	if err != nil && *err != nil {
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
	var sumIdleNanoseconds int64
	var maxIdleNanoseconds int64
	var sumActiveNanoseconds int64
	var maxActiveNanoseconds int64
	var sumMessages int64
	var sumItems int64
	var maxItems int64

	edge := snd.Key

	edgeTo := "nil"
	edgeFrom := "nil"
	edgeType := "nil"

	if edge != nil {
		edgeTo = edge.GetTo().GetUniqueLabel()
		edgeFrom = edge.GetFrom().GetUniqueLabel()

		switch edge.GetEdgeType() {
		case edgeTypeDirect:
			edgeType = "direct"
		case edgeTypeComputed:
			edgeType = "computed"
		case edgeTypeRewrite:
			edgeType = "rewrite"
		case edgeTypeTTU:
			edgeType = "ttu"
		case edgeTypeDirectLogical:
			edgeType = "direct_logical"
		case edgeTypeTTULogical:
			edgeType = "ttu_logical"
		default:
			edgeType = "unknown"
		}
	}

	const (
		labelEdgeTo            string = "edge.to"
		labelEdgeFrom          string = "edge.from"
		labelEdgeType          string = "edge.type"
		labelIdleDurationSum   string = "idle.duration.sum"
		labelIdleDurationMax   string = "idle.duration.max"
		labelActiveDurationSum string = "active.duration.sum"
		labelActiveDurationMax string = "active.duration.max"
		labelMessagesSum       string = "messages.sum"
		labelItemsSum          string = "items.sum"
		labelItemsMax          string = "items.max"
	)

	ctx, span := pipelineTracer.Start(
		ctx, "sender.drain",
		trace.WithAttributes(
			attribute.String(labelEdgeTo, edgeTo),
			attribute.String(labelEdgeFrom, edgeFrom),
			attribute.String(labelEdgeType, edgeType),
			attribute.Int64(labelIdleDurationSum, sumIdleNanoseconds),
			attribute.Int64(labelIdleDurationMax, maxIdleNanoseconds),
			attribute.Int64(labelActiveDurationSum, sumActiveNanoseconds),
			attribute.Int64(labelActiveDurationMax, maxActiveNanoseconds),
			attribute.Int64(labelMessagesSum, sumMessages),
			attribute.Int64(labelItemsSum, sumItems),
			attribute.Int64(labelItemsMax, maxItems),
		),
	)

	idleStart := time.Now()
	for msg := range snd.C {
		elapsedIdleNanoseconds := time.Since(idleStart).Nanoseconds()
		maxIdleNanoseconds = max(maxIdleNanoseconds, elapsedIdleNanoseconds)
		sumIdleNanoseconds += elapsedIdleNanoseconds
		activeStart := time.Now()

		sumMessages++
		maxItems = max(maxItems, int64(len(msg.Value)))
		sumItems += int64(len(msg.Value))

		if ctx.Err() != nil {
			msg.Done()
			elapsedActiveNanoseconds := time.Since(activeStart).Nanoseconds()
			maxActiveNanoseconds = max(maxActiveNanoseconds, elapsedActiveNanoseconds)
			sumActiveNanoseconds += elapsedActiveNanoseconds
			idleStart = time.Now()
			continue
		}

		yield(ctx, edge, msg)
		elapsedActiveNanoseconds := time.Since(activeStart).Nanoseconds()
		maxActiveNanoseconds = max(maxActiveNanoseconds, elapsedActiveNanoseconds)
		sumActiveNanoseconds += elapsedActiveNanoseconds
		idleStart = time.Now()
	}

	span.SetAttributes(
		attribute.Int64(labelIdleDurationSum, sumIdleNanoseconds),
		attribute.Int64(labelIdleDurationMax, maxIdleNanoseconds),
		attribute.Int64(labelActiveDurationSum, sumActiveNanoseconds),
		attribute.Int64(labelActiveDurationMax, maxActiveNanoseconds),
		attribute.Int64(labelMessagesSum, sumMessages),
		attribute.Int64(labelItemsSum, sumItems),
		attribute.Int64(labelItemsMax, maxItems),
	)
	span.End()
}
