package pipeline

import (
	"context"
	"iter"

	"github.com/openfga/openfga/internal/seq"
	"github.com/openfga/openfga/pkg/server/commands/reverseexpand/pipeline/track"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// resolver is an interface that is consumed by a worker struct.
// A resolver is responsible for consuming messages from a worker's
// senders and broadcasting the result of processing the consumed
// messages to the worker's listeners.
type resolver interface {

	// resolve is a function that consumes messages from the
	// provided senders, and broadcasts the results of processing
	// the consumed messages to the provided listeners.
	Resolve(context.Context, []*sender, []*listener)
}

type resolverCore struct {
	// interpreter is an `interpreter` that transforms a sender's input into output which it
	// broadcasts to all of the parent worker's listeners.
	interpreter interpreter

	// tracker may be owned or shared with other resolvers. It is used to report messages from
	// this resolver that are still in-flight.
	tracker *track.Tracker

	// reporter may be owned or shared with other resolvers. It is used to report on the status of
	// the resolver.
	reporter *track.Reporter

	// bufferPool is intended to be shared with other resolvers. It is used to manage a pool of
	// Item slices so that additional allocations can be avoided.
	bufferPool *bufferPool

	// numProcs indicates the number of goroutines to spawn for processing each sender.
	numProcs int
}

func (r *resolverCore) broadcast(
	results iter.Seq[Item],
	listeners []*listener,
) int {
	var sentCount int

	// Grab a buffer from the pool for reading. The buffer's size
	// is set by the bufferPool.
	buffer := r.bufferPool.Get()
	reader := seq.NewSeqReader(results)

	for {
		count := reader.Read(*buffer)

		if count == 0 {
			// No more values to read from the iter.Seq.
			break
		}

		for i := range len(listeners) {
			// Grab a buffer that will be specific to this message.
			values := r.bufferPool.Get()
			// This copy is done so that the content of the message buffer is
			// not altered when the read buffer has new values written to it.
			copy(*values, (*buffer)[:count])

			// Increment the resolver's tracker to account for the new message.
			r.tracker.Inc()
			m := &message{
				// Only slice the values in the read buffer up to what was actually read.
				Value: (*values)[:count],

				// Stored for cleanup
				buffer:     values,
				bufferPool: r.bufferPool,
				tracker:    r.tracker,
			}

			if !listeners[i].Send(m) {
				// If the message was not sent, we need to release the message resources.
				m.Done()
			}
		}
		sentCount += count
	}
	reader.Close()

	// Release the read buffer back to the buffer pool.
	r.bufferPool.Put(buffer)

	return sentCount
}

func (r *resolverCore) drain(
	ctx context.Context,
	snd *sender,
	yield func(context.Context, *Edge, *message),
) {
	edge := snd.Key()

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

	var msg *message

	for snd.Recv(&msg) {
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
