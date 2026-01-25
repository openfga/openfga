package pipeline

import (
	"context"

	"github.com/openfga/openfga/internal/containers"
	"github.com/openfga/openfga/internal/pipe"
)

// operatorProcessor processes messages for set operation resolvers.
// Unlike baseProcessor which streams results immediately, operatorProcessor collects
// all items for the resolver to compute set operations (intersection, exclusion).
type operatorProcessor struct {
	resolverCore
	items   pipe.Tx[Object]
	cleanup *containers.Bag[func()]
}

// process is a function that processes the output of a single sender through an interpreter. All
// output from the interpreter is collected in the items pipe.Tx and a cleanup Bag is used to
// collect all resource cleaning functions for later use by the Resolve function. As soon as a
// message is received from the sender, its values are swapped to a buffer that is local to the
// current iteration, and the message resources are released immediately. Messages are not sent to
// the listeners at this point.
func (p *operatorProcessor) process(ctx context.Context, edge *Edge, msg *message) {
	// Increment the tracker to account for an in-flight message.
	p.membership.Tracker().Inc()
	values := p.bufferPool.Get()
	// Copy to local buffer to allow immediate message cleanup.
	// Operator resolvers need message data to remain valid until all senders complete;
	// copying lets us release the message buffer early while preserving the data.
	copy(*values, msg.Value)
	size := len(msg.Value)

	// Release message resources.
	msg.Done()

	unseen := make([]string, 0, size)

	// Only take the number of values that existed in the original
	// message. Reading beyond that will corrupt pipeline state.
	for _, obj := range (*values)[:size] {
		value, err := obj.Object()

		if err != nil {
			p.items.Send(obj)
			continue
		}
		unseen = append(unseen, value)
	}
	// Buffer length must match pool allocation size; changing it would break
	// subsequent Get() calls. The pool relies on uniform buffer sizes for reuse.
	p.bufferPool.Put(values)

	results := p.interpreter.Interpret(ctx, edge, unseen)

	for item := range results {
		p.items.Send(item)
	}

	// Defer tracker decrementation until after set operation completes.
	// Operator resolvers need accurate tracker counts to know when all inputs have arrived;
	// decrementing too early would trigger premature shutdown before computing the set operation.
	p.cleanup.Add(p.membership.Tracker().Dec)
}
