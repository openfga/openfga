package pipeline

import (
	"context"

	"github.com/openfga/openfga/internal/containers"
	"github.com/openfga/openfga/internal/pipe"
)

type operatorProcessor struct {
	resolverCore
	items   pipe.Tx[Item]
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
	p.tracker.Inc()
	values := p.bufferPool.Get()
	// Copy values from message to local buffer so that the message
	// can release its buffer back to the pool.
	copy(*values, msg.Value)
	size := len(msg.Value)

	// Release message resources.
	msg.Done()

	unseen := make([]string, 0, size)

	// Only take the number of values that existed in the original
	// message. Reading beyond that will corrupt pipeline state.
	for _, item := range (*values)[:size] {
		if item.Err != nil {
			p.items.Send(item)
			continue
		}
		unseen = append(unseen, item.Value)
	}
	// When returning the buffer to the pool, its length must remain
	// unaltered, lest the chunk size no longer be respected.
	p.bufferPool.Put(values)

	results := p.interpreter.Interpret(ctx, edge, unseen)

	for item := range results {
		p.items.Send(item)
	}

	// Save the tracker decrementation for later execution in the Resolve
	// function. This is critical to ensure that resolvers sharing this
	// instance's tracker observe the appropriate count for the entirely
	// of processing.
	p.cleanup.Add(p.tracker.Dec)
}
