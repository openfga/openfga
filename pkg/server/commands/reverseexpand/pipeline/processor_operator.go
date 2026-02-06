package pipeline

import (
	"context"

	"github.com/openfga/openfga/internal/containers"
	"github.com/openfga/openfga/internal/pipe"
)

// operatorProcessor collects all items for set operations (intersection, exclusion).
type operatorProcessor struct {
	resolverCore
	items   pipe.Tx[string]
	cleanup *containers.Bag[func()]
}

func (p *operatorProcessor) process(ctx context.Context, edge *Edge, msg *message) {
	p.membership.Tracker().Inc()

	// Copy to local buffer so we can release the message immediately.
	values := p.bufferPool.Get()
	copy(*values, msg.Value)
	size := len(msg.Value)
	msg.Done()

	results := p.interpreter.Interpret(ctx, edge, (*values)[:size])

	for item := range results {
		value, err := item.Object()
		if err != nil {
			p.error(err)
			continue
		}
		p.items.Send(value)
	}

	p.bufferPool.Put(values)

	// Defer decrement until set operation completes to prevent premature shutdown.
	p.cleanup.Add(p.membership.Tracker().Dec)
}
