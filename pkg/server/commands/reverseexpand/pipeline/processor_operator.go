package pipeline

import (
	"context"

	"github.com/openfga/openfga/internal/containers"
	"github.com/openfga/openfga/internal/pipe"
)

// operatorProcessor collects all items for set operations (intersection, exclusion).
type operatorProcessor struct {
	resolverCore
	items   pipe.Tx[Object]
	cleanup *containers.Bag[func()]
}

func (p *operatorProcessor) process(ctx context.Context, edge *Edge, msg *message) {
	p.membership.Tracker().Inc()

	// Copy to local buffer so we can release the message immediately.
	values := p.bufferPool.Get()
	copy(*values, msg.Value)
	size := len(msg.Value)
	msg.Done()

	unseen := make([]string, 0, size)

	for _, obj := range (*values)[:size] {
		value, err := obj.Object()

		if err != nil {
			p.items.Send(obj)
			continue
		}
		unseen = append(unseen, value)
	}
	p.bufferPool.Put(values)

	results := p.interpreter.Interpret(ctx, edge, unseen)

	for item := range results {
		p.items.Send(item)
	}

	// Defer decrement until set operation completes to prevent premature shutdown.
	p.cleanup.Add(p.membership.Tracker().Dec)
}
