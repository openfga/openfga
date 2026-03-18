package pipeline

import (
	"context"

	"github.com/openfga/openfga/internal/containers"
)

type Tx interface {
	Add(...string)
}

type ChanTx struct {
	c chan<- string
}

func (c *ChanTx) Send(s string) {
	c.c <- s
}

// operatorProcessor collects all items for set operations (intersection, exclusion).
type operatorProcessor struct {
	resolverCore
	items   Tx
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
		p.items.Add(value)
	}

	p.bufferPool.Put(values)

	// Defer decrement until set operation completes to prevent premature shutdown.
	p.cleanup.Add(p.membership.Tracker().Dec)
}
