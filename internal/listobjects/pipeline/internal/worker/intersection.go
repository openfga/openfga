package worker

import (
	"context"
	"maps"
	"sync"
	"sync/atomic"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/containers"
)

// Intersection is a worker that accumulates results from each sender into
// separate bags and then outputs only the items common to all of them.
// If any sender produces zero results, the context is cancelled early
// and the output is empty.
type Intersection struct {
	bags []containers.Bag[string]
	*Core
}

// ProcessSender reads messages from the sender at the given index, interprets
// them, and accumulates the results into the corresponding bag. It returns
// the total number of items accumulated, or zero if the sender or context
// produced no results.
func (w *Intersection) ProcessSender(ctx context.Context, ndx int) int64 {
	sender := w.senders[ndx]

	bag := &w.bags[ndx]
	edge := sender.Key()

	input := make(chan *Message)

	var count atomic.Int64

	var wg sync.WaitGroup
	for range w.NumProcs {
		wg.Add(1)
		go func(ctx context.Context) {
			var err error
			var localCount int64
			var msg *Message
			defer wg.Done()
			defer func() {
				count.Add(localCount)
			}()
			defer func() {
				if msg != nil {
					msg.Done()
				}
			}()
			defer w.error(&err)
			defer concurrency.RecoverFromPanic(&err)

			for msg = range input {
				results := w.Interpreter.Interpret(ctx, edge, msg.Value)

				for item := range results {
					if ctx.Err() != nil {
						break
					}

					value, err := item.Object()
					if err != nil {
						w.error(&err)
						break
					}
					bag.Add(value)
					localCount++
				}
				msg.Done()
			}
		}(ctx)
	}

	// Pull values out of the sender and queue them for processing.
MessageLoop:
	for {
		msg, ok := sender.Recv(ctx)
		if !ok {
			break
		}

		select {
		case input <- msg:
		case <-ctx.Done():
			msg.Done()
			break MessageLoop
		}
	}

	// No additional input will be retrieved from the sender
	// at this point.
	close(input)

	// Wait for all processors to complete.
	wg.Wait()

	if ctx.Err() != nil {
		// At this point we don't want the intersection to
		// have any results, because it could be wrong.
		bag.Clear()
	}
	return count.Load()
}

// Execute processes all senders concurrently, accumulating each sender's
// interpreted results into a separate bag. Once all senders complete, it
// computes the intersection of all bags and broadcasts the common items.
func (w *Intersection) Execute(ctx context.Context) {
	defer w.Cleanup()

	if len(w.senders) == 0 {
		return
	}

	var wg sync.WaitGroup

	w.bags = make([]containers.Bag[string], len(w.senders))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for ndx := range len(w.senders) {
		wg.Add(1)
		go func(ndx int) {
			var err error
			defer wg.Done()
			defer w.error(&err)
			defer concurrency.RecoverFromPanic(&err)
			defer DrainSender(w.senders[ndx])
			if w.ProcessSender(ctx, ndx) == 0 {
				cancel()
			}
		}(ndx)
	}
	wg.Wait()

	if ctx.Err() != nil {
		return
	}

	output := make(map[string]struct{})

	for value := range w.bags[0].Seq() {
		output[value] = struct{}{}
	}

	for i := 1; i < len(w.bags); i++ {
		found := make(map[string]struct{}, len(output))
		for value := range w.bags[i].Seq() {
			if _, ok := output[value]; ok {
				found[value] = struct{}{}
			}
		}
		output = found
	}

	w.Broadcast(ctx, maps.Keys(output))
}
