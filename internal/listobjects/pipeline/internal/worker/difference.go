package worker

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/containers"
	"github.com/openfga/openfga/internal/containers/mpsc"
	"github.com/openfga/openfga/internal/seq"
)

// Adder is a generic interface for appending values to a collection.
type Adder[T any] interface {
	Add(...T)
}

// AccumulatorAdder adapts an [mpsc.Accumulator] to satisfy the [Adder] interface.
type AccumulatorAdder[T any] mpsc.Accumulator[T]

// Add sends each value to the underlying accumulator.
func (a *AccumulatorAdder[T]) Add(values ...T) {
	for _, value := range values {
		(*mpsc.Accumulator[T])(a).Send(value)
	}
}

// Difference is a worker that computes the set difference of its first
// sender (base) minus its second sender (subtract). It requires at least
// two registered senders.
type Difference struct {
	*Core
}

// ProcessSender reads messages from the given sender, interprets them,
// and appends the results to adder. It returns the total number of items
// accumulated.
func (w *Difference) ProcessSender(ctx context.Context, sender Sender, adder Adder[string]) int64 {
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
					adder.Add(value)
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

	return count.Load()
}

// Execute processes the base and subtract senders concurrently. Once both
// complete, it broadcasts the base items that are not present in the subtract
// set. If the base sender produces no results, the context is cancelled early.
func (w *Difference) Execute(ctx context.Context) {
	defer w.Cleanup()

	if len(w.senders) < 2 {
		return
	}

	base := mpsc.NewAccumulator[string]()

	var subtract containers.Bag[string]

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wgBase sync.WaitGroup
	defer wgBase.Wait()

	wgBase.Add(1)
	go func() {
		var err error
		defer wgBase.Done()
		defer base.Close()
		defer DrainSender(w.senders[0])
		defer w.error(&err)
		defer concurrency.RecoverFromPanic(&err)
		if w.ProcessSender(ctx, w.senders[0], (*AccumulatorAdder[string])(base)) == 0 {
			cancel()
		}
	}()

	var wgSubtract sync.WaitGroup

	wgSubtract.Add(1)
	go func() {
		var err error
		defer wgSubtract.Done()
		defer DrainSender(w.senders[1])
		defer w.error(&err)
		defer concurrency.RecoverFromPanic(&err)
		w.ProcessSender(ctx, w.senders[1], &subtract)
	}()

	wgSubtract.Wait()

	if ctx.Err() != nil {
		return
	}

	subtractions := make(map[string]struct{})

	for value := range subtract.Seq() {
		subtractions[value] = struct{}{}
	}

	output := seq.Filter(base.Seq(ctx), func(value string) bool {
		_, ok := subtractions[value]
		return !ok
	})

	w.Broadcast(ctx, output)
}
