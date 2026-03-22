package worker

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/containers"
	"github.com/openfga/openfga/internal/seq"
)

var white Color = White
var black Color = Black

// Deduplicator tracks previously seen values and filters duplicates
// across multiple calls to [Deduplicator.Deduplicate]. It is safe for
// concurrent use.
type Deduplicator struct {
	m containers.AtomicMap[string, struct{}]
}

// Deduplicate returns only the values that have not been seen in any
// previous call. The returned slice preserves the order of first occurrence.
func (d *Deduplicator) Deduplicate(values []string) []string {
	unseen := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := d.m.LoadOrStore(value, struct{}{}); !ok {
			unseen = append(unseen, value)
		}
	}
	return unseen
}

// Identity returns values unmodified. It serves as a no-op preprocessor
// for [Basic.ProcessSender] when deduplication is not needed.
func Identity(values []string) []string {
	return values
}

// Basic is a worker that passes interpreted results through to its listeners,
// deduplicating output across all senders. It handles both standard (non-cyclical)
// and cyclical edges, coordinating cycle termination through its [Membership]
// when present.
type Basic struct {
	Membership   *Membership
	outputBuffer containers.AtomicMap[string, struct{}]
	color        atomic.Pointer[Color]
	*Core
}

// Broadcast marks the worker as active (color [Black]) and then delegates
// to [Core.Broadcast].
func (w *Basic) Broadcast(ctx context.Context, values iter.Seq[string]) {
	w.color.Store(&black)
	w.Core.Broadcast(ctx, values)
}

// ProcessSender reads messages from the sender at the given index,
// interprets them, deduplicates the results, and broadcasts output to
// listeners. It spawns NumProcs goroutines for parallel interpretation.
func (w *Basic) ProcessSender(ctx context.Context, index int, preprocess func([]string) []string) {
	sender := w.senders[index]
	edge := sender.Key()

	input := make(chan *Message)

	var wg sync.WaitGroup

	for range w.NumProcs {
		wg.Add(1)
		go func(ctx context.Context) {
			var err error
			var msg *Message
			defer wg.Done()
			defer func() {
				if msg != nil {
					msg.Done()
				}
			}()
			defer w.error(&err)
			defer concurrency.RecoverFromPanic(&err)

			for msg = range input {
				values := preprocess(msg.Value)
				results := w.Interpreter.Interpret(ctx, edge, values)
				results = seq.Filter(results, func(obj Item) bool {
					value, err := obj.Object()
					if err != nil {
						w.error(&err)
						return false
					}

					// Deduplicate the interpreted values using the buffer shared by all processors
					// of all senders.
					_, loaded := w.outputBuffer.LoadOrStore(value, struct{}{})
					return !loaded
				})

				output := seq.Transform(results, func(obj Item) string {
					value, _ := obj.Object()
					return value
				})

				w.Broadcast(ctx, output)
				msg.Done()
			}
		}(ctx)
	}

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

	close(input)

	wg.Wait()
}

// Execute processes all registered senders concurrently. Standard senders run
// in parallel goroutines. Cyclical senders run in their own goroutines and
// are terminated by a probe-based protocol once all standard senders have
// been exhausted and the cycle reaches quiescence.
func (w *Basic) Execute(ctx context.Context) {
	defer w.Cleanup()

	if len(w.senders) == 0 {
		return
	}

	w.color.Store(&white)

	label := w.String()

	defer w.outputBuffer.Clear()

	var wgStandard sync.WaitGroup
	var wgRecursive sync.WaitGroup

	var hasCycles bool

	for ndx, sender := range w.senders {
		edge := sender.Key()
		cyclical := IsCyclical(edge)

		if cyclical {
			hasCycles = true
			wgRecursive.Add(1)
			go func(index int) {
				var err error
				defer wgRecursive.Done()
				defer w.error(&err)
				defer concurrency.RecoverFromPanic(&err)

				var d Deduplicator
				w.ProcessSender(ctx, index, d.Deduplicate)
			}(ndx)
			continue
		}

		wgStandard.Add(1)
		go func(index int) {
			var err error
			defer wgStandard.Done()
			defer w.error(&err)
			defer concurrency.RecoverFromPanic(&err)
			defer DrainSender(w.senders[index])

			w.ProcessSender(ctx, index, Identity)
		}(ndx)
	}

	wgStandard.Wait()

	defer wgRecursive.Wait()

	if w.Membership != nil {
		w.Membership.SignalReady()
		if !w.Membership.WaitForAllReady(ctx) {
			return
		}

		if !hasCycles {
			return
		}
		defer w.Cleanup()

		if w.Membership.IsLeader() {
			w.Membership.SendProbe(ctx, &Probe{
				Label: label,
			})
		}

		for {
			probe, ok := w.Membership.RecvProbe(ctx)
			if !ok {
				break
			}

			color := *w.color.Swap(&white)

			if probe.Label == label {
				if probe.Color == White && color == White {
					w.Membership.EndProbe()
					break
				}
				probe.Color = White
			}

			if color == Black {
				probe.Color = Black
			}

			if !w.Membership.SendProbe(ctx, probe) {
				break
			}
		}
	}
}
