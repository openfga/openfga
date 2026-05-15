package worker_test

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/openfga/openfga/internal/containers/mpsc"
	"github.com/openfga/openfga/internal/listobjects/pipeline/internal/worker"
)

// --- Test Helpers ---

const chunkSize = 100

// mockSender implements worker.Sender using a buffered channel.
type mockSender struct {
	edge *worker.Edge
	ch   chan *worker.Message
}

func (s *mockSender) Key() *worker.Edge { return s.edge }

func (s *mockSender) Recv(ctx context.Context) (*worker.Message, bool) {
	select {
	case msg, ok := <-s.ch:
		return msg, ok
	case <-ctx.Done():
		return nil, false
	}
}

func (s *mockSender) String() string { return "mock-sender" }

// sendItems creates a mockSender that delivers the given values in a single
// message and then closes.
func sendItems(values ...string) *mockSender {
	ch := make(chan *worker.Message, 1)
	items := make([]string, len(values))
	copy(items, values)
	ch <- &worker.Message{Value: items}
	close(ch)
	return &mockSender{ch: ch}
}

// sendMultipleMessages creates a mockSender that delivers each batch as a
// separate message, then closes.
func sendMultipleMessages(batches ...[]string) *mockSender {
	ch := make(chan *worker.Message, len(batches))
	for _, batch := range batches {
		items := make([]string, len(batch))
		copy(items, batch)
		ch <- &worker.Message{Value: items}
	}
	close(ch)
	return &mockSender{ch: ch}
}

// sendNothing creates a mockSender that delivers no messages.
func sendNothing() *mockSender {
	ch := make(chan *worker.Message)
	close(ch)
	return &mockSender{ch: ch}
}

// mockInterpreter implements worker.Interpreter with a configurable function.
type mockInterpreter struct {
	fn func(context.Context, *worker.Edge, []string) worker.Receiver[worker.Item]
}

func (m *mockInterpreter) Interpret(ctx context.Context, edge *worker.Edge, items []string) worker.Receiver[worker.Item] {
	return m.fn(ctx, edge, items)
}

// passthroughInterpreter returns each input string as a successful Item.
func passthroughInterpreter() *mockInterpreter {
	return &mockInterpreter{
		fn: func(_ context.Context, _ *worker.Edge, items []string) worker.Receiver[worker.Item] {
			return worker.MapReceiver(worker.NewSliceReceiver(items), func(s string) worker.Item {
				return worker.Item{Value: s}
			})
		},
	}
}

func newPool() *worker.MessagePool {
	return worker.NewMessagePool(chunkSize, 10)
}

func newCore(interp worker.Interpreter, errs *mpsc.Accumulator[error]) *worker.Core {
	return &worker.Core{
		Label:       "test",
		Errors:      errs,
		Interpreter: interp,
		ChunkSize:   chunkSize,
		NumProcs:    1,
		Pool:        newPool(),
		MediumFunc: func(edge *worker.Edge, capacity int) worker.Medium {
			return worker.NewChannelMedium(edge, capacity)
		},
	}
}

// collectErrors drains all errors from an Accumulator after it has been closed.
func collectErrors(acc *mpsc.Accumulator[error]) []error {
	var errs []error
	for {
		e, ok := acc.TryRecv()
		if !ok {
			break
		}
		errs = append(errs, e)
	}
	return errs
}

// collectOutput drains all messages from a Sender and returns the combined values.
func collectOutput(sender worker.Sender) []string {
	var results []string
	for {
		msg, ok := sender.Recv(context.Background())
		if !ok {
			break
		}
		results = append(results, msg.Value...)
		msg.Done()
	}
	return results
}

// sorted returns a sorted copy of the input slice.
func sorted(s []string) []string {
	out := make([]string, len(s))
	copy(out, s)
	sort.Strings(out)
	return out
}

// --- Preprocessor Tests ---

func TestIdentityProcessor_Process(t *testing.T) {
	var p worker.IdentityProcessor
	input := []string{"a", "b", "c"}
	result := p.Process(input, make([]string, 0))
	assert.Equal(t, input, result)
}

func TestIdentityProcessor_Process_Empty(t *testing.T) {
	var p worker.IdentityProcessor
	result := p.Process([]string{}, make([]string, 0))
	assert.Empty(t, result)
}

func TestIdentityProcessor_Process_Nil(t *testing.T) {
	var p worker.IdentityProcessor
	result := p.Process(nil, make([]string, 0))
	assert.Nil(t, result)
}

func TestDeduplicatingProcessor_Process_RemovesDuplicates(t *testing.T) {
	var d worker.DeduplicatingProcessor
	result := d.Process([]string{"a", "b", "a", "c", "b"}, make([]string, 0))
	assert.Equal(t, []string{"a", "b", "c"}, result)
}

func TestDeduplicatingProcessor_Process_AcrossCalls(t *testing.T) {
	var d worker.DeduplicatingProcessor

	r1 := d.Process([]string{"a", "b"}, make([]string, 0))
	assert.Equal(t, []string{"a", "b"}, r1)

	r2 := d.Process([]string{"b", "c"}, make([]string, 0))
	assert.Equal(t, []string{"c"}, r2)

	r3 := d.Process([]string{"a", "b", "c"}, make([]string, 0))
	assert.Empty(t, r3)
}

func TestDeduplicatingProcessor_Process_Empty(t *testing.T) {
	var d worker.DeduplicatingProcessor
	result := d.Process([]string{}, make([]string, 0))
	assert.Empty(t, result)
}

func TestDeduplicatingProcessor_Process_AllUnique(t *testing.T) {
	var d worker.DeduplicatingProcessor
	result := d.Process([]string{"x", "y", "z"}, make([]string, 0))
	assert.Equal(t, []string{"x", "y", "z"}, result)
}

func TestDefaultPreprocessor_IsIdentity(t *testing.T) {
	input := []string{"a", "b"}
	result := worker.DefaultPreprocessor.Process(input, make([]string, 0))
	assert.Equal(t, input, result)
}

// --- Basic Worker Tests ---

func TestBasic_Execute_NoSenders(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Basic{Core: newCore(passthroughInterpreter(), errs)}
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Empty(t, collectOutput(output))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestBasic_Execute_SingleSender(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Basic{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b", "c"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"a", "b", "c"}, sorted(collectOutput(output)))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestBasic_Execute_DeduplicatesWithinSender(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Basic{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b", "a", "b", "c"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"a", "b", "c"}, sorted(collectOutput(output)))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestBasic_Execute_DeduplicatesAcrossSenders(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Basic{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b"))
	w.Listen(sendItems("b", "c"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"a", "b", "c"}, sorted(collectOutput(output)))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestBasic_Execute_InterpreterError(t *testing.T) {
	defer goleak.VerifyNone(t)

	sentinelErr := errors.New("interpret failed")
	errs := mpsc.NewAccumulator[error]()

	interp := &mockInterpreter{
		fn: func(_ context.Context, _ *worker.Edge, items []string) worker.Receiver[worker.Item] {
			return worker.MapReceiver(worker.NewSliceReceiver(items), func(s string) worker.Item {
				if s == "bad" {
					return worker.Item{Err: sentinelErr}
				}
				return worker.Item{Value: s}
			})
		},
	}

	w := &worker.Basic{Core: newCore(interp, errs)}
	w.Listen(sendItems("bad", "good"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())
	errs.Close()

	assert.Nil(t, collectOutput(output))
	got := collectErrors(errs)
	require.Len(t, got, 1)
	assert.ErrorIs(t, got[0], sentinelErr)
}

func TestBasic_Execute_ContextCancelled(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Basic{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b", "c"))
	output := w.Subscribe(nil, chunkSize)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	w.Execute(ctx)

	assert.Empty(t, collectOutput(output))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

// --- Intersection Worker Tests ---

func TestIntersection_Execute_NoSenders(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Intersection{Core: newCore(passthroughInterpreter(), errs)}
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Empty(t, collectOutput(output))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestIntersection_Execute_CommonItems(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Intersection{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b", "c"))
	w.Listen(sendItems("b", "c", "d"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"b", "c"}, sorted(collectOutput(output)))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestIntersection_Execute_NoOverlap(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Intersection{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b"))
	w.Listen(sendItems("c", "d"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Empty(t, collectOutput(output))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestIntersection_Execute_OneSenderEmpty(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Intersection{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b"))
	w.Listen(sendNothing())
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Empty(t, collectOutput(output))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestIntersection_Execute_IdenticalSets(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Intersection{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b"))
	w.Listen(sendItems("a", "b"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"a", "b"}, sorted(collectOutput(output)))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestIntersection_Execute_ThreeSenders(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Intersection{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b", "c"))
	w.Listen(sendItems("b", "c", "d"))
	w.Listen(sendItems("c", "d", "e"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"c"}, sorted(collectOutput(output)))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestIntersection_Execute_InterpreterError(t *testing.T) {
	defer goleak.VerifyNone(t)

	sentinelErr := errors.New("interpret failed")
	errs := mpsc.NewAccumulator[error]()

	interp := &mockInterpreter{
		fn: func(_ context.Context, _ *worker.Edge, items []string) worker.Receiver[worker.Item] {
			return worker.MapReceiver(worker.NewSliceReceiver(items), func(s string) worker.Item {
				if s == "bad" {
					return worker.Item{Err: sentinelErr}
				}
				return worker.Item{Value: s}
			})
		},
	}

	w := &worker.Intersection{Core: newCore(interp, errs)}
	w.Listen(sendItems("a", "bad"))
	w.Listen(sendItems("a"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())
	errs.Close()

	_ = collectOutput(output)
	got := collectErrors(errs)
	require.NotEmpty(t, got)
	assert.ErrorIs(t, got[0], sentinelErr)
}

func TestIntersection_Execute_ContextCancelled(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Intersection{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b"))
	w.Listen(sendItems("a", "b"))
	output := w.Subscribe(nil, chunkSize)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	w.Execute(ctx)
	errs.Close()

	assert.Empty(t, collectOutput(output))
}

func TestIntersection_Execute_SingleSender(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Intersection{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b", "c"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	// With a single sender, all items should pass through (intersection of one set).
	assert.Equal(t, []string{"a", "b", "c"}, sorted(collectOutput(output)))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

// --- Difference Worker Tests ---

func TestDifference_Execute_LessThanTwoSenders(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Difference{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Empty(t, collectOutput(output))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestDifference_Execute_SubtractsFromBase(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Difference{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b", "c"))
	w.Listen(sendItems("b"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"a", "c"}, sorted(collectOutput(output)))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestDifference_Execute_EmptyBase(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Difference{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendNothing())
	w.Listen(sendItems("a"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Empty(t, collectOutput(output))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestDifference_Execute_EmptySubtract(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Difference{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b"))
	w.Listen(sendNothing())
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"a", "b"}, sorted(collectOutput(output)))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestDifference_Execute_CompleteSubtraction(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Difference{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b"))
	w.Listen(sendItems("a", "b"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Empty(t, collectOutput(output))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestDifference_Execute_DisjointSets(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Difference{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b"))
	w.Listen(sendItems("c", "d"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"a", "b"}, sorted(collectOutput(output)))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestDifference_Execute_InterpreterError(t *testing.T) {
	defer goleak.VerifyNone(t)

	sentinelErr := errors.New("interpret failed")
	errs := mpsc.NewAccumulator[error]()

	interp := &mockInterpreter{
		fn: func(_ context.Context, _ *worker.Edge, items []string) worker.Receiver[worker.Item] {
			return worker.MapReceiver(worker.NewSliceReceiver(items), func(s string) worker.Item {
				if s == "bad" {
					return worker.Item{Err: sentinelErr}
				}
				return worker.Item{Value: s}
			})
		},
	}

	w := &worker.Difference{Core: newCore(interp, errs)}
	w.Listen(sendItems("a", "bad"))
	w.Listen(sendItems("x"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())
	errs.Close()

	_ = collectOutput(output)
	got := collectErrors(errs)
	require.NotEmpty(t, got)
	assert.ErrorIs(t, got[0], sentinelErr)
}

func TestDifference_Execute_ContextCancelled(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Difference{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b"))
	w.Listen(sendItems("c"))
	output := w.Subscribe(nil, chunkSize)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	w.Execute(ctx)
	errs.Close()

	assert.Empty(t, collectOutput(output))
}

func TestDifference_Execute_NoSenders(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Difference{Core: newCore(passthroughInterpreter(), errs)}
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Empty(t, collectOutput(output))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestDifference_Execute_DuplicatesInSubtractSet(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Difference{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b", "c"))
	w.Listen(sendItems("b", "b", "c", "c"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"a"}, sorted(collectOutput(output)))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestBasic_Execute_MultipleMessagesPerSender(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Basic{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendMultipleMessages([]string{"a", "b"}, []string{"b", "c"}))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	// "b" appears in both messages but should be deduplicated.
	assert.Equal(t, []string{"a", "b", "c"}, sorted(collectOutput(output)))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}

func TestIntersection_Execute_DuplicatesWithinSender(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := mpsc.NewAccumulator[error]()
	w := &worker.Intersection{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "a", "b"))
	w.Listen(sendItems("a", "b", "b"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"a", "b"}, sorted(collectOutput(output)))
	errs.Close()
	assert.Empty(t, collectErrors(errs))
}
