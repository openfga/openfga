package worker_test

import (
	"context"
	"errors"
	"iter"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

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

// sendNothing creates a mockSender that delivers no messages.
func sendNothing() *mockSender {
	ch := make(chan *worker.Message)
	close(ch)
	return &mockSender{ch: ch}
}

// mockInterpreter implements worker.Interpreter with a configurable function.
type mockInterpreter struct {
	fn func(context.Context, *worker.Edge, []string) iter.Seq[worker.Item]
}

func (m *mockInterpreter) Interpret(ctx context.Context, edge *worker.Edge, items []string) iter.Seq[worker.Item] {
	return m.fn(ctx, edge, items)
}

// passthroughInterpreter returns each input string as a successful Item.
func passthroughInterpreter() *mockInterpreter {
	return &mockInterpreter{
		fn: func(_ context.Context, _ *worker.Edge, items []string) iter.Seq[worker.Item] {
			return func(yield func(worker.Item) bool) {
				for _, item := range items {
					if !yield(worker.Item{Value: item}) {
						return
					}
				}
			}
		},
	}
}

func newPool() *worker.BufferPool {
	return worker.NewBufferPool(chunkSize, 10)
}

func newCore(interp worker.Interpreter, errs chan<- error) *worker.Core {
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

	errs := make(chan error, 10)
	w := &worker.Basic{Core: newCore(passthroughInterpreter(), errs)}
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Empty(t, collectOutput(output))
	assert.Empty(t, errs)
}

func TestBasic_Execute_SingleSender(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := make(chan error, 10)
	w := &worker.Basic{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b", "c"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"a", "b", "c"}, sorted(collectOutput(output)))
	assert.Empty(t, errs)
}

func TestBasic_Execute_DeduplicatesWithinSender(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := make(chan error, 10)
	w := &worker.Basic{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b", "a", "b", "c"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"a", "b", "c"}, sorted(collectOutput(output)))
	assert.Empty(t, errs)
}

func TestBasic_Execute_DeduplicatesAcrossSenders(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := make(chan error, 10)
	w := &worker.Basic{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b"))
	w.Listen(sendItems("b", "c"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"a", "b", "c"}, sorted(collectOutput(output)))
	assert.Empty(t, errs)
}

func TestBasic_Execute_InterpreterError(t *testing.T) {
	defer goleak.VerifyNone(t)

	sentinelErr := errors.New("interpret failed")
	errs := make(chan error, 10)

	interp := &mockInterpreter{
		fn: func(_ context.Context, _ *worker.Edge, items []string) iter.Seq[worker.Item] {
			return func(yield func(worker.Item) bool) {
				for _, item := range items {
					if item == "bad" {
						if !yield(worker.Item{Err: sentinelErr}) {
							return
						}
						continue
					}
					if !yield(worker.Item{Value: item}) {
						return
					}
				}
			}
		},
	}

	w := &worker.Basic{Core: newCore(interp, errs)}
	w.Listen(sendItems("bad", "good"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"good"}, collectOutput(output))
	require.Len(t, errs, 1)
	assert.ErrorIs(t, <-errs, sentinelErr)
}

func TestBasic_Execute_ContextCancelled(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := make(chan error, 10)
	w := &worker.Basic{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b", "c"))
	output := w.Subscribe(nil, chunkSize)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	w.Execute(ctx)

	assert.Empty(t, collectOutput(output))
	assert.Empty(t, errs)
}

// --- Intersection Worker Tests ---

func TestIntersection_Execute_NoSenders(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := make(chan error, 10)
	w := &worker.Intersection{Core: newCore(passthroughInterpreter(), errs)}
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Empty(t, collectOutput(output))
	assert.Empty(t, errs)
}

func TestIntersection_Execute_CommonItems(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := make(chan error, 10)
	w := &worker.Intersection{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b", "c"))
	w.Listen(sendItems("b", "c", "d"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"b", "c"}, sorted(collectOutput(output)))
	assert.Empty(t, errs)
}

func TestIntersection_Execute_NoOverlap(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := make(chan error, 10)
	w := &worker.Intersection{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b"))
	w.Listen(sendItems("c", "d"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Empty(t, collectOutput(output))
	assert.Empty(t, errs)
}

func TestIntersection_Execute_OneSenderEmpty(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := make(chan error, 10)
	w := &worker.Intersection{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b"))
	w.Listen(sendNothing())
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Empty(t, collectOutput(output))
	assert.Empty(t, errs)
}

func TestIntersection_Execute_IdenticalSets(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := make(chan error, 10)
	w := &worker.Intersection{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b"))
	w.Listen(sendItems("a", "b"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"a", "b"}, sorted(collectOutput(output)))
	assert.Empty(t, errs)
}

func TestIntersection_Execute_ThreeSenders(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := make(chan error, 10)
	w := &worker.Intersection{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b", "c"))
	w.Listen(sendItems("b", "c", "d"))
	w.Listen(sendItems("c", "d", "e"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"c"}, sorted(collectOutput(output)))
	assert.Empty(t, errs)
}

func TestIntersection_Execute_InterpreterError(t *testing.T) {
	defer goleak.VerifyNone(t)

	sentinelErr := errors.New("interpret failed")
	errs := make(chan error, 10)

	interp := &mockInterpreter{
		fn: func(_ context.Context, _ *worker.Edge, items []string) iter.Seq[worker.Item] {
			return func(yield func(worker.Item) bool) {
				for _, item := range items {
					if item == "bad" {
						yield(worker.Item{Err: sentinelErr})
						return
					}
					if !yield(worker.Item{Value: item}) {
						return
					}
				}
			}
		},
	}

	w := &worker.Intersection{Core: newCore(interp, errs)}
	w.Listen(sendItems("a", "bad"))
	w.Listen(sendItems("a"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	_ = collectOutput(output)
	require.NotEmpty(t, errs)
	assert.ErrorIs(t, <-errs, sentinelErr)
}

func TestIntersection_Execute_ContextCancelled(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := make(chan error, 10)
	w := &worker.Intersection{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b"))
	w.Listen(sendItems("a", "b"))
	output := w.Subscribe(nil, chunkSize)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	w.Execute(ctx)

	assert.Empty(t, collectOutput(output))
}

func TestIntersection_Execute_SingleSender(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := make(chan error, 10)
	w := &worker.Intersection{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b", "c"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	// With a single sender, all items should pass through (intersection of one set).
	assert.Equal(t, []string{"a", "b", "c"}, sorted(collectOutput(output)))
	assert.Empty(t, errs)
}

// --- Difference Worker Tests ---

func TestDifference_Execute_LessThanTwoSenders(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := make(chan error, 10)
	w := &worker.Difference{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Empty(t, collectOutput(output))
	assert.Empty(t, errs)
}

func TestDifference_Execute_SubtractsFromBase(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := make(chan error, 10)
	w := &worker.Difference{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b", "c"))
	w.Listen(sendItems("b"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"a", "c"}, sorted(collectOutput(output)))
	assert.Empty(t, errs)
}

func TestDifference_Execute_EmptyBase(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := make(chan error, 10)
	w := &worker.Difference{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendNothing())
	w.Listen(sendItems("a"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Empty(t, collectOutput(output))
	assert.Empty(t, errs)
}

func TestDifference_Execute_EmptySubtract(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := make(chan error, 10)
	w := &worker.Difference{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b"))
	w.Listen(sendNothing())
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"a", "b"}, sorted(collectOutput(output)))
	assert.Empty(t, errs)
}

func TestDifference_Execute_CompleteSubtraction(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := make(chan error, 10)
	w := &worker.Difference{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b"))
	w.Listen(sendItems("a", "b"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Empty(t, collectOutput(output))
	assert.Empty(t, errs)
}

func TestDifference_Execute_DisjointSets(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := make(chan error, 10)
	w := &worker.Difference{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b"))
	w.Listen(sendItems("c", "d"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Equal(t, []string{"a", "b"}, sorted(collectOutput(output)))
	assert.Empty(t, errs)
}

func TestDifference_Execute_InterpreterError(t *testing.T) {
	defer goleak.VerifyNone(t)

	sentinelErr := errors.New("interpret failed")
	errs := make(chan error, 10)

	interp := &mockInterpreter{
		fn: func(_ context.Context, _ *worker.Edge, items []string) iter.Seq[worker.Item] {
			return func(yield func(worker.Item) bool) {
				for _, item := range items {
					if item == "bad" {
						yield(worker.Item{Err: sentinelErr})
						return
					}
					if !yield(worker.Item{Value: item}) {
						return
					}
				}
			}
		},
	}

	w := &worker.Difference{Core: newCore(interp, errs)}
	w.Listen(sendItems("a", "bad"))
	w.Listen(sendItems("x"))
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	_ = collectOutput(output)
	require.NotEmpty(t, errs)
	assert.ErrorIs(t, <-errs, sentinelErr)
}

func TestDifference_Execute_ContextCancelled(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := make(chan error, 10)
	w := &worker.Difference{Core: newCore(passthroughInterpreter(), errs)}
	w.Listen(sendItems("a", "b"))
	w.Listen(sendItems("c"))
	output := w.Subscribe(nil, chunkSize)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	w.Execute(ctx)

	assert.Empty(t, collectOutput(output))
}

func TestDifference_Execute_NoSenders(t *testing.T) {
	defer goleak.VerifyNone(t)

	errs := make(chan error, 10)
	w := &worker.Difference{Core: newCore(passthroughInterpreter(), errs)}
	output := w.Subscribe(nil, chunkSize)

	w.Execute(context.Background())

	assert.Empty(t, collectOutput(output))
	assert.Empty(t, errs)
}
