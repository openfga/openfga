package worker_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/internal/listobjects/pipeline/internal/worker"
)

// collectMessages drains all messages from a Sender, preserving message boundaries.
func collectMessages(sender worker.Sender) [][]string {
	var messages [][]string
	for {
		msg, ok := sender.Recv(context.Background())
		if !ok {
			break
		}
		batch := make([]string, len(msg.Value))
		copy(batch, msg.Value)
		messages = append(messages, batch)
		msg.Done()
	}
	return messages
}

// iterOf returns an iter.Seq[string] that yields the given values in order.
func iterOf(values ...string) func(yield func(string) bool) {
	return func(yield func(string) bool) {
		for _, v := range values {
			if !yield(v) {
				return
			}
		}
	}
}

// --- Message Tests ---

func TestMessage_Done_NilCallback(t *testing.T) {
	m := &worker.Message{Value: []string{"a"}}
	assert.NotPanics(t, m.Done)
}

func TestMessage_Done_CallsCallback(t *testing.T) {
	var called bool
	m := &worker.Message{
		Value:    []string{"a"},
		Callback: func() { called = true },
	}

	m.Done()

	assert.True(t, called)
}

// --- Item Tests ---

func TestItem_Object_Success(t *testing.T) {
	item := worker.Item{Value: "doc:1"}

	value, err := item.Object()

	assert.Equal(t, "doc:1", value)
	assert.NoError(t, err)
}

func TestItem_Object_WithError(t *testing.T) {
	sentinel := errors.New("lookup failed")
	item := worker.Item{Value: "doc:1", Err: sentinel}

	value, err := item.Object()

	assert.Equal(t, "doc:1", value)
	assert.ErrorIs(t, err, sentinel)
}

// --- Core Tests ---

func TestCore_Subscribe_UsesCustomMediumFunc(t *testing.T) {
	var called bool
	core := &worker.Core{
		MediumFunc: func(edge *worker.Edge, capacity int) worker.Medium {
			called = true
			return worker.NewChannelMedium(edge, capacity)
		},
	}

	sender := core.Subscribe(nil, 10)

	assert.True(t, called)
	assert.NotNil(t, sender)
}

func TestCore_Subscribe_UsesDefaultMediumFunc(t *testing.T) {
	core := &worker.Core{}

	sender := core.Subscribe(nil, 10)

	assert.NotNil(t, sender)
	core.Cleanup()

	// Verify the returned sender is functional (channel closed by Cleanup).
	msg, ok := sender.Recv(context.Background())
	assert.Nil(t, msg)
	assert.False(t, ok)
}

func TestCore_Cleanup_ClosesSubscribedSenders(t *testing.T) {
	core := &worker.Core{}
	s1 := core.Subscribe(nil, 10)
	s2 := core.Subscribe(nil, 10)

	core.Cleanup()

	// Both senders should report closed.
	msg, ok := s1.Recv(context.Background())
	assert.Nil(t, msg)
	assert.False(t, ok)

	msg, ok = s2.Recv(context.Background())
	assert.Nil(t, msg)
	assert.False(t, ok)
}

func TestCore_Message_SendsToListener(t *testing.T) {
	core := &worker.Core{
		ChunkSize: chunkSize,
		Pool:      newPool(),
	}
	output := core.Subscribe(nil, chunkSize)

	core.Broadcast(context.Background(), iterOf("a", "b", "c"))
	core.Cleanup()

	assert.Equal(t, []string{"a", "b", "c"}, collectOutput(output))
}

func TestCore_Message_ChunksOutput(t *testing.T) {
	core := &worker.Core{
		ChunkSize: 2,
		Pool:      newPool(),
	}
	output := core.Subscribe(nil, chunkSize)

	core.Broadcast(context.Background(), iterOf("a", "b", "c", "d", "e"))
	core.Cleanup()

	messages := collectMessages(output)
	require.Len(t, messages, 3)
	assert.Equal(t, []string{"a", "b"}, messages[0])
	assert.Equal(t, []string{"c", "d"}, messages[1])
	assert.Equal(t, []string{"e"}, messages[2])
}

func TestCore_Message_BroadcastsToMultipleListeners(t *testing.T) {
	core := &worker.Core{
		ChunkSize: chunkSize,
		Pool:      newPool(),
	}
	out1 := core.Subscribe(nil, chunkSize)
	out2 := core.Subscribe(nil, chunkSize)

	core.Broadcast(context.Background(), iterOf("x", "y"))
	core.Cleanup()

	assert.Equal(t, []string{"x", "y"}, collectOutput(out1))
	assert.Equal(t, []string{"x", "y"}, collectOutput(out2))
}

func TestCore_Message_CallsMsgFunc(t *testing.T) {
	var capturedValues [][]string
	core := &worker.Core{
		ChunkSize: 2,
		Pool:      newPool(),
		MsgFunc: func(msg *worker.Message, edge *worker.Edge) {
			cp := make([]string, len(msg.Value))
			copy(cp, msg.Value)
			capturedValues = append(capturedValues, cp)
		},
	}
	output := core.Subscribe(nil, chunkSize)

	core.Broadcast(context.Background(), iterOf("a", "b", "c"))
	core.Cleanup()

	_ = collectOutput(output)

	// 3 items / chunk 2 = 2 messages, 1 listener → 2 MsgFunc calls.
	require.Len(t, capturedValues, 2)
	assert.Equal(t, []string{"a", "b"}, capturedValues[0])
	assert.Equal(t, []string{"c"}, capturedValues[1])
}

func TestCore_Message_StopsOnCancelledContext(t *testing.T) {
	core := &worker.Core{
		ChunkSize: chunkSize,
		Pool:      newPool(),
	}
	output := core.Subscribe(nil, chunkSize)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	core.Broadcast(ctx, iterOf("a", "b", "c"))
	core.Cleanup()

	assert.Empty(t, collectOutput(output))
}

func TestCore_Message_EmptyIterator(t *testing.T) {
	core := &worker.Core{
		ChunkSize: chunkSize,
		Pool:      newPool(),
	}
	output := core.Subscribe(nil, chunkSize)

	core.Broadcast(context.Background(), iterOf())
	core.Cleanup()

	assert.Empty(t, collectOutput(output))
}

func TestCore_Message_ReturnsBufferToPool(t *testing.T) {
	pool := &sync.Pool{
		New: func() any {
			s := make([]string, 0, chunkSize)
			return &s
		},
	}

	core := &worker.Core{
		ChunkSize: chunkSize,
		Pool:      pool,
	}
	output := core.Subscribe(nil, chunkSize)

	core.Broadcast(context.Background(), iterOf("a"))
	core.Cleanup()

	// Drain the output so the message callback returns the buffer.
	_ = collectOutput(output)

	// The pool should have buffers returned (the read buffer + the message buffer).
	// Verify by getting from the pool — should not allocate.
	buf := pool.Get().(*[]string)
	assert.NotNil(t, buf)
	assert.Empty(t, *buf)
}

// --- CycleGroup / Membership Tests ---

func TestCycleGroup_Size(t *testing.T) {
	g := worker.NewCycleGroup()
	assert.Equal(t, 0, g.Size())

	g.Join("A")
	assert.Equal(t, 1, g.Size())

	g.Join("B")
	assert.Equal(t, 2, g.Size())

	g.Join("C")
	assert.Equal(t, 3, g.Size())
}

func TestMembership_IsLeader(t *testing.T) {
	g := worker.NewCycleGroup()
	a := g.Join("A")
	b := g.Join("B")
	c := g.Join("C")

	// The most recently joined member is the leader.
	assert.False(t, a.IsLeader())
	assert.False(t, b.IsLeader())
	assert.True(t, c.IsLeader())
}

func TestMembership_String_ShowsCyclePath(t *testing.T) {
	g := worker.NewCycleGroup()
	a := g.Join("A")
	b := g.Join("B")
	c := g.Join("C")

	// Each member's string starts from itself and traverses the ring.
	assert.Equal(t, "C->B->A->C", c.String())
	assert.Equal(t, "B->A->C->B", b.String())
	assert.Equal(t, "A->C->B->A", a.String())
}

func TestMembership_Next_TraversesRing(t *testing.T) {
	g := worker.NewCycleGroup()
	a := g.Join("A")
	b := g.Join("B")
	c := g.Join("C")

	// Next follows the ring in the teardown direction.
	assert.Equal(t, b, c.Next())
	assert.Equal(t, a, b.Next())
	assert.Equal(t, c, a.Next())
}

func TestMembership_Wake_UnblocksSleep(t *testing.T) {
	g := worker.NewCycleGroup()
	a := g.Join("A")
	a.SignalReady()

	done := make(chan struct{})
	go func() {
		a.Sleep(context.Background())
		close(done)
	}()

	a.Wake()
	<-done
}

func TestMembership_Wake_IsIdempotent(t *testing.T) {
	g := worker.NewCycleGroup()
	a := g.Join("A")
	a.SignalReady()

	// Multiple Wake calls must not panic.
	a.Wake()
	a.Wake()
	a.Wake()
}

func TestMembership_Sleep_ReturnsOnContextCancellation(t *testing.T) {
	g := worker.NewCycleGroup()
	a := g.Join("A")
	a.SignalReady()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Sleep returns immediately when the context is already cancelled.
	a.Sleep(ctx)
}

func TestMembership_WakeThenSleep_ReturnsImmediately(t *testing.T) {
	g := worker.NewCycleGroup()
	a := g.Join("A")
	a.SignalReady()

	// If Wake is called before Sleep, Sleep must not block.
	a.Wake()
	a.Sleep(context.Background())
}

func TestMembership_SignalReady_WaitForAllReady(t *testing.T) {
	g := worker.NewCycleGroup()
	a := g.Join("A")
	b := g.Join("B")
	c := g.Join("C")

	a.SignalReady()
	b.SignalReady()
	c.SignalReady()

	assert.True(t, a.WaitForAllReady(context.Background()))
}

func TestMembership_WaitForAllReady_ContextCancelled(t *testing.T) {
	g := worker.NewCycleGroup()
	a := g.Join("A")
	_ = g.Join("B") // B never signals ready.

	a.SignalReady()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.False(t, a.WaitForAllReady(ctx))
}

func TestMembership_IncDec_AffectsQuiescence(t *testing.T) {
	g := worker.NewCycleGroup()
	a := g.Join("A")

	// Simulate an in-flight cyclical message.
	a.Inc()
	a.SignalReady()

	// All members reported ready, but inflight > 0: wait should not complete.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, a.WaitForAllReady(ctx))

	// After Dec, inflight reaches 0: wait completes.
	a.Dec()
	assert.True(t, a.WaitForAllReady(context.Background()))
}
