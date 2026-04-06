package worker_test

import (
	"context"
	"errors"
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

func TestMessage_Done_ClearsFields(t *testing.T) {
	m := &worker.Message{
		Value:    []string{"a", "b"},
		Callback: func() {},
	}

	m.Done()

	assert.Nil(t, m.Callback, "Callback should be nil after Done")
}

func TestMessage_Done_IdempotentAfterClear(t *testing.T) {
	callCount := 0
	m := &worker.Message{
		Value:    []string{"a"},
		Callback: func() { callCount++ },
	}

	m.Done()
	m.Done() // second call should be safe and not invoke callback again

	assert.Equal(t, 1, callCount)
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
	core.Broadcast(context.Background(), worker.NewValueReceiver("a", "b", "c"))
	core.Cleanup()

	assert.Equal(t, []string{"a", "b", "c"}, collectOutput(output))
}

func TestCore_Message_ChunksOutput(t *testing.T) {
	core := &worker.Core{
		ChunkSize: 2,
		Pool:      newPool(),
	}
	output := core.Subscribe(nil, chunkSize)

	core.Broadcast(context.Background(), worker.NewValueReceiver("a", "b", "c", "d", "e"))
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

	core.Broadcast(context.Background(), worker.NewValueReceiver("x", "y"))
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

	core.Broadcast(context.Background(), worker.NewValueReceiver("a", "b", "c"))
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

	core.Broadcast(ctx, worker.NewValueReceiver("a", "b", "c"))
	core.Cleanup()

	assert.Empty(t, collectOutput(output))
}

func TestCore_Message_EmptyIterator(t *testing.T) {
	core := &worker.Core{
		ChunkSize: chunkSize,
		Pool:      newPool(),
	}
	output := core.Subscribe(nil, chunkSize)

	core.Broadcast(context.Background(), worker.NewEmptyReceiver[string]())
	core.Cleanup()

	assert.Empty(t, collectOutput(output))
}

func TestCore_Message_ReturnsMessageToPool(t *testing.T) {
	pool := worker.NewMessagePool(chunkSize, 10)

	core := &worker.Core{
		ChunkSize: chunkSize,
		Pool:      pool,
	}
	output := core.Subscribe(nil, chunkSize)

	core.Broadcast(context.Background(), worker.NewValueReceiver("a"))
	core.Cleanup()

	// Drain the output so the message callback returns the message.
	_ = collectOutput(output)

	// The pool should have message returned.
	// Verify by getting from the pool — should not allocate.
	buf := pool.Get()
	assert.NotNil(t, buf)
	assert.Empty(t, buf.Value)
}

// --- MessagePool Tests ---

func TestMessagePool_Get_ReturnsMessageWithEmptyValue(t *testing.T) {
	pool := worker.NewMessagePool(10, 5)
	msg := pool.Get()
	assert.NotNil(t, msg)
	assert.Empty(t, msg.Value)
	assert.Equal(t, 10, cap(msg.Value))
}

func TestMessagePool_PutGet_RecyclesMessage(t *testing.T) {
	pool := worker.NewMessagePool(10, 5)
	msg := pool.Get()

	// Simulate usage.
	msg.Value = append(msg.Value, "a", "b", "c")
	pool.Put(msg)

	recycled := pool.Get()
	assert.Empty(t, recycled.Value, "recycled message Value should be empty")
	assert.Equal(t, 10, cap(recycled.Value), "recycled message should retain original capacity")
}

func TestMessagePool_Put_ClearsValueContents(t *testing.T) {
	pool := worker.NewMessagePool(4, 5)
	msg := pool.Get()

	msg.Value = append(msg.Value, "secret")
	pool.Put(msg)

	recycled := pool.Get()
	// The backing array should be zeroed out.
	raw := recycled.Value[:cap(recycled.Value)]
	for i, v := range raw {
		assert.Empty(t, v, "backing array index %d should be zero-value", i)
	}
}

func TestMessagePool_Put_DropsWhenFull(t *testing.T) {
	pool := worker.NewMessagePool(4, 1)

	msg1 := pool.Get()
	msg2 := pool.Get()

	pool.Put(msg1)
	pool.Put(msg2) // pool capacity is 1, so this should be silently dropped

	got := pool.Get()
	assert.NotNil(t, got)
}

func TestInitMessagePool(t *testing.T) {
	var pool worker.MessagePool
	worker.InitMessagePool(&pool, 8, 3)

	msg := pool.Get()
	assert.NotNil(t, msg)
	assert.Equal(t, 8, cap(msg.Value))
}

// --- Core.Len Tests ---

func TestCore_Len_ReturnsListenerCount(t *testing.T) {
	core := &worker.Core{}
	assert.Equal(t, 0, core.Len())

	core.Subscribe(nil, 10)
	assert.Equal(t, 1, core.Len())

	core.Subscribe(nil, 10)
	assert.Equal(t, 2, core.Len())

	core.Cleanup()
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

func TestCycleGroup_SingleMember(t *testing.T) {
	g := worker.NewCycleGroup()
	m := g.Join("A")

	assert.Equal(t, 1, g.Size())
	assert.True(t, m.IsLeader())
	assert.Equal(t, m, m.Next(), "single member's Next should point to itself")
	assert.Equal(t, "A->A", m.String())
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
