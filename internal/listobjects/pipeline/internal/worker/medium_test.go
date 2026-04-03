package worker_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/internal/listobjects/pipeline/internal/worker"
)

// --- AccumulatorMedium Tests ---

func TestAccumulatorMedium_SendRecv(t *testing.T) {
	m := worker.NewAccumulatorMedium(nil)

	msg := &worker.Message{Value: []string{"a", "b"}}
	ok := m.Send(context.Background(), msg)
	require.True(t, ok)

	m.Close()

	got, more := m.Recv(context.Background())
	require.True(t, more)
	assert.Equal(t, []string{"a", "b"}, got.Value)

	// After draining, Recv returns false.
	got, more = m.Recv(context.Background())
	assert.Nil(t, got)
	assert.False(t, more)
}

func TestAccumulatorMedium_RecvAfterClose_ReturnsFalse(t *testing.T) {
	m := worker.NewAccumulatorMedium(nil)
	m.Close()

	got, more := m.Recv(context.Background())
	assert.Nil(t, got)
	assert.False(t, more)
}

func TestAccumulatorMedium_RecvAfterDrain_ReturnsFalseRepeatedly(t *testing.T) {
	m := worker.NewAccumulatorMedium(nil)
	m.Close()

	// First call sets closed = true internally.
	_, _ = m.Recv(context.Background())

	// Second call takes the m.closed fast path.
	got, more := m.Recv(context.Background())
	assert.Nil(t, got)
	assert.False(t, more)
}

func TestAccumulatorMedium_Send_CancelledContext(t *testing.T) {
	m := worker.NewAccumulatorMedium(nil)
	defer m.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ok := m.Send(ctx, &worker.Message{Value: []string{"x"}})
	assert.False(t, ok)
}

func TestAccumulatorMedium_MultipleProducers(t *testing.T) {
	m := worker.NewAccumulatorMedium(nil)

	const producers = 5
	var wg sync.WaitGroup
	wg.Add(producers)

	for i := range producers {
		go func(id int) {
			defer wg.Done()
			m.Send(context.Background(), &worker.Message{Value: []string{string(rune('a' + id))}})
		}(i)
	}

	go func() {
		wg.Wait()
		m.Close()
	}()

	var values []string
	for {
		msg, more := m.Recv(context.Background())
		if !more {
			break
		}
		values = append(values, msg.Value...)
	}

	assert.Len(t, values, producers)
}

func TestAccumulatorMedium_Key(t *testing.T) {
	m := worker.NewAccumulatorMedium(nil)
	assert.Nil(t, m.Key())
}

func TestAccumulatorMedium_String(t *testing.T) {
	m := worker.NewAccumulatorMedium(nil)
	assert.Equal(t, "nil->nil", m.String())
}

// --- ChannelMedium Tests ---

func TestChannelMedium_SendRecv(t *testing.T) {
	m := worker.NewChannelMedium(nil, 2)

	msg := &worker.Message{Value: []string{"x", "y"}}
	ok := m.Send(context.Background(), msg)
	require.True(t, ok)

	m.Close()

	got, more := m.Recv(context.Background())
	require.True(t, more)
	assert.Equal(t, []string{"x", "y"}, got.Value)

	// After draining, Recv returns false.
	got, more = m.Recv(context.Background())
	assert.Nil(t, got)
	assert.False(t, more)
}

func TestChannelMedium_RecvAfterClose_ReturnsFalse(t *testing.T) {
	m := worker.NewChannelMedium(nil, 1)
	m.Close()

	got, more := m.Recv(context.Background())
	assert.Nil(t, got)
	assert.False(t, more)
}

func TestChannelMedium_Recv_CancelledContext(t *testing.T) {
	m := worker.NewChannelMedium(nil, 1)
	defer m.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got, more := m.Recv(ctx)
	assert.Nil(t, got)
	assert.False(t, more)
}

func TestChannelMedium_Send_CancelledContext(t *testing.T) {
	// Use capacity 0 so Send would block, forcing the ctx.Done() path.
	m := worker.NewChannelMedium(nil, 0)
	defer m.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ok := m.Send(ctx, &worker.Message{Value: []string{"z"}})
	assert.False(t, ok)
}

func TestChannelMedium_Close_IsIdempotent(t *testing.T) {
	m := worker.NewChannelMedium(nil, 1)

	assert.NotPanics(t, func() {
		m.Close()
		m.Close()
		m.Close()
	})
}

func TestChannelMedium_Key(t *testing.T) {
	m := worker.NewChannelMedium(nil, 1)
	assert.Nil(t, m.Key())
}

func TestChannelMedium_String(t *testing.T) {
	m := worker.NewChannelMedium(nil, 1)
	assert.Equal(t, "nil->nil", m.String())
}

func TestChannelMedium_OrderPreserved(t *testing.T) {
	m := worker.NewChannelMedium(nil, 3)

	for _, v := range []string{"first", "second", "third"} {
		m.Send(context.Background(), &worker.Message{Value: []string{v}})
	}
	m.Close()

	var order []string
	for {
		msg, more := m.Recv(context.Background())
		if !more {
			break
		}
		order = append(order, msg.Value...)
	}

	assert.Equal(t, []string{"first", "second", "third"}, order)
}

func TestChannelMedium_CallbackInvokedOnDone(t *testing.T) {
	m := worker.NewChannelMedium(nil, 1)

	var called bool
	msg := &worker.Message{
		Value:    []string{"val"},
		Callback: func() { called = true },
	}

	m.Send(context.Background(), msg)
	m.Close()

	received, ok := m.Recv(context.Background())
	require.True(t, ok)
	received.Done()

	assert.True(t, called)
}

// --- FloorPowerOfTwo Tests ---

func TestFloorPowerOfTwo(t *testing.T) {
	tests := []struct {
		name   string
		input  uint
		expect int
	}{
		{name: "zero_returns_one", input: 0, expect: 1},
		{name: "one", input: 1, expect: 1},
		{name: "two", input: 2, expect: 2},
		{name: "three_rounds_down", input: 3, expect: 2},
		{name: "four", input: 4, expect: 4},
		{name: "five_rounds_down", input: 5, expect: 4},
		{name: "seven_rounds_down", input: 7, expect: 4},
		{name: "eight", input: 8, expect: 8},
		{name: "127_rounds_down", input: 127, expect: 64},
		{name: "128", input: 128, expect: 128},
		{name: "129_rounds_down", input: 129, expect: 128},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expect, worker.FloorPowerOfTwo(tc.input))
		})
	}
}

// --- QueueMedium Tests ---

func TestQueueMedium_SendRecv(t *testing.T) {
	m := worker.NewQueueMedium(nil, 4)

	msg := &worker.Message{Value: []string{"a", "b"}}
	ok := m.Send(context.Background(), msg)
	require.True(t, ok)

	m.Close()

	got, more := m.Recv(context.Background())
	require.True(t, more)
	assert.Equal(t, []string{"a", "b"}, got.Value)

	// After draining, Recv returns false.
	got, more = m.Recv(context.Background())
	assert.Nil(t, got)
	assert.False(t, more)
}

func TestQueueMedium_RecvAfterClose_ReturnsFalse(t *testing.T) {
	m := worker.NewQueueMedium(nil, 4)
	m.Close()

	got, more := m.Recv(context.Background())
	assert.Nil(t, got)
	assert.False(t, more)
}

func TestQueueMedium_RecvAfterDrain_ReturnsFalseRepeatedly(t *testing.T) {
	m := worker.NewQueueMedium(nil, 4)
	m.Close()

	// First call sets closed = true internally.
	_, _ = m.Recv(context.Background())

	// Second call takes the m.closed fast path.
	got, more := m.Recv(context.Background())
	assert.Nil(t, got)
	assert.False(t, more)
}

func TestQueueMedium_Send_CancelledContext(t *testing.T) {
	m := worker.NewQueueMedium(nil, 4)
	defer m.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ok := m.Send(ctx, &worker.Message{Value: []string{"x"}})
	assert.False(t, ok)
}

func TestQueueMedium_OrderPreserved(t *testing.T) {
	m := worker.NewQueueMedium(nil, 4)

	for _, v := range []string{"first", "second", "third"} {
		m.Send(context.Background(), &worker.Message{Value: []string{v}})
	}
	m.Close()

	var order []string
	for {
		msg, more := m.Recv(context.Background())
		if !more {
			break
		}
		order = append(order, msg.Value...)
	}

	assert.Equal(t, []string{"first", "second", "third"}, order)
}

func TestQueueMedium_Key(t *testing.T) {
	m := worker.NewQueueMedium(nil, 4)
	assert.Nil(t, m.Key())
}

func TestQueueMedium_String(t *testing.T) {
	m := worker.NewQueueMedium(nil, 4)
	assert.Equal(t, "nil->nil", m.String())
}

func TestQueueMedium_ImplementsMedium(t *testing.T) {
	var _ worker.Medium = worker.NewQueueMedium(nil, 4)
}

// --- Interface compliance ---

func TestAccumulatorMedium_ImplementsMedium(t *testing.T) {
	var _ worker.Medium = worker.NewAccumulatorMedium(nil)
}

func TestChannelMedium_ImplementsMedium(t *testing.T) {
	var _ worker.Medium = worker.NewChannelMedium(nil, 1)
}
