package worker_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/openfga/openfga/internal/listobjects/pipeline/internal/worker"
)

func TestEdgeLabels_NilEdge(t *testing.T) {
	src, dst := worker.EdgeLabels(nil)
	assert.Equal(t, "nil", src)
	assert.Equal(t, "nil", dst)
}

func TestIsCyclical_NilEdge(t *testing.T) {
	assert.False(t, worker.IsCyclical(nil))
}

func TestDrainSender_EmptySender(t *testing.T) {
	m := worker.NewChannelMedium(nil, 0)
	m.Close()

	// DrainSender on a closed, empty sender should return immediately.
	worker.DrainSender(context.Background(), m)
}

func TestDrainSender_WithMessages(t *testing.T) {
	m := worker.NewChannelMedium(nil, 3)

	var callbackCount int
	for range 3 {
		msg := &worker.Message{
			Value:    []string{"x"},
			Callback: func() { callbackCount++ },
		}
		m.Send(context.Background(), msg)
	}
	m.Close()

	worker.DrainSender(context.Background(), m)

	// All callbacks should have been invoked by Done.
	assert.Equal(t, 3, callbackCount)

	// Sender should be fully drained.
	msg, ok := m.Recv(context.Background())
	assert.Nil(t, msg)
	assert.False(t, ok)
}
