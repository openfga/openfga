package worker_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/openfga/openfga/internal/listobjects/pipeline/internal/worker"
)

func TestEmptyReceiver_ReturnsNotOkImmediately(t *testing.T) {
	r := worker.NewEmptyReceiver[string]()
	value, ok := r.Recv(context.Background())
	assert.False(t, ok)
	assert.Empty(t, value)
}

func TestEmptyReceiver_ReturnsNotOkOnRepeatedCalls(t *testing.T) {
	r := worker.NewEmptyReceiver[int]()
	for range 3 {
		_, ok := r.Recv(context.Background())
		assert.False(t, ok)
	}
}

func TestSliceReceiver_YieldsAllElementsInOrder(t *testing.T) {
	r := worker.NewSliceReceiver([]string{"a", "b", "c"})

	var got []string
	for {
		v, ok := r.Recv(context.Background())
		if !ok {
			break
		}
		got = append(got, v)
	}
	assert.Equal(t, []string{"a", "b", "c"}, got)
}

func TestSliceReceiver_EmptySliceReturnsNotOk(t *testing.T) {
	r := worker.NewSliceReceiver([]string{})
	_, ok := r.Recv(context.Background())
	assert.False(t, ok)
}

func TestSliceReceiver_NilSliceReturnsNotOk(t *testing.T) {
	r := worker.NewSliceReceiver[string](nil)
	_, ok := r.Recv(context.Background())
	assert.False(t, ok)
}

func TestSliceReceiver_ReturnsNotOkAfterExhausted(t *testing.T) {
	r := worker.NewSliceReceiver([]string{"x"})

	_, ok := r.Recv(context.Background())
	assert.True(t, ok)

	_, ok = r.Recv(context.Background())
	assert.False(t, ok)
}

func TestSliceReceiver_CancelledContextReturnsNotOk(t *testing.T) {
	r := worker.NewSliceReceiver([]string{"a", "b"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, ok := r.Recv(ctx)
	assert.False(t, ok)
}

func TestSliceReceiver_CloseStopsIteration(t *testing.T) {
	r := worker.NewSliceReceiver([]string{"a", "b", "c"})

	v, ok := r.Recv(context.Background())
	assert.True(t, ok)
	assert.Equal(t, "a", v)

	r.Close()

	_, ok = r.Recv(context.Background())
	assert.False(t, ok)
}

func TestSliceReceiver_CloseIsIdempotent(t *testing.T) {
	r := worker.NewSliceReceiver([]string{"a"})
	r.Close()
	r.Close()

	_, ok := r.Recv(context.Background())
	assert.False(t, ok)
}

func TestValueReceiver_SingleValue(t *testing.T) {
	r := worker.NewValueReceiver(42)

	v, ok := r.Recv(context.Background())
	assert.True(t, ok)
	assert.Equal(t, 42, v)

	_, ok = r.Recv(context.Background())
	assert.False(t, ok)
}

func TestValueReceiver_MultipleValues(t *testing.T) {
	r := worker.NewValueReceiver("x", "y")

	v1, ok := r.Recv(context.Background())
	assert.True(t, ok)
	assert.Equal(t, "x", v1)

	v2, ok := r.Recv(context.Background())
	assert.True(t, ok)
	assert.Equal(t, "y", v2)

	_, ok = r.Recv(context.Background())
	assert.False(t, ok)
}

func TestValueReceiver_NoValuesReturnsNotOk(t *testing.T) {
	r := worker.NewValueReceiver[string]()
	_, ok := r.Recv(context.Background())
	assert.False(t, ok)
}

func TestMapReceiver_TransformsValues(t *testing.T) {
	inner := worker.NewSliceReceiver([]int{1, 2, 3})
	r := worker.MapReceiver(inner, func(i int) string {
		return string(rune('a' - 1 + i))
	})

	var got []string
	for {
		v, ok := r.Recv(context.Background())
		if !ok {
			break
		}
		got = append(got, v)
	}
	assert.Equal(t, []string{"a", "b", "c"}, got)
}

func TestMapReceiver_EmptyInnerReturnsNotOk(t *testing.T) {
	inner := worker.NewEmptyReceiver[int]()
	r := worker.MapReceiver(inner, func(i int) string { return "" })

	_, ok := r.Recv(context.Background())
	assert.False(t, ok)
}

func TestMapReceiver_ClosePropagatesToInner(t *testing.T) {
	inner := worker.NewSliceReceiver([]string{"a", "b", "c"})
	r := worker.MapReceiver(inner, func(s string) string { return s })

	r.Close()

	_, ok := inner.Recv(context.Background())
	assert.False(t, ok)
}

func TestMapReceiver_CancelledContextReturnsNotOk(t *testing.T) {
	inner := worker.NewSliceReceiver([]int{1, 2, 3})
	r := worker.MapReceiver(inner, func(i int) int { return i * 2 })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, ok := r.Recv(ctx)
	assert.False(t, ok)
}
