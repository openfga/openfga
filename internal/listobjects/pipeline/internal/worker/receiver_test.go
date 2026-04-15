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

// --- SeqReceiver Tests ---

// seqOf returns an iter.Seq that yields the given values in order.
func seqOf[T any](values ...T) func(yield func(T) bool) {
	return func(yield func(T) bool) {
		for _, v := range values {
			if !yield(v) {
				return
			}
		}
	}
}

func TestSeqReceiver_YieldsAllElementsInOrder(t *testing.T) {
	r := worker.NewSeqReceiver(seqOf("a", "b", "c"))
	defer r.Close()

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

func TestSeqReceiver_EmptySeqReturnsNotOk(t *testing.T) {
	r := worker.NewSeqReceiver(seqOf[string]())
	defer r.Close()

	_, ok := r.Recv(context.Background())
	assert.False(t, ok)
}

func TestSeqReceiver_ReturnsNotOkAfterExhausted(t *testing.T) {
	r := worker.NewSeqReceiver(seqOf("only"))
	defer r.Close()

	v, ok := r.Recv(context.Background())
	assert.True(t, ok)
	assert.Equal(t, "only", v)

	_, ok = r.Recv(context.Background())
	assert.False(t, ok)
}

func TestSeqReceiver_CloseStopsIteration(t *testing.T) {
	r := worker.NewSeqReceiver(seqOf("a", "b", "c"))

	v, ok := r.Recv(context.Background())
	assert.True(t, ok)
	assert.Equal(t, "a", v)

	r.Close()

	_, ok = r.Recv(context.Background())
	assert.False(t, ok)
}

func TestSeqReceiver_CloseIsIdempotent(t *testing.T) {
	r := worker.NewSeqReceiver(seqOf("a"))
	r.Close()
	r.Close() // must not panic

	_, ok := r.Recv(context.Background())
	assert.False(t, ok)
}

func TestSeqReceiver_RecvAfterCloseReturnsZero(t *testing.T) {
	r := worker.NewSeqReceiver(seqOf(1, 2, 3))
	r.Close()

	v, ok := r.Recv(context.Background())
	assert.False(t, ok)
	assert.Zero(t, v)
}

func TestSeqReceiver_CancelledContextReturnsNotOk(t *testing.T) {
	r := worker.NewSeqReceiver(seqOf("a", "b", "c"))
	defer r.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, ok := r.Recv(ctx)
	assert.False(t, ok)
}

func TestSeqReceiver_CancelledContextReleasesIterator(t *testing.T) {
	r := worker.NewSeqReceiver(seqOf("a", "b", "c"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Recv on cancelled ctx should release the iterator internally.
	_, ok := r.Recv(ctx)
	assert.False(t, ok)

	// Subsequent Recv with a valid context should also return false
	// because the iterator was already released.
	_, ok = r.Recv(context.Background())
	assert.False(t, ok)
}

func TestSeqReceiver_PartialConsumptionThenClose(t *testing.T) {
	var yielded int
	seq := func(yield func(int) bool) {
		for i := range 100 {
			yielded++
			if !yield(i) {
				return
			}
		}
	}

	r := worker.NewSeqReceiver(seq)

	// Consume only a few elements.
	for range 3 {
		_, ok := r.Recv(context.Background())
		assert.True(t, ok)
	}
	r.Close()

	// The iterator should have stopped early rather than
	// running all 100 iterations.
	assert.Less(t, yielded, 100)
}

func TestSeqReceiver_ImplementsReceiver(t *testing.T) {
	r := worker.NewSeqReceiver(seqOf[string]())
	defer r.Close()

	var _ worker.Receiver[string] = r
}
