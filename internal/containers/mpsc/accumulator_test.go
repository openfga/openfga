package mpsc_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/internal/containers/mpsc"
)

func BenchmarkAccumulator(b *testing.B) {
	const totalItems int = 1000

	for b.Loop() {
		acc := mpsc.NewAccumulator[int]()

		var wg sync.WaitGroup
		wg.Go(func() {
			for i := range totalItems {
				if !acc.Send(i) {
					b.Fail()
				}
			}
			acc.Close()
		})

		wg.Go(func() {
			for range acc.Seq(context.Background()) {
			}
		})

		wg.Wait()
	}
}

func TestAccumulator_AddCloseThenRead(t *testing.T) {
	acc := mpsc.NewAccumulator[int]()
	require.True(t, acc.Send(1))
	require.True(t, acc.Send(2))
	require.True(t, acc.Send(3))
	acc.Close()

	var got []int
	for v := range acc.Seq(context.Background()) {
		got = append(got, v)
	}

	require.Equal(t, []int{1, 2, 3}, got)
}

func TestAccumulator_ReadWhileAdding(t *testing.T) {
	acc := mpsc.NewAccumulator[int]()

	var got []int
	done := make(chan struct{})

	go func() {
		defer close(done)
		for v := range acc.Seq(context.Background()) {
			got = append(got, v)
		}
	}()

	for i := range 100 {
		require.True(t, acc.Send(i))
	}
	acc.Close()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for consumer")
	}

	require.Len(t, got, 100)
	for i := range 100 {
		require.Equal(t, i, got[i])
	}
}

func TestAccumulator_ConcurrentProducers(t *testing.T) {
	acc := mpsc.NewAccumulator[int]()

	const numProducers = 10
	const itemsPerProducer = 100

	var wg sync.WaitGroup
	wg.Add(numProducers)

	for range numProducers {
		go func() {
			defer wg.Done()
			for i := range itemsPerProducer {
				acc.Send(i)
			}
		}()
	}

	var got []int
	done := make(chan struct{})

	go func() {
		defer close(done)
		for v := range acc.Seq(context.Background()) {
			got = append(got, v)
		}
	}()

	wg.Wait()
	acc.Close()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for consumer")
	}

	require.Len(t, got, numProducers*itemsPerProducer)
}

func TestAccumulator_CloseMultipleTimes(t *testing.T) {
	acc := mpsc.NewAccumulator[int]()

	require.NotPanics(t, func() {
		acc.Close()
		acc.Close()
		acc.Close()
	})
}

func TestAccumulator_EmptyAccumulator(t *testing.T) {
	acc := mpsc.NewAccumulator[int]()
	acc.Close()

	var got []int
	for v := range acc.Seq(context.Background()) {
		got = append(got, v)
	}

	require.Empty(t, got)
}

func TestAccumulator_AddThenCloseRace(t *testing.T) {
	for range 10_000 {
		acc := mpsc.NewAccumulator[int]()

		done := make(chan struct{})
		var got []int

		go func() {
			defer close(done)
			for v := range acc.Seq(context.Background()) {
				got = append(got, v)
			}
		}()

		require.True(t, acc.Send(42))
		acc.Close()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for consumer")
		}

		require.Equal(t, []int{42}, got)
	}
}

func TestAccumulator_SendAfterClose(t *testing.T) {
	acc := mpsc.NewAccumulator[int]()
	acc.Close()

	require.False(t, acc.Send(1))
}

func TestAccumulator_RecvCancelledContext(t *testing.T) {
	acc := mpsc.NewAccumulator[int]()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, ok := acc.Recv(ctx)
	require.False(t, ok)

	acc.Close()
}

func TestAccumulator_RecvDirect(t *testing.T) {
	acc := mpsc.NewAccumulator[int]()
	require.True(t, acc.Send(10))
	require.True(t, acc.Send(20))
	acc.Close()

	v1, ok := acc.Recv(context.Background())
	require.True(t, ok)
	require.Equal(t, 10, v1)

	v2, ok := acc.Recv(context.Background())
	require.True(t, ok)
	require.Equal(t, 20, v2)

	_, ok = acc.Recv(context.Background())
	require.False(t, ok)
}

func TestAccumulator_EarlyBreak(t *testing.T) {
	acc := mpsc.NewAccumulator[int]()
	for i := range 5 {
		require.True(t, acc.Send(i))
	}
	acc.Close()

	var got []int
	for v := range acc.Seq(context.Background()) {
		got = append(got, v)
		if v == 3 {
			break
		}
	}

	require.Equal(t, []int{0, 1, 2, 3}, got)
}
