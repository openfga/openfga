package mpsc_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/internal/containers/mpsc"
)

func BenchmarkAccumulator(b *testing.B) {
	const totalItems int = 1000

	b.Run("remake", func(b *testing.B) {
		for b.Loop() {
			acc := mpsc.NewAccumulator[int]()

			var wg sync.WaitGroup
			wg.Go(func() {
				for i := range totalItems {
					acc.Add(i)
				}
				acc.Close()
			})

			wg.Go(func() {
				for range acc.Seq() {
				}
			})

			wg.Wait()
		}
	})

	b.Run("reuse", func(b *testing.B) {
		acc := mpsc.NewAccumulator[int]()

		for b.Loop() {
			var wg sync.WaitGroup
			wg.Go(func() {
				for i := range totalItems {
					acc.Add(i)
				}
				acc.Close()
			})

			wg.Go(func() {
				for range acc.Seq() {
				}
			})

			wg.Wait()
		}
	})
}

func TestAccumulator_AddCloseThenRead(t *testing.T) {
	acc := mpsc.NewAccumulator[int]()
	acc.Add(1)
	acc.Add(2)
	acc.Add(3)
	acc.Close()

	var got []int
	for v := range acc.Seq() {
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
		for v := range acc.Seq() {
			got = append(got, v)
		}
	}()

	for i := range 100 {
		acc.Add(i)
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

func TestAccumulator_AddMultipleValues(t *testing.T) {
	acc := mpsc.NewAccumulator[string]()
	acc.Add("a", "b", "c")
	acc.Close()

	var got []string
	for v := range acc.Seq() {
		got = append(got, v)
	}

	require.Equal(t, []string{"a", "b", "c"}, got)
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
				acc.Add(i)
			}
		}()
	}

	var got []int
	done := make(chan struct{})

	go func() {
		defer close(done)
		for v := range acc.Seq() {
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

func TestAccumulator_AddAfterClose(t *testing.T) {
	acc := mpsc.NewAccumulator[int]()
	acc.Add(1)
	acc.Close()

	var got []int
	for v := range acc.Seq() {
		got = append(got, v)
	}
	require.Equal(t, []int{1}, got)

	// Reuse: add more, close again, consume again.
	acc.Add(2, 3)
	acc.Close()

	got = nil
	for v := range acc.Seq() {
		got = append(got, v)
	}
	require.Equal(t, []int{2, 3}, got)
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
	for v := range acc.Seq() {
		got = append(got, v)
	}

	require.Empty(t, got)
}

func TestAccumulator_AddEmpty(t *testing.T) {
	acc := mpsc.NewAccumulator[int]()
	acc.Add()
	acc.Add(1)
	acc.Close()

	var got []int
	for v := range acc.Seq() {
		got = append(got, v)
	}

	require.Equal(t, []int{1}, got)
}

func TestAccumulator_AddThenCloseRace(t *testing.T) {
	for range 10_000 {
		acc := mpsc.NewAccumulator[int]()

		done := make(chan struct{})
		var got []int

		go func() {
			defer close(done)
			for v := range acc.Seq() {
				got = append(got, v)
			}
		}()

		acc.Add(42)
		acc.Close()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for consumer")
		}

		require.Equal(t, []int{42}, got)
	}
}

func TestAccumulator_EarlyBreak(t *testing.T) {
	acc := mpsc.NewAccumulator[int]()
	acc.Add(1, 2, 3, 4, 5)
	acc.Close()

	var got []int
	for v := range acc.Seq() {
		got = append(got, v)
		if v == 3 {
			break
		}
	}

	require.Equal(t, []int{1, 2, 3}, got)
}

func TestAccumulator_SeqResumptionAfterEarlyBreak(t *testing.T) {
	acc := mpsc.NewAccumulator[int]()
	acc.Add(1, 2, 3, 4, 5)
	acc.Close()

	var got []int
	for v := range acc.Seq() {
		got = append(got, v)
		if v == 3 {
			break
		}
	}
	require.Equal(t, []int{1, 2, 3}, got)

	got = nil
	for v := range acc.Seq() {
		got = append(got, v)
	}
	require.Equal(t, []int{4, 5}, got)
}
