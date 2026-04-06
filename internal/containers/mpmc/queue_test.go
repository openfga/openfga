package mpmc

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	defaultCapacity   int    = 1 << 7
	defaultExtensions int    = -1
	messageCount      uint64 = 1000
)

type item struct{}

func feed(p *Queue[item]) {
	for range messageCount {
		p.Send(context.Background(), item{})
	}
}

func consume(p *Queue[item], count *atomic.Uint64) {
	for {
		_, ok := p.Recv(context.Background())
		if !ok {
			break
		}
		count.Add(1)
	}
}

func BenchmarkQueue(b *testing.B) {
	b.Run("single_producer_single_consumer", func(b *testing.B) {
		for b.Loop() {
			p, err := NewQueue[item](defaultCapacity, defaultExtensions)
			require.NoError(b, err)

			var count atomic.Uint64
			var wg sync.WaitGroup

			wg.Add(1)
			go func() {
				defer wg.Done()
				feed(p)
				p.Close()
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				consume(p, &count)
			}()

			wg.Wait()

			require.Equal(b, messageCount, count.Load())
		}
	})

	b.Run("multiple_producer_single_consumer", func(b *testing.B) {
		for b.Loop() {
			p, err := NewQueue[item](defaultCapacity, defaultExtensions)
			require.NoError(b, err)

			var count atomic.Uint64
			var swg sync.WaitGroup
			var cwg sync.WaitGroup

			for range 4 {
				swg.Add(1)
				go func() {
					defer swg.Done()
					feed(p)
				}()
			}

			cwg.Add(1)
			go func() {
				defer cwg.Done()
				consume(p, &count)
			}()

			swg.Wait()
			p.Close()
			cwg.Wait()

			require.Equal(b, messageCount*4, count.Load())
		}
	})

	b.Run("single_producer_multiple_consumer", func(b *testing.B) {
		for b.Loop() {
			p, err := NewQueue[item](defaultCapacity, defaultExtensions)
			require.NoError(b, err)

			var count atomic.Uint64
			var swg sync.WaitGroup
			var cwg sync.WaitGroup

			swg.Add(1)
			go func() {
				defer swg.Done()
				feed(p)
			}()

			for range 4 {
				cwg.Add(1)
				go func() {
					defer cwg.Done()
					consume(p, &count)
				}()
			}

			swg.Wait()
			p.Close()
			cwg.Wait()

			require.Equal(b, messageCount, count.Load())
		}
	})

	b.Run("multiple_producer_multiple_consumer", func(b *testing.B) {
		for b.Loop() {
			p, err := NewQueue[item](defaultCapacity, defaultExtensions)
			require.NoError(b, err)

			var count atomic.Uint64
			var swg sync.WaitGroup
			var cwg sync.WaitGroup

			for range 4 {
				swg.Add(1)
				go func() {
					defer swg.Done()
					feed(p)
				}()
			}

			for range 4 {
				cwg.Add(1)
				go func() {
					defer cwg.Done()
					consume(p, &count)
				}()
			}

			swg.Wait()
			p.Close()
			cwg.Wait()

			require.Equal(b, messageCount*4, count.Load())
		}
	})
}

func TestQueue(t *testing.T) {
	t.Run("buffer_cycles", func(t *testing.T) {
		tests := []struct {
			name     string
			capacity int
			parts    int
			cycles   int
		}{
			{
				name:     "small",
				capacity: 1 << 2,
				parts:    1,
				cycles:   10,
			},
			{
				name:     "medium",
				capacity: 1 << 10,
				parts:    13,
				cycles:   10,
			},
			{
				name:     "large",
				capacity: 1 << 20,
				parts:    333,
				cycles:   10,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				expected := make([]int, tc.capacity)
				for i := range tc.capacity {
					expected[i] = i + 1
				}

				p, err := NewQueue[int](tc.capacity, defaultExtensions)
				require.NoError(t, err)

				for range tc.cycles {
					var count int

					actual := make([]int, 0, 10)

					for i := range tc.capacity {
						p.Send(context.Background(), i+1)
						count++

						if count == tc.parts {
							for range tc.parts {
								val, ok := p.Recv(context.Background())
								require.True(t, ok)

								count--
								actual = append(actual, val)
							}
						}
					}

					for range count {
						val, ok := p.Recv(context.Background())
						require.True(t, ok)

						actual = append(actual, val)
					}

					require.Equal(t, expected, actual)
				}

				expected = make([]int, 0, tc.capacity)
				for i := tc.capacity; i > 0; i-- {
					p.Send(context.Background(), i)
					expected = append(expected, i)
				}

				p.Close()

				var actual []int

				for val, ok := p.Recv(context.Background()); ok; val, ok = p.Recv(context.Background()) {
					actual = append(actual, val)
				}

				require.Equal(t, expected, actual)
			})
		}
	})

	t.Run("single_producer_single_consumer", func(t *testing.T) {
		p, err := NewQueue[item](defaultCapacity, defaultExtensions)
		require.NoError(t, err)

		var count atomic.Uint64
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			feed(p)
			p.Close()
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			consume(p, &count)
		}()

		wg.Wait()

		require.Equal(t, messageCount, count.Load())
	})

	t.Run("multiple_producer_single_consumer", func(t *testing.T) {
		p, err := NewQueue[item](defaultCapacity, defaultExtensions)
		require.NoError(t, err)

		var count atomic.Uint64
		var swg sync.WaitGroup
		var cwg sync.WaitGroup

		for range 4 {
			swg.Add(1)
			go func() {
				defer swg.Done()
				feed(p)
			}()
		}

		cwg.Add(1)
		go func() {
			defer cwg.Done()
			consume(p, &count)
		}()

		swg.Wait()
		p.Close()
		cwg.Wait()

		require.Equal(t, messageCount*4, count.Load())
	})

	t.Run("single_producer_multiple_consumer", func(t *testing.T) {
		p, err := NewQueue[item](defaultCapacity, defaultExtensions)
		require.NoError(t, err)

		var count atomic.Uint64
		var swg sync.WaitGroup
		var cwg sync.WaitGroup

		swg.Add(1)
		go func() {
			defer swg.Done()
			feed(p)
		}()

		for range 4 {
			cwg.Add(1)
			go func() {
				defer cwg.Done()
				consume(p, &count)
			}()
		}

		swg.Wait()
		p.Close()
		cwg.Wait()

		require.Equal(t, messageCount, count.Load())
	})

	t.Run("multiple_producer_multiple_consumer", func(t *testing.T) {
		p, err := NewQueue[item](defaultCapacity, defaultExtensions)
		require.NoError(t, err)

		var count atomic.Uint64
		var swg sync.WaitGroup
		var cwg sync.WaitGroup

		for range 4 {
			swg.Add(1)
			go func() {
				defer swg.Done()
				feed(p)
			}()
		}

		for range 4 {
			cwg.Add(1)
			go func() {
				defer cwg.Done()
				consume(p, &count)
			}()
		}

		swg.Wait()
		p.Close()
		cwg.Wait()

		require.Equal(t, messageCount*4, count.Load())
	})

	t.Run("dynamic_buffer_extension", func(t *testing.T) {
		const initialCapacity int = 1
		const maxExtensions int = 3
		const maxItems int = 1 << maxExtensions

		p, err := NewQueue[item](initialCapacity, maxExtensions)
		require.NoError(t, err)
		defer p.Close()

		for i := maxItems; i > 0; i-- {
			p.Send(context.Background(), item{})
		}
		require.Equal(t, maxItems, p.Size())
	})

	t.Run("manual_buffer_extension", func(t *testing.T) {
		const initialCapacity int = 1
		const maxExtensions int = 3                      // Set to ensure limit is bypassed.
		const targetItems int = 1 << (maxExtensions + 1) // Grow once beyond max.

		p, err := NewQueue[item](initialCapacity, maxExtensions)
		require.NoError(t, err)
		defer p.Close()

		next := initialCapacity

		for i := range targetItems {
			if i == next {
				next <<= 1
				err := p.Grow(next)
				require.NoError(t, err)
			}
			p.Send(context.Background(), item{})
		}
		require.Equal(t, targetItems, p.Size())
	})

	t.Run("unbounded_buffer_extension", func(t *testing.T) {
		const initialCapacity int = 1
		const maxExtensions int = -1
		const targetExtensions int = 10
		const targetItems int = 1 << targetExtensions

		p, err := NewQueue[item](initialCapacity, maxExtensions)
		require.NoError(t, err)
		defer p.Close()

		for i := targetItems; i > 0; i-- {
			p.Send(context.Background(), item{})
		}
		require.Equal(t, targetItems, p.Size())
	})

	t.Run("invalid_capacity_returns_error", func(t *testing.T) {
		_, err := NewQueue[int](3, 0)
		require.ErrorIs(t, err, ErrInvalidCapacity)
	})

	t.Run("capacity_getter", func(t *testing.T) {
		p, err := NewQueue[int](8, 0)
		require.NoError(t, err)
		defer p.Close()

		require.Equal(t, 8, p.Capacity())
	})

	t.Run("capacity_after_grow", func(t *testing.T) {
		p, err := NewQueue[int](4, 0)
		require.NoError(t, err)
		defer p.Close()

		require.NoError(t, p.Grow(16))
		require.Equal(t, 16, p.Capacity())
	})

	t.Run("grow_invalid_returns_error", func(t *testing.T) {
		p, err := NewQueue[int](4, 0)
		require.NoError(t, err)
		defer p.Close()

		require.ErrorIs(t, p.Grow(5), ErrInvalidCapacity)
	})

	t.Run("send_cancelled_context", func(t *testing.T) {
		p, err := NewQueue[int](4, 0)
		require.NoError(t, err)
		defer p.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		ok := p.Send(ctx, 1)
		require.False(t, ok)
	})

	t.Run("recv_cancelled_context", func(t *testing.T) {
		p, err := NewQueue[int](4, 0)
		require.NoError(t, err)
		defer p.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, ok := p.Recv(ctx)
		require.False(t, ok)
	})

	t.Run("send_after_close", func(t *testing.T) {
		p, err := NewQueue[int](4, 0)
		require.NoError(t, err)

		p.Close()

		ok := p.Send(context.Background(), 1)
		require.False(t, ok)
	})

	t.Run("close_idempotent", func(t *testing.T) {
		p, err := NewQueue[int](4, 0)
		require.NoError(t, err)

		require.NoError(t, p.Close())
		require.NoError(t, p.Close())
	})

	t.Run("close_wakes_blocked_receiver", func(t *testing.T) {
		p, err := NewQueue[int](4, 0)
		require.NoError(t, err)

		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = p.Recv(context.Background())
		}()

		p.Close()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Recv did not return after Close")
		}
	})

	t.Run("seq_yields_values_then_terminates", func(t *testing.T) {
		p, err := NewQueue[int](4, 0)
		require.NoError(t, err)

		go func() {
			for i := range 3 {
				p.Send(context.Background(), i)
			}
			p.Close()
		}()

		var got []int
		for v := range p.Seq(context.Background()) {
			got = append(got, v)
		}
		require.Equal(t, []int{0, 1, 2}, got)
	})
}

func TestMustQueue_Panics(t *testing.T) {
	require.Panics(t, func() {
		MustQueue[int](3, 0)
	})
}

func TestMustQueue_Valid(t *testing.T) {
	q := MustQueue[int](4, 0)
	defer q.Close()
	require.NotNil(t, q)
}
