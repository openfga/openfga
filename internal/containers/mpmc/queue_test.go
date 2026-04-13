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
	testMessageCount  uint64 = 200
	benchMessageCount uint64 = 1000
)

type item struct{}

func feedN(p *Queue[item], n uint64) {
	for range n {
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
				feedN(p, benchMessageCount)
				p.Close()
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				consume(p, &count)
			}()

			wg.Wait()

			require.Equal(b, benchMessageCount, count.Load())
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
					feedN(p, benchMessageCount)
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

			require.Equal(b, benchMessageCount*4, count.Load())
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
				feedN(p, benchMessageCount)
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

			require.Equal(b, benchMessageCount, count.Load())
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
					feedN(p, benchMessageCount)
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

			require.Equal(b, benchMessageCount*4, count.Load())
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
				capacity: 1 << 14,
				parts:    333,
				cycles:   3,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				expected := make([]int, tc.capacity)
				for i := range tc.capacity {
					expected[i] = i + 1
				}

				p, err := NewQueue[int](tc.capacity, 0)
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
			feedN(p, testMessageCount)
			p.Close()
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			consume(p, &count)
		}()

		wg.Wait()

		require.Equal(t, testMessageCount, count.Load())
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
				feedN(p, testMessageCount)
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

		require.Equal(t, testMessageCount*4, count.Load())
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
			feedN(p, testMessageCount)
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

		require.Equal(t, testMessageCount, count.Load())
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
				feedN(p, testMessageCount)
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

		require.Equal(t, testMessageCount*4, count.Load())
	})

	t.Run("dynamic_buffer_extension", func(t *testing.T) {
		const initialCapacity int = 2
		const maxExtensions int = 3
		const maxItems int = initialCapacity << maxExtensions

		p, err := NewQueue[item](initialCapacity, maxExtensions)
		require.NoError(t, err)
		defer p.Close()

		for i := maxItems; i > 0; i-- {
			p.Send(context.Background(), item{})
		}
		require.Equal(t, maxItems, p.Size())
	})

	t.Run("manual_buffer_extension", func(t *testing.T) {
		const initialCapacity int = 2
		const maxExtensions int = 3                      // Set to ensure limit is bypassed.
		const targetItems int = 1 << (maxExtensions + 2) // Grow once beyond max.

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
		const initialCapacity int = 2
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

	t.Run("capacity_one_returns_error", func(t *testing.T) {
		_, err := NewQueue[int](1, 0)
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

		p.Close()
		p.Close()
	})

	t.Run("close_wakes_blocked_receiver", func(t *testing.T) {
		p, err := NewQueue[int](4, 0)
		require.NoError(t, err)

		type result struct {
			value int
			ok    bool
		}
		ch := make(chan result, 1)
		go func() {
			v, ok := p.Recv(context.Background())
			ch <- result{v, ok}
		}()

		p.Close()

		select {
		case r := <-ch:
			require.False(t, r.ok)
			require.Zero(t, r.value)
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

	t.Run("seq_break_closes_queue", func(t *testing.T) {
		p, err := NewQueue[int](8, 0)
		require.NoError(t, err)

		for i := range 5 {
			p.Send(context.Background(), i)
		}

		var got []int
		for v := range p.Seq(context.Background()) {
			got = append(got, v)
			if len(got) == 2 {
				break
			}
		}
		require.Equal(t, []int{0, 1}, got)

		// Seq defers Close, so Send should return false.
		ok := p.Send(context.Background(), 99)
		require.False(t, ok)
	})

	t.Run("fifo_ordering_spsc", func(t *testing.T) {
		const n = 256
		p, err := NewQueue[int](16, 0)
		require.NoError(t, err)

		go func() {
			for i := range n {
				p.Send(context.Background(), i)
			}
			p.Close()
		}()

		got := make([]int, 0, n)
		for v := range p.Seq(context.Background()) {
			got = append(got, v)
		}

		require.Len(t, got, n)
		for i, v := range got {
			require.Equal(t, i, v)
		}
	})

	t.Run("send_blocks_on_full_then_wakes", func(t *testing.T) {
		p, err := NewQueue[int](2, 0)
		require.NoError(t, err)
		defer p.Close()

		// Fill the buffer.
		p.Send(context.Background(), 1)
		p.Send(context.Background(), 2)

		sent := make(chan bool, 1)
		go func() {
			sent <- p.Send(context.Background(), 3)
		}()

		// Sender should be parked; give it time to block.
		select {
		case <-sent:
			t.Fatal("Send returned before Recv freed a slot")
		case <-time.After(50 * time.Millisecond):
		}

		// Free a slot.
		v, ok := p.Recv(context.Background())
		require.True(t, ok)
		require.Equal(t, 1, v)

		select {
		case result := <-sent:
			require.True(t, result)
		case <-time.After(time.Second):
			t.Fatal("Send did not wake after Recv")
		}
	})

	t.Run("close_wakes_blocked_sender", func(t *testing.T) {
		p, err := NewQueue[int](2, 0)
		require.NoError(t, err)

		p.Send(context.Background(), 1)
		p.Send(context.Background(), 2)

		sent := make(chan bool, 1)
		go func() {
			sent <- p.Send(context.Background(), 3)
		}()

		// Let the sender block.
		select {
		case <-sent:
			t.Fatal("Send returned before Close")
		case <-time.After(50 * time.Millisecond):
		}

		p.Close()

		select {
		case ok := <-sent:
			require.False(t, ok)
		case <-time.After(time.Second):
			t.Fatal("Send did not return after Close")
		}
	})

	t.Run("recv_drains_after_close", func(t *testing.T) {
		p, err := NewQueue[int](8, 0)
		require.NoError(t, err)

		for i := range 4 {
			p.Send(context.Background(), i+1)
		}
		p.Close()

		var got []int
		for {
			v, ok := p.Recv(context.Background())
			if !ok {
				break
			}
			got = append(got, v)
		}
		require.Equal(t, []int{1, 2, 3, 4}, got)
	})

	t.Run("recv_returns_zero_when_closed_and_empty", func(t *testing.T) {
		p, err := NewQueue[int](4, 0)
		require.NoError(t, err)

		p.Close()

		v, ok := p.Recv(context.Background())
		require.False(t, ok)
		require.Zero(t, v)
	})

	t.Run("send_context_cancelled_while_blocked", func(t *testing.T) {
		p, err := NewQueue[int](2, 0)
		require.NoError(t, err)
		defer p.Close()

		p.Send(context.Background(), 1)
		p.Send(context.Background(), 2)

		ctx, cancel := context.WithCancel(context.Background())
		sent := make(chan bool, 1)
		go func() {
			sent <- p.Send(ctx, 3)
		}()

		// Let the sender park.
		select {
		case <-sent:
			t.Fatal("Send returned before cancel")
		case <-time.After(50 * time.Millisecond):
		}

		cancel()

		select {
		case ok := <-sent:
			require.False(t, ok)
		case <-time.After(time.Second):
			t.Fatal("Send did not return after context cancel")
		}
	})

	t.Run("recv_context_cancelled_while_blocked", func(t *testing.T) {
		p, err := NewQueue[int](4, 0)
		require.NoError(t, err)
		defer p.Close()

		ctx, cancel := context.WithCancel(context.Background())

		type result struct {
			value int
			ok    bool
		}
		ch := make(chan result, 1)
		go func() {
			v, ok := p.Recv(ctx)
			ch <- result{v, ok}
		}()

		// Let the receiver park.
		select {
		case <-ch:
			t.Fatal("Recv returned before cancel")
		case <-time.After(50 * time.Millisecond):
		}

		cancel()

		select {
		case r := <-ch:
			require.False(t, r.ok)
			require.Zero(t, r.value)
		case <-time.After(time.Second):
			t.Fatal("Recv did not return after context cancel")
		}
	})

	t.Run("grow_noop_when_sufficient", func(t *testing.T) {
		p, err := NewQueue[int](8, 0)
		require.NoError(t, err)
		defer p.Close()

		require.NoError(t, p.Grow(4))
		require.Equal(t, 8, p.Capacity())

		require.NoError(t, p.Grow(8))
		require.Equal(t, 8, p.Capacity())
	})

	t.Run("size_reflects_sends_and_recvs", func(t *testing.T) {
		p, err := NewQueue[int](8, 0)
		require.NoError(t, err)
		defer p.Close()

		require.Equal(t, 0, p.Size())

		p.Send(context.Background(), 1)
		p.Send(context.Background(), 2)
		p.Send(context.Background(), 3)
		require.Equal(t, 3, p.Size())

		p.Recv(context.Background())
		require.Equal(t, 2, p.Size())

		p.Recv(context.Background())
		p.Recv(context.Background())
		require.Equal(t, 0, p.Size())
	})

	t.Run("slot_data_zeroed_after_recv", func(t *testing.T) {
		p, err := NewQueue[*int](4, 0)
		require.NoError(t, err)
		defer p.Close()

		v := 42
		p.Send(context.Background(), &v)

		got, ok := p.Recv(context.Background())
		require.True(t, ok)
		require.Equal(t, 42, *got)

		// The slot that held the pointer should now contain nil,
		// verifiable by inspecting the internal data slice.
		require.Nil(t, p.data[0].Data)
	})

	t.Run("extend_preserves_data_integrity", func(t *testing.T) {
		p, err := NewQueue[int](2, -1)
		require.NoError(t, err)
		defer p.Close()

		// Fill and trigger an extension.
		p.Send(context.Background(), 10)
		p.Send(context.Background(), 20)
		p.Send(context.Background(), 30) // triggers extend to 4

		require.Equal(t, 4, p.Capacity())

		v1, ok := p.Recv(context.Background())
		require.True(t, ok)
		require.Equal(t, 10, v1)

		v2, ok := p.Recv(context.Background())
		require.True(t, ok)
		require.Equal(t, 20, v2)

		v3, ok := p.Recv(context.Background())
		require.True(t, ok)
		require.Equal(t, 30, v3)
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
