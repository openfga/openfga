package pipe

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	pipeBufferSize int    = 1 << 7
	messageCount   uint64 = 1000
)

type item struct{}

func feed(p *Pipe[item]) {
	for range messageCount {
		p.Send(item{})
	}
}

func consume(p *Pipe[item], count *atomic.Uint64) {
	for {
		var msg item
		ok := p.Recv(&msg)
		if !ok {
			break
		}
		count.Add(1)
	}
}

func BenchmarkMessaging(b *testing.B) {
	b.Run("single_producer_single_consumer", func(b *testing.B) {
		for b.Loop() {
			p, err := New[item](pipeBufferSize)
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
			p, err := New[item](pipeBufferSize)
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
			p, err := New[item](pipeBufferSize)
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
			p, err := New[item](pipeBufferSize)
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

func TestMessaging(t *testing.T) {
	t.Run("buffer_cycles", func(t *testing.T) {
		tests := []struct {
			name   string
			size   int
			parts  int
			cycles int
		}{
			{
				name:   "small",
				size:   1 << 2,
				parts:  1,
				cycles: 10,
			},
			{
				name:   "medium",
				size:   1 << 10,
				parts:  13,
				cycles: 10,
			},
			{
				name:   "large",
				size:   1 << 20,
				parts:  333,
				cycles: 10,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				expected := make([]int, tc.size)
				for i := range tc.size {
					expected[i] = i + 1
				}

				p, err := New[int](tc.size)
				require.NoError(t, err)

				for range tc.cycles {
					var val int
					var count int

					actual := make([]int, 0, 10)

					for i := range tc.size {
						p.Send(i + 1)
						count++

						if count == tc.parts {
							for range tc.parts {
								ok := p.Recv(&val)
								require.True(t, ok)

								count--
								actual = append(actual, val)
							}
						}
					}

					for range count {
						ok := p.Recv(&val)
						require.True(t, ok)

						actual = append(actual, val)
					}

					require.Equal(t, expected, actual)
				}

				expected = make([]int, 0, tc.size)
				for i := tc.size; i > 0; i-- {
					p.Send(i)
					expected = append(expected, i)
				}

				p.Close()

				var val int

				var actual []int

				for ok := p.Recv(&val); ok; ok = p.Recv(&val) {
					actual = append(actual, val)
				}

				require.Equal(t, expected, actual)
			})
		}
	})

	t.Run("single_producer_single_consumer", func(t *testing.T) {
		p, err := New[item](pipeBufferSize)
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
		p, err := New[item](pipeBufferSize)
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
		p, err := New[item](pipeBufferSize)
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
		p, err := New[item](pipeBufferSize)
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

	t.Run("buffer_extension", func(t *testing.T) {
		const initialBufferSize int = 1
		const extendAfter time.Duration = time.Microsecond
		const maxExtensions int = 3
		const maxItems int = 1 << maxExtensions

		p, err := New[item](initialBufferSize)
		require.NoError(t, err)
		defer p.Close()

		p.SetExtensionConfig(extendAfter, maxExtensions)

		for i := maxItems; i > 0; i-- {
			p.Send(item{})
		}
		require.Equal(t, maxItems, p.Size())
	})
}
