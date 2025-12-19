package pipe

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

const pipeBufferSize int = 128

const messageCount uint64 = 1000

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
			p := New[item](pipeBufferSize)

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
			p := New[item](pipeBufferSize)

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
			p := New[item](pipeBufferSize)

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
			p := New[item](pipeBufferSize)

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
	t.Run("single_producer_single_consumer", func(t *testing.T) {
		p := New[item](pipeBufferSize)

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
		p := New[item](pipeBufferSize)

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
		p := New[item](pipeBufferSize)

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
		p := New[item](pipeBufferSize)

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
}
