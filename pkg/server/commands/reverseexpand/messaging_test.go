package reverseexpand

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func feed(p *pipe) {
	for range MessageCount {
		p.send(group{})
	}
}

func consume(p *pipe, count *atomic.Uint64) {
	for {
		msg, ok := p.recv()
		if !ok {
			break
		}
		msg.done()
		count.Add(1)
	}
}

const MessageCount uint64 = 1000

func BenchmarkMessaging(b *testing.B) {
	b.Run("single_producer_single_consumer", func(b *testing.B) {
		for b.Loop() {
			p := newPipe(new(atomic.Int64))

			var count atomic.Uint64
			var wg sync.WaitGroup

			wg.Add(1)
			go func() {
				defer wg.Done()
				feed(p)
				p.close()
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				consume(p, &count)
			}()

			wg.Wait()

			require.Equal(b, MessageCount, count.Load())
		}
	})

	b.Run("multiple_producer_single_consumer", func(b *testing.B) {
		for b.Loop() {
			p := newPipe(new(atomic.Int64))

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
			p.close()
			cwg.Wait()

			require.Equal(b, MessageCount*4, count.Load())
		}
	})

	b.Run("single_producer_multiple_consumer", func(b *testing.B) {
		for b.Loop() {
			p := newPipe(new(atomic.Int64))

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
			p.close()
			cwg.Wait()

			require.Equal(b, MessageCount, count.Load())
		}
	})

	b.Run("multiple_producer_multiple_consumer", func(b *testing.B) {
		for b.Loop() {
			p := newPipe(new(atomic.Int64))

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
			p.close()
			cwg.Wait()

			require.Equal(b, MessageCount*4, count.Load())
		}
	})
}

func TestMessaging(t *testing.T) {
	t.Run("single_producer_single_consumer", func(t *testing.T) {
		p := newPipe(new(atomic.Int64))

		var count atomic.Uint64
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			feed(p)
			p.close()
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			consume(p, &count)
		}()

		wg.Wait()

		require.Equal(t, MessageCount, count.Load())
	})

	t.Run("multiple_producer_single_consumer", func(t *testing.T) {
		p := newPipe(new(atomic.Int64))

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
		p.close()
		cwg.Wait()

		require.Equal(t, MessageCount*4, count.Load())
	})

	t.Run("single_producer_multiple_consumer", func(t *testing.T) {
		p := newPipe(new(atomic.Int64))

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
		p.close()
		cwg.Wait()

		require.Equal(t, MessageCount, count.Load())
	})

	t.Run("multiple_producer_multiple_consumer", func(t *testing.T) {
		p := newPipe(new(atomic.Int64))

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
		p.close()
		cwg.Wait()

		require.Equal(t, MessageCount*4, count.Load())
	})
}
