package concurrency

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewPool(t *testing.T) {
	t.Run("runs_tasks_and_returns_first_error", func(t *testing.T) {
		p := NewPool(context.Background(), 2)
		require.NotNil(t, p)

		wantErr := errors.New("task failed")
		p.Go(func(context.Context) error { return nil })
		p.Go(func(context.Context) error { return wantErr })

		err := p.Wait()
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("all_succeed", func(t *testing.T) {
		p := NewPool(context.Background(), 3)
		var counter atomicCounter
		for i := 0; i < 5; i++ {
			p.Go(func(context.Context) error {
				counter.inc()
				return nil
			})
		}
		require.NoError(t, p.Wait())
		require.Equal(t, int64(5), counter.value())
	})
}

func TestRecoverFromPanic(t *testing.T) {
	t.Run("captures_panic_into_error", func(t *testing.T) {
		var err error
		func() {
			defer RecoverFromPanic(&err)
			panic("boom")
		}()
		require.Error(t, err)
		require.Contains(t, err.Error(), "recovered from panic")
		require.Contains(t, err.Error(), "boom")
	})

	t.Run("no_panic_leaves_error_nil", func(t *testing.T) {
		var err error
		func() {
			defer RecoverFromPanic(&err)
		}()
		require.NoError(t, err)
	})

	t.Run("nil_error_pointer_swallows_panic", func(t *testing.T) {
		require.NotPanics(t, func() {
			func() {
				defer RecoverFromPanic(nil)
				panic("ignored")
			}()
		})
	})
}

// atomicCounter is a tiny race-free counter for the pool test.
type atomicCounter struct {
	mu sync.Mutex
	n  int64
}

func (c *atomicCounter) inc() {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
}

func (c *atomicCounter) value() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}
