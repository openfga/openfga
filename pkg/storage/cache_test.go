package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/openfga/openfga/internal/concurrency"
)

func TestInMemoryCache(t *testing.T) {
	t.Run("set_and_get", func(t *testing.T) {
		cache, err := NewInMemoryLRUCache[string]()
		require.NoError(t, err)
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		defer cache.Stop()
		cache.Set("key", "value", 1*time.Second)
		result := cache.Get("key")
		require.Equal(t, "value", result)
	})

	t.Run("stop_multiple_times", func(t *testing.T) {
		cache, err := NewInMemoryLRUCache[string]()
		require.NoError(t, err)
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})

		cache.Stop()
		cache.Stop()
	})

	t.Run("stop_concurrently", func(t *testing.T) {
		cache, err := NewInMemoryLRUCache[string]()
		require.NoError(t, err)
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})

		pool := concurrency.NewPool(context.Background(), 2)
		pool.Go(func(ctx context.Context) error {
			cache.Stop()
			return nil
		})
		pool.Go(func(ctx context.Context) error {
			cache.Stop()
			return nil
		})
		err = pool.Wait()
		require.NoError(t, err)
	})
}
