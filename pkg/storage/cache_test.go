package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestInMemoryCache(t *testing.T) {
	cache := NewInMemoryLRUCache[string]()
	defer cache.Stop()

	t.Run("set_and_get", func(t *testing.T) {
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		defer cache.Stop()
		cache.Set("key", "value", 1*time.Second)
		result := cache.Get("key")
		require.Equal(t, "value", result)
	})

	t.Run("stop_multiple_times", func(t *testing.T) {
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})

		cache.Stop()
		cache.Stop()
	})
}
