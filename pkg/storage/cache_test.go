package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestInMemoryCache(t *testing.T) {
	t.Run("set_and_get", func(t *testing.T) {
		cache := NewInMemoryLRUCache[string]()
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		defer cache.Stop()
		cache.Set("key", "value", 1*time.Second)
		result := cache.Get("key")
		require.Equal(t, "value", result)
	})

	t.Run("set_and_get_expired", func(t *testing.T) {
		cache := NewInMemoryLRUCache[string]() // can't call get/set on a closed cache store
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		defer cache.Stop()
		cache.Set("key", "value", -1*time.Nanosecond)
		result := cache.Get("key")
		require.Equal(t, "", result)
	})

	t.Run("stop_multiple_times", func(t *testing.T) {
		cache := NewInMemoryLRUCache[string]()
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})

		cache.Stop()
		cache.Stop()
	})
}
