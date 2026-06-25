package graph

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
)

func TestCachedCheckResolver_AppliesOptions(t *testing.T) {
	cache, err := storage.NewInMemoryLRUCache[any]()
	require.NoError(t, err)
	t.Cleanup(cache.Stop)

	resolver, err := NewCachedCheckResolver(
		WithCacheTTL(time.Minute),
		WithExistingCache(cache),
		WithJitterPercentage(15),
		WithLogger(logger.NewNoopLogger()),
	)
	require.NoError(t, err)
	require.NotNil(t, resolver)
	t.Cleanup(resolver.Close)
}

func TestDispatchThrottlingCheckResolver_AppliesOptions(t *testing.T) {
	resolver := NewDispatchThrottlingCheckResolver(
		WithConstantRateThrottler(time.Second, "test-metric"),
	)
	require.NotNil(t, resolver)
	t.Cleanup(resolver.Close)
}
