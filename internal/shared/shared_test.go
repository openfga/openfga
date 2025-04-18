package shared

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/singleflight"

	"github.com/openfga/openfga/internal/cachecontroller"
	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/server/config"
)

func TestSharedDatastoreResources(t *testing.T) {
	sharedCtx := context.Background()
	sharedSf := &singleflight.Group{}
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	t.Run("without", func(t *testing.T) {
		settings := config.CacheSettings{}
		s, err := NewSharedDatastoreResources(sharedCtx, sharedSf, mockDatastore, settings)
		require.NoError(t, err)
		t.Cleanup(s.Close)

		require.Equal(t, sharedCtx, s.ServerCtx)
		require.Equal(t, sharedSf, s.SingleflightGroup)
		require.Nil(t, s.CheckCache)
		require.NotNil(t, s.WaitGroup)
		require.NotNil(t, s.CacheController)
		_, ok := s.CacheController.(*cachecontroller.NoopCacheController)
		require.True(t, ok)
	})

	t.Run("with_cache", func(t *testing.T) {
		settings := config.CacheSettings{
			CheckCacheLimit:           1,
			CheckIteratorCacheEnabled: true,
		}

		s, err := NewSharedDatastoreResources(sharedCtx, sharedSf, mockDatastore, settings)
		require.NoError(t, err)
		t.Cleanup(s.Close)

		require.NotNil(t, s.CheckCache)
	})

	t.Run("with_cache_controller", func(t *testing.T) {
		settings := config.CacheSettings{
			CheckCacheLimit:           1,
			CheckIteratorCacheEnabled: true,
			CacheControllerEnabled:    true,
		}

		s, err := NewSharedDatastoreResources(sharedCtx, sharedSf, mockDatastore, settings)
		require.NoError(t, err)
		t.Cleanup(s.Close)

		require.NotNil(t, s.CacheController)
		_, ok := s.CacheController.(*cachecontroller.InMemoryCacheController)
		require.True(t, ok)
	})
}
