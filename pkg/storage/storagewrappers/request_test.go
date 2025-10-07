package storagewrappers

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/internal/shared"
	"github.com/openfga/openfga/internal/utils/apimethod"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage/storagewrappers/sharediterator"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestRequestStorageWrapper(t *testing.T) {
	const maxConcurrentReads = 1000

	t.Run("check_api_with_caching_on", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		requestContextualTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("doc:1", "viewer", "user:maria"),
		}

		br := NewRequestStorageWrapperWithCache(mockDatastore, requestContextualTuples, &Operation{Concurrency: maxConcurrentReads, Method: apimethod.Check},
			DataResourceConfiguration{
				Resources: &shared.SharedDatastoreResources{
					CheckCache: mockCache,
					Logger:     logger.NewNoopLogger(),
				},
				CacheSettings: config.CacheSettings{
					CheckIteratorCacheEnabled: true,
					CheckCacheLimit:           1,
				},
			},
		)
		require.NotNil(t, br)

		// assert on the chain
		a, ok := br.RelationshipTupleReader.(*CombinedTupleReader)
		require.True(t, ok)

		c, ok := a.RelationshipTupleReader.(*CachedDatastore)
		// require.Equal(t, mockCache, c.cache)
		// require.Equal(t, sf, c.sf)
		// require.Equal(t, 1000, c.maxResultSize)
		// require.Equal(t, 10*time.Second, c.ttl)
		require.True(t, ok)

		d, ok := c.RelationshipTupleReader.(*BoundedTupleReader)
		require.Equal(t, maxConcurrentReads, cap(d.limiter))
		require.True(t, ok)
	})

	t.Run("check_api_with_caching_shared_iterator_on", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		requestContextualTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("doc:1", "viewer", "user:maria"),
		}

		sharedIteratorStorage := sharediterator.NewSharedIteratorDatastoreStorage()

		br := NewRequestStorageWrapperWithCache(mockDatastore, requestContextualTuples, &Operation{Concurrency: maxConcurrentReads, Method: apimethod.Check},
			DataResourceConfiguration{
				Resources: &shared.SharedDatastoreResources{
					CheckCache:            mockCache,
					Logger:                logger.NewNoopLogger(),
					SharedIteratorStorage: sharedIteratorStorage,
				},
				CacheSettings: config.CacheSettings{
					CheckIteratorCacheEnabled: true,
					CheckCacheLimit:           1,
					SharedIteratorEnabled:     true,
				},
			},
		)
		require.NotNil(t, br)

		// assert on the chain
		a, ok := br.RelationshipTupleReader.(*CombinedTupleReader)
		require.True(t, ok)

		c, ok := a.RelationshipTupleReader.(*sharediterator.IteratorDatastore)
		require.True(t, ok)

		d, ok := c.RelationshipTupleReader.(*CachedDatastore)
		require.True(t, ok)

		e, ok := d.RelationshipTupleReader.(*BoundedTupleReader)
		require.Equal(t, maxConcurrentReads, cap(e.limiter))
		require.True(t, ok)
	})

	t.Run("check_api_with_caching_off", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)

		requestContextualTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("doc:1", "viewer", "user:maria"),
		}

		br := NewRequestStorageWrapperWithCache(
			mockDatastore,
			requestContextualTuples,
			&Operation{
				Method:      apimethod.Check,
				Concurrency: maxConcurrentReads,
			},
			DataResourceConfiguration{
				Resources:     &shared.SharedDatastoreResources{Logger: logger.NewNoopLogger()},
				CacheSettings: config.CacheSettings{},
			},
		)
		require.NotNil(t, br)

		// assert on the chain
		a, ok := br.RelationshipTupleReader.(*CombinedTupleReader)
		require.True(t, ok)

		c, ok := a.RelationshipTupleReader.(*BoundedTupleReader)
		require.Equal(t, maxConcurrentReads, cap(c.limiter))
		require.True(t, ok)
	})

	t.Run("list_apis", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)

		requestContextualTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("doc:1", "viewer", "user:maria"),
		}

		br := NewRequestStorageWrapper(mockDatastore, requestContextualTuples,
			&Operation{Concurrency: maxConcurrentReads,
				Method: apimethod.ListObjects,
			})
		require.NotNil(t, br)

		// assert on the chain
		a, ok := br.RelationshipTupleReader.(*CombinedTupleReader)
		require.True(t, ok)

		c, ok := a.RelationshipTupleReader.(*BoundedTupleReader)
		require.Equal(t, maxConcurrentReads, cap(c.limiter))
		require.True(t, ok)
	})

	t.Run("list_apis_with_cache", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		requestContextualTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("doc:1", "viewer", "user:maria"),
		}

		br := NewRequestStorageWrapperWithCache(mockDatastore, requestContextualTuples,
			&Operation{Concurrency: maxConcurrentReads, Method: apimethod.ListObjects},
			DataResourceConfiguration{
				Resources: &shared.SharedDatastoreResources{
					CheckCache: mockCache,
					Logger:     logger.NewNoopLogger(),
				},
				CacheSettings: config.CacheSettings{
					ListObjectsIteratorCacheEnabled:    true,
					CheckCacheLimit:                    1,
					ListObjectsIteratorCacheMaxResults: 1,
				},
			},
		)
		require.NotNil(t, br)

		// assert on the chain
		a, ok := br.RelationshipTupleReader.(*CombinedTupleReader)
		require.True(t, ok)

		c, ok := a.RelationshipTupleReader.(*CachedDatastore)
		require.True(t, ok)

		d, ok := c.RelationshipTupleReader.(*BoundedTupleReader)
		require.Equal(t, maxConcurrentReads, cap(d.limiter))
		require.True(t, ok)
	})

	t.Run("list_apis_with_shadow_cache", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockCache := mocks.NewMockInMemoryCache[any](ctrl)

		requestContextualTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("doc:1", "viewer", "user:maria"),
		}

		br := NewRequestStorageWrapperWithCache(mockDatastore, requestContextualTuples,
			&Operation{Concurrency: maxConcurrentReads, Method: apimethod.ListObjects},
			DataResourceConfiguration{
				Resources: &shared.SharedDatastoreResources{
					CheckCache: mockCache,
					Logger:     logger.NewNoopLogger(),
				},
				CacheSettings: config.CacheSettings{
					ListObjectsIteratorCacheEnabled:    true,
					CheckCacheLimit:                    1,
					ListObjectsIteratorCacheMaxResults: 1,
				},
			},
		)
		require.NotNil(t, br)

		// assert on the chain
		a, ok := br.RelationshipTupleReader.(*CombinedTupleReader)
		require.True(t, ok)

		c, ok := a.RelationshipTupleReader.(*CachedDatastore)
		require.True(t, ok)

		d, ok := c.RelationshipTupleReader.(*BoundedTupleReader)
		require.Equal(t, maxConcurrentReads, cap(d.limiter))
		require.True(t, ok)
	})
}
