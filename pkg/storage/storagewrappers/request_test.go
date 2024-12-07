package storagewrappers

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/internal/shared"
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

		br := NewRequestStorageWrapperForCheckAPI(mockDatastore, requestContextualTuples, maxConcurrentReads,
			&shared.SharedResources{
				CheckCache: mockCache,
			}, config.CacheSettings{
				CheckIteratorCacheEnabled: true,
				CheckCacheLimit:           1,
			})
		require.NotNil(t, br)

		// assert on the chain
		a, ok := br.RelationshipTupleReader.(*CombinedTupleReader)
		require.Equal(t, requestContextualTuples, a.contextualTuples)
		require.True(t, ok)

		b, ok := a.RelationshipTupleReader.(*InstrumentedOpenFGAStorage)
		require.True(t, ok)

		c, ok := b.RelationshipTupleReader.(*graph.CachedDatastore)
		// require.Equal(t, mockCache, c.cache)
		// require.Equal(t, sf, c.sf)
		// require.Equal(t, 1000, c.maxResultSize)
		// require.Equal(t, 10*time.Second, c.ttl)
		require.True(t, ok)

		d, ok := c.RelationshipTupleReader.(*BoundedConcurrencyTupleReader)
		require.Equal(t, maxConcurrentReads, cap(d.limiter))
		require.True(t, ok)
	})

	t.Run("check_api_with_caching_off", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)

		requestContextualTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("doc:1", "viewer", "user:maria"),
		}

		br := NewRequestStorageWrapperForCheckAPI(mockDatastore, requestContextualTuples, maxConcurrentReads, &shared.SharedResources{}, config.CacheSettings{})
		require.NotNil(t, br)

		// assert on the chain
		a, ok := br.RelationshipTupleReader.(*CombinedTupleReader)
		require.Equal(t, requestContextualTuples, a.contextualTuples)
		require.True(t, ok)

		b, ok := a.RelationshipTupleReader.(*InstrumentedOpenFGAStorage)
		require.True(t, ok)

		c, ok := b.RelationshipTupleReader.(*BoundedConcurrencyTupleReader)
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

		br := NewRequestStorageWrapperForListAPIs(mockDatastore, requestContextualTuples, maxConcurrentReads)
		require.NotNil(t, br)

		// assert on the chain
		a, ok := br.RelationshipTupleReader.(*CombinedTupleReader)
		require.Equal(t, requestContextualTuples, a.contextualTuples)
		require.True(t, ok)

		b, ok := a.RelationshipTupleReader.(*InstrumentedOpenFGAStorage)
		require.True(t, ok)

		c, ok := b.RelationshipTupleReader.(*BoundedConcurrencyTupleReader)
		require.Equal(t, maxConcurrentReads, cap(c.limiter))
		require.True(t, ok)
	})
}
