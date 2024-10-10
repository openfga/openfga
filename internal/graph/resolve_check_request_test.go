package graph

import (
	"sync"
	"testing"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/openfga/openfga/pkg/tuple"
)

func TestCloneResolveCheckRequest(t *testing.T) {
	t.Run("non_empty_clone", func(t *testing.T) {
		contextStruct, err := structpb.NewStruct(map[string]interface{}{
			"x": 10,
		})
		require.NoError(t, err)
		orig := &ResolveCheckRequest{
			StoreID:              "12",
			AuthorizationModelID: "33",
			TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
			ContextualTuples:     []*openfgav1.TupleKey{tuple.NewTupleKey("document:def", "writer", "user:123")},
			Context:              contextStruct,
			RequestMetadata:      NewCheckRequestMetadata(20),
			VisitedPaths: map[string]struct{}{
				"abc": {},
			},
			Consistency: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
		}
		orig.GetRequestMetadata().DatastoreQueryCount++
		orig.GetRequestMetadata().DispatchCounter.Add(2)

		// First, assert the values of the orig
		require.Equal(t, "12", orig.GetStoreID())
		require.Equal(t, "33", orig.GetAuthorizationModelID())
		require.Equal(t, "user:XYZ", orig.GetTupleKey().GetUser())
		require.Equal(t, "reader", orig.GetTupleKey().GetRelation())
		require.Equal(t, "document:abc", orig.GetTupleKey().GetObject())
		require.Len(t, orig.GetContextualTuples(), 1)
		require.Equal(t, "user:123", orig.GetContextualTuples()[0].GetUser())
		require.Equal(t, "writer", orig.GetContextualTuples()[0].GetRelation())
		require.Equal(t, "document:def", orig.GetContextualTuples()[0].GetObject())
		require.Equal(t, contextStruct, orig.GetContext())
		require.Equal(t, uint32(1), orig.GetRequestMetadata().DatastoreQueryCount)
		require.Equal(t, uint32(20), orig.GetRequestMetadata().Depth)
		require.Equal(t, uint32(2), orig.GetRequestMetadata().DispatchCounter.Load())
		require.False(t, orig.GetRequestMetadata().WasThrottled.Load())
		require.Equal(t, map[string]struct{}{
			"abc": {},
		}, orig.VisitedPaths)

		// now, clone the orig and update the orig
		cloned := orig.clone()
		orig.GetRequestMetadata().DatastoreQueryCount++
		orig.GetRequestMetadata().DispatchCounter.Add(5)
		orig.VisitedPaths = map[string]struct{}{
			"abc": {},
			"xyz": {},
		}
		orig.GetRequestMetadata().WasThrottled.Store(true)

		// Assert the new values of the orig
		require.Equal(t, "12", orig.GetStoreID())
		require.Equal(t, "33", orig.GetAuthorizationModelID())
		require.Equal(t, "user:XYZ", orig.GetTupleKey().GetUser())
		require.Equal(t, "reader", orig.GetTupleKey().GetRelation())
		require.Equal(t, "document:abc", orig.GetTupleKey().GetObject())
		require.Len(t, orig.GetContextualTuples(), 1)
		require.Equal(t, "user:123", orig.GetContextualTuples()[0].GetUser())
		require.Equal(t, "writer", orig.GetContextualTuples()[0].GetRelation())
		require.Equal(t, "document:def", orig.GetContextualTuples()[0].GetObject())
		require.Equal(t, contextStruct, orig.GetContext())
		require.Equal(t, uint32(2), orig.GetRequestMetadata().DatastoreQueryCount)
		require.Equal(t, uint32(20), orig.GetRequestMetadata().Depth)
		require.Equal(t, uint32(7), orig.GetRequestMetadata().DispatchCounter.Load())
		require.True(t, orig.GetRequestMetadata().WasThrottled.Load())
		require.Equal(t, map[string]struct{}{
			"abc": {},
			"xyz": {},
		}, orig.VisitedPaths)

		require.Equal(t, "12", cloned.GetStoreID())
		require.Equal(t, "33", cloned.GetAuthorizationModelID())
		require.Equal(t, "user:XYZ", cloned.GetTupleKey().GetUser())
		require.Equal(t, "reader", cloned.GetTupleKey().GetRelation())
		require.Equal(t, "document:abc", cloned.GetTupleKey().GetObject())
		require.Len(t, cloned.GetContextualTuples(), 1)
		require.Equal(t, "user:123", cloned.GetContextualTuples()[0].GetUser())
		require.Equal(t, "writer", cloned.GetContextualTuples()[0].GetRelation())
		require.Equal(t, "document:def", cloned.GetContextualTuples()[0].GetObject())
		require.Equal(t, contextStruct, cloned.GetContext())
		require.Equal(t, uint32(1), cloned.GetRequestMetadata().DatastoreQueryCount)
		require.Equal(t, uint32(20), cloned.GetRequestMetadata().Depth)
		require.Equal(t, uint32(7), cloned.GetRequestMetadata().DispatchCounter.Load()) // note that it is intended to have the request metadata share the same dispatch counter
		require.True(t, cloned.GetRequestMetadata().WasThrottled.Load())                // it is intended to share the same was throttled state
		require.Equal(t, map[string]struct{}{
			"abc": {},
		}, cloned.VisitedPaths)
	})
	t.Run("empty_clone", func(t *testing.T) {
		var orig *ResolveCheckRequest
		r := orig.clone()
		require.Equal(t, "", r.GetStoreID())
		require.Equal(t, "", r.GetAuthorizationModelID())
		require.Nil(t, r.GetTupleKey())
		require.Nil(t, r.GetContextualTuples())
		require.Nil(t, r.GetRequestMetadata())
		require.Nil(t, r.GetContext())
		require.Equal(t, openfgav1.ConsistencyPreference_UNSPECIFIED, r.GetConsistency())
		require.Equal(t, map[string]struct{}{}, r.GetVisitedPaths())
		require.Zero(t, r.GetLastCacheInvalidationTime())
	})

	t.Run("thread_safe_clone", func(t *testing.T) {
		contextStruct, err := structpb.NewStruct(map[string]interface{}{
			"x": 10,
		})
		require.NoError(t, err)

		orig := &ResolveCheckRequest{
			StoreID:              "12",
			AuthorizationModelID: "33",
			TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
			ContextualTuples:     []*openfgav1.TupleKey{tuple.NewTupleKey("document:def", "writer", "user:123")},
			Context:              contextStruct,
			RequestMetadata:      NewCheckRequestMetadata(20),
			VisitedPaths: map[string]struct{}{
				"abc": {},
			},
			Consistency: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
		}

		const clonesCount = 100
		var wg sync.WaitGroup
		wg.Add(clonesCount)

		clones := make([]*ResolveCheckRequest, clonesCount)

		for i := 0; i < clonesCount; i++ {
			go func(i int) {
				defer wg.Done()
				cloned := orig.clone()
				// Simulate some processing time to help expose concurrency issues
				time.Sleep(10 * time.Millisecond)
				clones[i] = cloned
			}(i)
		}

		wg.Wait()

		// Ensure each clone is correct and doesn't reflect changes made to orig after cloning
		for _, cloned := range clones {
			require.Equal(t, "12", cloned.GetStoreID())
			require.Equal(t, "33", cloned.GetAuthorizationModelID())
			require.Equal(t, "user:XYZ", cloned.GetTupleKey().GetUser())
			require.Equal(t, "reader", cloned.GetTupleKey().GetRelation())
			require.Equal(t, "document:abc", cloned.GetTupleKey().GetObject())
			require.Len(t, cloned.GetContextualTuples(), 1)
			require.Equal(t, "user:123", cloned.GetContextualTuples()[0].GetUser())
			require.Equal(t, "writer", cloned.GetContextualTuples()[0].GetRelation())
			require.Equal(t, "document:def", cloned.GetContextualTuples()[0].GetObject())
			require.Equal(t, contextStruct, cloned.GetContext())
			require.Equal(t, uint32(20), cloned.GetRequestMetadata().Depth)
			require.False(t, cloned.GetRequestMetadata().WasThrottled.Load())
			require.Equal(t, map[string]struct{}{
				"abc": {},
			}, cloned.VisitedPaths)
		}
	})
}

func TestDefaultValueRequestMetadata(t *testing.T) {
	var r *ResolveCheckRequest
	require.Equal(t, "", r.GetStoreID())
	require.Equal(t, "", r.GetAuthorizationModelID())
	require.Nil(t, r.GetTupleKey())
	require.Nil(t, r.GetContextualTuples())
	require.Nil(t, r.GetRequestMetadata())
	require.Nil(t, r.GetContext())
	require.Equal(t, openfgav1.ConsistencyPreference_UNSPECIFIED, r.GetConsistency())
	require.Equal(t, map[string]struct{}{}, r.GetVisitedPaths())
	require.Zero(t, r.GetLastCacheInvalidationTime())
}
