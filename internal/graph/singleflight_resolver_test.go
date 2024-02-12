package graph

import (
	"context"
	"sync"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestSingleflightCheckResolver_ContextCancellation(t *testing.T) {
	storeID := ulid.Make().String()

	ds := memory.New()
	err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		tuple.NewTupleKey("folder:1", "folder_reader", "user:anne"),
		tuple.NewTupleKey("folder:1", "allowed", "user:anne"),
		tuple.NewTupleKey("folder:2", "parent", "folder:1"),
		tuple.NewTupleKey("folder:4", "parent", "folder:2"),
		tuple.NewTupleKey("folder:4", "blocked", "user:*"),
		tuple.NewTupleKey("folder:4", "allowed", "user:*"),
	})
	require.NoError(t, err)

	model := testutils.MustTransformDSLToProtoWithID(`model
  schema 1.1

type user
type folder
relations
	define parent: [folder]
	define folder_reader: [user] or folder_reader from parent
	define blocked: [user:*]
	define allowed: [user, user:*] or allowed from parent
	define reader: folder_reader and allowed
	define can_read: reader but not blocked
 `)

	err = ds.WriteAuthorizationModel(context.Background(), storeID, model)
	require.NoError(t, err)

	ctx := typesystem.ContextWithTypesystem(
		context.Background(),
		typesystem.New(model),
	)
	ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

	singleflightCheckResolver := NewSingleflightCheckResolver()
	localCheckResolver := NewLocalChecker()

	singleflightCheckResolver.SetDelegate(localCheckResolver)
	localCheckResolver.SetDelegate(singleflightCheckResolver)

	var wg sync.WaitGroup

	for i := 0; i < 10000; i++ {
		wg.Add(1)
		wg.Add(1)

		go func() {
			defer wg.Done()

			ctx2 := ctx

			_, err := singleflightCheckResolver.ResolveCheck(ctx2, &ResolveCheckRequest{
				StoreID:              storeID,
				AuthorizationModelID: model.GetId(),
				TupleKey:             tuple.NewTupleKey("folder:4", "can_read", "user:anne"),
				ResolutionMetadata: &ResolutionMetadata{
					Depth: 25,
				},
			})
			require.NoError(t, err)
		}()

		go func() {
			defer wg.Done()

			ctx1, cancel := context.WithCancel(ctx)

			_, err := singleflightCheckResolver.ResolveCheck(ctx1, &ResolveCheckRequest{
				StoreID:              storeID,
				AuthorizationModelID: model.GetId(),
				TupleKey:             tuple.NewTupleKey("folder:2", "can_read", "user:anne"),
				ResolutionMetadata: &ResolutionMetadata{
					Depth: 25,
				},
			})
			require.NoError(t, err)

			cancel()
		}()

		wg.Wait()
	}
}

func TestSingleflightResolver(t *testing.T) {
	storeID := ulid.Make().String()

	model := testutils.MustTransformDSLToProtoWithID(`model  
  schema 1.1  

  type user
  type doc
    relations
      define a1: [user]
	  define a2: a1
	  define a3: a2
	  define a4: a2 or a3`)

	ctx := typesystem.ContextWithTypesystem(context.Background(), typesystem.New(model))

	t.Run("Check_evaluates_correctly", func(t *testing.T) {
		ds := memory.New()
		t.Cleanup(ds.Close)
		err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("doc:1", "a1", "user:jon"),
		})
		require.NoError(t, err)

		ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

		singleflightCheckResolver := NewSingleflightCheckResolver()
		localCheckResolver := NewLocalChecker()

		singleflightCheckResolver.SetDelegate(localCheckResolver)
		localCheckResolver.SetDelegate(singleflightCheckResolver)

		t.Cleanup(localCheckResolver.Close)
		t.Cleanup(singleflightCheckResolver.Close)

		resp, err := singleflightCheckResolver.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:            storeID,
			TupleKey:           tuple.NewTupleKey("doc:1", "a4", "user:jon"),
			ContextualTuples:   nil,
			ResolutionMetadata: &ResolutionMetadata{Depth: defaultResolveNodeLimit},
		})
		require.NoError(t, err)
		require.True(t, resp.GetAllowed())

		resp, err = singleflightCheckResolver.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:            storeID,
			TupleKey:           tuple.NewTupleKey("doc:2", "a4", "user:jon"),
			ContextualTuples:   nil,
			ResolutionMetadata: &ResolutionMetadata{Depth: defaultResolveNodeLimit},
		})
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
	})

	t.Run("Check_with_singleflight_resolver_reduces_DB_query_count_when_compared_to_not_using_it", func(t *testing.T) {
		ds := memory.New()
		t.Cleanup(ds.Close)
		err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("doc:2", "a1", "user:jon"),
		})
		require.NoError(t, err)

		ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

		singleflightCheckResolver := NewSingleflightCheckResolver()
		localCheckResolver := NewLocalChecker()

		singleflightCheckResolver.SetDelegate(localCheckResolver)
		localCheckResolver.SetDelegate(singleflightCheckResolver)

		t.Cleanup(localCheckResolver.Close)
		t.Cleanup(singleflightCheckResolver.Close)

		// The results of the singleflight resolver are not deterministic.
		// For better test reliability, the test is repeated a number of times
		// and then assertions made within a reasonable threshold.
		testIterations := 5

		// We know that given this model and the specific tuples, that there are
		// expected to be two DB reads when singleflight is not used. We then use
		// this expected number to derive the difference of DB queries saved when
		// using the singleflight resolver below.
		expectedNumDBReads := 2

		expectedOptimizedNumDBReads := expectedNumDBReads - 1 // Expect singleflight to reduce DB calls by one (usually)

		//var dbReadsWith uint32
		var numFewerDBQueries int
		for i := 0; i < testIterations; i++ {
			resp, err := singleflightCheckResolver.ResolveCheck(ctx, &ResolveCheckRequest{
				StoreID:            storeID,
				TupleKey:           tuple.NewTupleKey("doc:1", "a4", "user:jon"),
				ContextualTuples:   nil,
				ResolutionMetadata: &ResolutionMetadata{Depth: defaultResolveNodeLimit},
			})
			require.NoError(t, err)
			require.False(t, resp.GetAllowed())

			require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(expectedNumDBReads))
			require.GreaterOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(expectedOptimizedNumDBReads))

			numFewerDBQueries += expectedNumDBReads - int(resp.GetResolutionMetadata().DatastoreQueryCount)
		}

		require.Greater(t, numFewerDBQueries, 0) //singleflight resolver will always result in fewer DB reads than without
	})

	t.Run("cyclic relationship detected", func(t *testing.T) {
		ds := memory.New()

		err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer3", "document:1#viewer3"),
		})
		require.NoError(t, err)

		model := testutils.MustTransformDSLToProtoWithID(`model
		schema 1.1
	  
	  type user
	  
	  type document
		relations
			define viewer1: [user, document#viewer1]
			define viewer2: viewer1 or viewer2
			define viewer3: viewer1 or viewer2`)

		ctx := typesystem.ContextWithTypesystem(context.Background(), typesystem.New(model))

		ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

		singleflightCheckResolver := NewSingleflightCheckResolver()
		localCheckResolver := NewLocalChecker()

		singleflightCheckResolver.SetDelegate(localCheckResolver)
		localCheckResolver.SetDelegate(singleflightCheckResolver)

		t.Cleanup(localCheckResolver.Close)
		t.Cleanup(singleflightCheckResolver.Close)

		resp, err := singleflightCheckResolver.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:            storeID,
			TupleKey:           tuple.NewTupleKey("document:1", "viewer3", "user:jon"),
			ContextualTuples:   nil,
			ResolutionMetadata: &ResolutionMetadata{Depth: defaultResolveNodeLimit},
		})

		require.ErrorIs(t, err, ErrCycleDetected)
		require.Nil(t, resp)
	})
}
