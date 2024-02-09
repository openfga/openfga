package graph

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

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

		checkerWithSingleflight := NewLocalChecker(
			storagewrappers.NewCombinedTupleReader(ds, []*openfgav1.TupleKey{}),
			WithSingleflightResolver(),
		)
		t.Cleanup(checkerWithSingleflight.Close)

		resp, err := checkerWithSingleflight.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:            storeID,
			TupleKey:           tuple.NewTupleKey("doc:1", "a4", "user:jon"),
			ContextualTuples:   nil,
			ResolutionMetadata: &ResolutionMetadata{Depth: defaultResolveNodeLimit},
		})
		require.NoError(t, err)
		require.True(t, resp.GetAllowed())

		resp, err = checkerWithSingleflight.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:            storeID,
			TupleKey:           tuple.NewTupleKey("doc:2", "a4", "user:jon"),
			ContextualTuples:   nil,
			ResolutionMetadata: &ResolutionMetadata{Depth: defaultResolveNodeLimit},
		})
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
	})

	t.Run("Check with singleflight resolver reduces DB query count when compared to not using it", func(t *testing.T) {
		ds := memory.New()
		t.Cleanup(ds.Close)
		err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("doc:2", "a1", "user:jon"),
		})
		require.NoError(t, err)

		checkerWithSingleflight := NewLocalChecker(
			storagewrappers.NewCombinedTupleReader(ds, []*openfgav1.TupleKey{}),
			WithSingleflightResolver(),
		)
		t.Cleanup(checkerWithSingleflight.Close)

		// The results of the singleflight resolver are not deterministic.
		// For better test reliability, the test is repeated a number of times
		// and then assertions made within a reasonable threshold.
		testIterations := 3

		var dbReadsWith uint32
		for i := 0; i < testIterations; i++ {
			resp, err := checkerWithSingleflight.ResolveCheck(ctx, &ResolveCheckRequest{
				StoreID:            storeID,
				TupleKey:           tuple.NewTupleKey("doc:1", "a4", "user:jon"),
				ContextualTuples:   nil,
				ResolutionMetadata: &ResolutionMetadata{Depth: defaultResolveNodeLimit},
			})
			require.NoError(t, err)
			require.False(t, resp.GetAllowed())
			dbReadsWith += resp.GetResolutionMetadata().DatastoreQueryCount
		}

		checkerWithoutSingleflight := NewLocalChecker(
			storagewrappers.NewCombinedTupleReader(ds, []*openfgav1.TupleKey{}),
		)

		var dbReadsWithout uint32
		for i := 0; i < testIterations; i++ {
			resp, err := checkerWithoutSingleflight.ResolveCheck(ctx, &ResolveCheckRequest{
				StoreID:            storeID,
				TupleKey:           tuple.NewTupleKey("doc:1", "a4", "user:jon"),
				ContextualTuples:   nil,
				ResolutionMetadata: &ResolutionMetadata{Depth: defaultResolveNodeLimit},
			})
			require.NoError(t, err)
			require.False(t, resp.GetAllowed())
			dbReadsWithout += resp.GetResolutionMetadata().DatastoreQueryCount
		}

		require.Less(t, dbReadsWith, dbReadsWithout) //singleflight resolver will always result in fewer DB reads than without

		require.LessOrEqual(t, dbReadsWith, uint32(testIterations+2)) // A buffer of two DB reads to ensure test reliability
		require.GreaterOrEqual(t, dbReadsWith, uint32(testIterations))

		require.Equal(t, dbReadsWithout, uint32(2*testIterations))
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

		checker := NewLocalChecker(
			storagewrappers.NewCombinedTupleReader(ds, []*openfgav1.TupleKey{}),
			WithSingleflightResolver(),
		)
		t.Cleanup(checker.Close)

		resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:            storeID,
			TupleKey:           tuple.NewTupleKey("document:1", "viewer3", "user:jon"),
			ContextualTuples:   nil,
			ResolutionMetadata: &ResolutionMetadata{Depth: defaultResolveNodeLimit},
		})

		require.ErrorIs(t, err, ErrCycleDetected)
		require.Nil(t, resp)
	})
}
