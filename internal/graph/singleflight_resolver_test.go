package graph

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/storagewrappers"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestSingleflightResolver(t *testing.T) {
	ds := memory.New()
	t.Cleanup(ds.Close)

	storeID := ulid.Make().String()

	var tuples = []*openfgav1.TupleKey{
		tuple.NewTupleKey("folder:1", "viewer", "user:jon"),

		tuple.NewTupleKey("folder:2", "parent1", "folder:1"),
		tuple.NewTupleKey("folder:2", "parent2", "folder:1"),

		tuple.NewTupleKey("folder:3", "parent1", "folder:2"),
		tuple.NewTupleKey("folder:3", "parent2", "folder:2"),
	}

	err := ds.Write(context.Background(), storeID, nil, tuples)
	require.NoError(t, err)

	model := testutils.MustTransformDSLToProtoWithID(`model
	schema 1.1

	type user

	type folder
	relations
	  define parent1: [folder]
	  define parent2: [folder]
	  define viewer: [user] or viewer from parent1 or viewer from parent2 or viewer`)

	ctx := typesystem.ContextWithTypesystem(context.Background(), typesystem.New(model))

	checker := NewLocalChecker(
		storagewrappers.NewCombinedTupleReader(ds, []*openfgav1.TupleKey{}),
	)
	t.Cleanup(checker.Close)

	resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:            storeID,
		TupleKey:           tuple.NewTupleKey("folder:3", "viewer", "user:jon"),
		ContextualTuples:   nil,
		ResolutionMetadata: &ResolutionMetadata{Depth: defaultResolveNodeLimit},
	})

	require.NoError(t, err)
	require.True(t, resp.GetAllowed())
	require.GreaterOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(4))
	require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(7))
}
func TestSingleflightResolverVersusWithout(t *testing.T) {
	ds := memory.New()
	t.Cleanup(ds.Close)

	storeID := ulid.Make().String()

	var tuples = []*openfgav1.TupleKey{
		tuple.NewTupleKey("folder:1", "viewer", "user:jon"),
	}
	for i := 1; i <= 15; i++ {
		tuples = append(tuples, tuple.NewTupleKey(fmt.Sprintf("folder:%d", i+1), "parent1", fmt.Sprintf("folder:%d", i)))
		tuples = append(tuples, tuple.NewTupleKey(fmt.Sprintf("folder:%d", i+1), "parent2", fmt.Sprintf("folder:%d", i)))
		tuples = append(tuples, tuple.NewTupleKey(fmt.Sprintf("folder:%d", i+1), "parent3", fmt.Sprintf("folder:%d", i)))
	}

	err := ds.Write(context.Background(), storeID, nil, tuples)
	require.NoError(t, err)

	model := testutils.MustTransformDSLToProtoWithID(`model  
    schema 1.1  

type user  

type folder  
    relations  
    define parent1: [folder]  
    define parent2: [folder]  
    define parent3: [folder]  
    define viewer: [user] or viewer from parent1 or viewer from parent2 or viewer from parent3`)

	ctx := typesystem.ContextWithTypesystem(context.Background(), typesystem.New(model))

	checkReq := ResolveCheckRequest{
		StoreID:            storeID,
		TupleKey:           tuple.NewTupleKey("folder:10", "viewer", "user:jon"),
		ContextualTuples:   nil,
		ResolutionMetadata: &ResolutionMetadata{Depth: defaultResolveNodeLimit},
	}

	checkerWithoutSingleflight := NewLocalChecker(
		storagewrappers.NewCombinedTupleReader(ds, []*openfgav1.TupleKey{}),
	)

	req := checkReq
	respWithoutSingleflight, err := checkerWithoutSingleflight.ResolveCheck(ctx, &req)

	require.NoError(t, err)
	require.True(t, respWithoutSingleflight.GetAllowed())

	checkerWithSingleflight := NewLocalChecker(
		storagewrappers.NewCombinedTupleReader(ds, []*openfgav1.TupleKey{}),
		WithSingleflightResolver(),
	)
	t.Cleanup(checkerWithSingleflight.Close)

	req = checkReq
	resWithSingleflight, err := checkerWithSingleflight.ResolveCheck(ctx, &req)

	require.NoError(t, err)
	require.True(t, resWithSingleflight.GetAllowed())
	require.LessOrEqual(t, resWithSingleflight.GetResolutionMetadata().DatastoreQueryCount, respWithoutSingleflight.GetResolutionMetadata().DatastoreQueryCount)
}

func TestSingleflightResolverWithCycle(t *testing.T) {
	ds := memory.New()
	t.Cleanup(ds.Close)

	storeID := ulid.Make().String()

	var tuples = []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer3", "document:1#viewer3"),
	}

	err := ds.Write(context.Background(), storeID, nil, tuples)
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
}
