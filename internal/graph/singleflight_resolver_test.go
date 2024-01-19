package graph

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
)

func TestSingleflightResolver(t *testing.T) {
	ds := memory.New()
	defer ds.Close()

	storeID := ulid.Make().String()

	var tuples = []*openfgav1.TupleKey{
		tuple.NewTupleKey("folder:1", "viewer", "user:jon"),
	}
	for i := 1; i <= 25; i++ {
		tuples = append(tuples, tuple.NewTupleKey(fmt.Sprintf("folder:%d", i+1), "parent", fmt.Sprintf("folder:%d", i)))
		tuples = append(tuples, tuple.NewTupleKey(fmt.Sprintf("folder:%d", i+1), "other_parent", fmt.Sprintf("folder:%d", i)))
	}

	err := ds.Write(context.Background(), storeID, nil, tuples)
	require.NoError(t, err)

	typedefs := parser.MustTransformDSLToProto(`model
	schema 1.1
	
	type user
	
	type folder
	relations
	  define parent: [folder]
	  define other_parent: [folder]
	  define viewer: [user] or viewer from parent or viewer from other_parent`).TypeDefinitions

	ctx := typesystem.ContextWithTypesystem(context.Background(), typesystem.New(
		&openfgav1.AuthorizationModel{
			Id:              ulid.Make().String(),
			TypeDefinitions: typedefs,
			SchemaVersion:   typesystem.SchemaVersion1_1,
		},
	))

	checker := NewLocalChecker(
		storagewrappers.NewCombinedTupleReader(ds, []*openfgav1.TupleKey{}),
		WithSingleflightResolver(),
	)
	defer checker.Close()

	res, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:            storeID,
		TupleKey:           tuple.NewTupleKey("folder:25", "viewer", "user:jon"),
		ContextualTuples:   nil,
		ResolutionMetadata: &ResolutionMetadata{Depth: 25},
	})

	require.NoError(t, err)
	require.True(t, res.GetResolutionMetadata().WasSharedRequest)
	require.True(t, res.GetAllowed())

	res, err = checker.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:            storeID,
		TupleKey:           tuple.NewTupleKey("folder:1", "viewer", "user:jon"),
		ContextualTuples:   nil,
		ResolutionMetadata: &ResolutionMetadata{Depth: 25},
	})

	require.NoError(t, err)
	require.False(t, res.GetResolutionMetadata().WasSharedRequest)
	require.True(t, res.GetAllowed())

	// Second time running the check will result in datastore query count being 0

	// secondLocalChecker := NewLocalChecker(
	// 	storagewrappers.NewCombinedTupleReader(ds, []*openfgav1.TupleKey{}),
	// 	WithMaxConcurrentReads(1),
	// 	WithSingleflightResolver(),
	// 	WithCachedResolver(
	// 		WithExistingCache(checkCache),
	// 		WithCacheTTL(10*time.Hour),
	// 	),
	// )

	// res, err = secondLocalChecker.ResolveCheck(ctx, &ResolveCheckRequest{
	// 	StoreID:            storeID,
	// 	TupleKey:           tuple.NewTupleKey("org:fga", "member", "user:maria"),
	// 	ContextualTuples:   nil,
	// 	ResolutionMetadata: &ResolutionMetadata{Depth: 25},
	// })

	// secondLocalChecker.Close()

	// require.NoError(t, err)
	// require.Equal(t, uint32(0), res.GetResolutionMetadata().DatastoreQueryCount)

	// // The ttuLocalChecker will use partial result from the cache and partial result from the local checker

	// ttuLocalChecker := NewLocalChecker(
	// 	storagewrappers.NewCombinedTupleReader(ds, []*openfgav1.TupleKey{}),
	// 	WithMaxConcurrentReads(1),
	// 	WithSingleflightResolver(),
	// 	WithCachedResolver(
	// 		WithExistingCache(checkCache),
	// 		WithCacheTTL(10*time.Hour),
	// 	),
	// )
	// res, err = ttuLocalChecker.ResolveCheck(ctx, &ResolveCheckRequest{
	// 	StoreID:            storeID,
	// 	TupleKey:           tuple.NewTupleKey("document:x", "ttu", "user:maria"),
	// 	ContextualTuples:   nil,
	// 	ResolutionMetadata: &ResolutionMetadata{Depth: 25},
	// })

	// ttuLocalChecker.Close()

	// require.NoError(t, err)
	// require.Equal(t, uint32(1), res.GetResolutionMetadata().DatastoreQueryCount)
}
