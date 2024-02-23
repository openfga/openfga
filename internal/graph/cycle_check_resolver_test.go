package graph

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestCycleDetectionResolver(t *testing.T) {
	ds := memory.New()
	storeID := ulid.Make().String()

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

	cycleDetectionCheckResolver := NewCycleDetectionCheckResolver()
	localCheckResolver := NewLocalChecker()

	t.Cleanup(func() {
		cycleDetectionCheckResolver.Close()
		localCheckResolver.Close()
		goleak.VerifyNone(t)
	})

	t.Run("detects_cycle_and_returns_no_error_with_local_check_resolver", func(t *testing.T) {
		cycleDetectionCheckResolver.SetDelegate(localCheckResolver)
		localCheckResolver.SetDelegate(cycleDetectionCheckResolver)

		resp, err := cycleDetectionCheckResolver.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:            storeID,
			TupleKey:           tuple.NewTupleKey("document:1", "viewer3", "user:jon"),
			ContextualTuples:   nil,
			ResolutionMetadata: &ResolutionMetadata{Depth: defaultResolveNodeLimit},
		})

		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
	})

	t.Run("detects_cycle_and_returns_no_error_with_singleflight_check_resolver", func(t *testing.T) {
		cycleDetectionCheckResolver.SetDelegate(localCheckResolver)
		localCheckResolver.SetDelegate(cycleDetectionCheckResolver)

		resp, err := cycleDetectionCheckResolver.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:            storeID,
			TupleKey:           tuple.NewTupleKey("document:1", "viewer3", "user:jon"),
			ContextualTuples:   nil,
			ResolutionMetadata: &ResolutionMetadata{Depth: defaultResolveNodeLimit},
		})

		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
	})
}
