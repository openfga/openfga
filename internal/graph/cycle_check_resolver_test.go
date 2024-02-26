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

func TestCycleDetectionCheckResolver(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ds := memory.New()
	t.Cleanup(ds.Close)
	storeID := ulid.Make().String()

	err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer3", "document:1#viewer3"),
		tuple.NewTupleKey("document:1", "viewer1", "user:1"),
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
	t.Cleanup(cycleDetectionCheckResolver.Close)
	localCheckResolver := NewLocalChecker()
	t.Cleanup(localCheckResolver.Close)

	t.Run("detects_cycle_and_returns_no_error", func(t *testing.T) {
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

	t.Run("correctly_undetects_cycle_and_returns_no_error", func(t *testing.T) {
		cycleDetectionCheckResolver.SetDelegate(localCheckResolver)
		localCheckResolver.SetDelegate(cycleDetectionCheckResolver)

		resp, err := cycleDetectionCheckResolver.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:            storeID,
			TupleKey:           tuple.NewTupleKey("document:1", "viewer1", "user:1"),
			ContextualTuples:   nil,
			ResolutionMetadata: &ResolutionMetadata{Depth: defaultResolveNodeLimit},
		})

		require.NoError(t, err)
		require.True(t, resp.GetAllowed())
	})
}
