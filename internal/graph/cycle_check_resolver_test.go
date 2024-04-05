package graph

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/typesystem"

	"github.com/openfga/openfga/pkg/tuple"
)

func TestCycleDetectionCheckResolver(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()

	cycleDetectionCheckResolver := NewCycleDetectionCheckResolver()
	t.Cleanup(cycleDetectionCheckResolver.Close)

	t.Run("detects_cycle_and_returns_cycle_detected_error", func(t *testing.T) {
		cyclicalTuple := tuple.NewTupleKey("document:1", "viewer", "user:will")

		visitedPaths := make(map[string]struct{}, 0)
		visitedPaths[tuple.TupleKeyToString(cyclicalTuple)] = struct{}{}

		resp, err := cycleDetectionCheckResolver.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:         uuid.NewString(),
			TupleKey:        cyclicalTuple,
			RequestMetadata: NewCheckRequestMetadata(defaultResolveNodeLimit),
			VisitedPaths:    visitedPaths,
		})

		require.ErrorIs(t, err, ErrCycleDetected)
		require.False(t, resp.GetAllowed())
	})

	t.Run("no_cycle_detected_delegates_request", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		mockLocalChecker := NewMockCheckResolver(ctrl)
		mockLocalChecker.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Return(&ResolveCheckResponse{
			Allowed: true,
		}, nil).Times(1)
		cycleDetectionCheckResolver.SetDelegate(mockLocalChecker)

		resp, err := cycleDetectionCheckResolver.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:         uuid.NewString(),
			TupleKey:        tuple.NewTupleKey("document:1", "viewer", "user:will"),
			RequestMetadata: NewCheckRequestMetadata(defaultResolveNodeLimit),
			VisitedPaths:    map[string]struct{}{},
		})

		require.NoError(t, err)
		require.True(t, resp.GetAllowed())
	})
}

func TestIntegrationWithLocalChecker(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	cycleDetectionCheckResolver := NewCycleDetectionCheckResolver()
	t.Cleanup(cycleDetectionCheckResolver.Close)
	localCheckResolver := NewLocalChecker()
	t.Cleanup(localCheckResolver.Close)

	cycleDetectionCheckResolver.SetDelegate(localCheckResolver)
	localCheckResolver.SetDelegate(cycleDetectionCheckResolver)

	ds := memory.New()
	t.Cleanup(ds.Close)

	storeID := ulid.Make().String()

	model := parser.MustTransformDSLToProto(`
		model
		  schema 1.1

		type user

		type group
		  relations
			define blocked: [user, group#member]
			define member: [user, group#member] but not blocked
`)

	err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		tuple.NewTupleKey("group:1", "member", "user:jon"),
		tuple.NewTupleKey("group:1", "blocked", "group:1#member"),
	})
	require.NoError(t, err)

	typesys, err := typesystem.NewAndValidate(
		context.Background(),
		model,
	)
	require.NoError(t, err)

	ctx := storage.ContextWithRelationshipTupleReader(context.Background(), ds)
	ctx = typesystem.ContextWithTypesystem(ctx, typesys)
	resp, err := cycleDetectionCheckResolver.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: model.GetId(),
		TupleKey:             tuple.NewTupleKey("group:1", "blocked", "user:jon"),
		RequestMetadata:      NewCheckRequestMetadata(25),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAllowed())
}
