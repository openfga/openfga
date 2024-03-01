package graph

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

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
			StoreID:            uuid.NewString(),
			TupleKey:           cyclicalTuple,
			ResolutionMetadata: &ResolutionMetadata{Depth: defaultResolveNodeLimit},
			VisitedPaths:       visitedPaths,
			DispatchCounter:    &atomic.Uint32{},
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
			StoreID:            uuid.NewString(),
			TupleKey:           tuple.NewTupleKey("document:1", "viewer", "user:will"),
			ResolutionMetadata: &ResolutionMetadata{Depth: defaultResolveNodeLimit},
			VisitedPaths:       map[string]struct{}{},
			DispatchCounter:    &atomic.Uint32{},
		})

		require.NoError(t, err)
		require.True(t, resp.GetAllowed())
	})
}
