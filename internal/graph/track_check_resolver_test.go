package graph

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestIntegrationTracker(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := logger.NewNoopLogger()
	cycleDetectionCheckResolver := NewCycleDetectionCheckResolver()
	t.Cleanup(cycleDetectionCheckResolver.Close)

	localChecker := NewLocalChecker()
	t.Cleanup(localChecker.Close)

	trackChecker := NewTrackCheckResolver(
		WithTrackerContext(ctx),
		WithTrackerLogger(logger))
	t.Cleanup(trackChecker.Close)

	cycleDetectionCheckResolver.SetDelegate(trackChecker)
	trackChecker.SetDelegate(localChecker)
	localChecker.SetDelegate(cycleDetectionCheckResolver)

	t.Run("tracker_delegates_request", func(t *testing.T) {
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

		err := ds.Write(
			ctx,
			storeID,
			nil,
			[]*openfgav1.TupleKey{
				tuple.NewTupleKey("group:1", "member", "user:jon"),
				tuple.NewTupleKey("group:2", "blocked", "group:1#member"),
				tuple.NewTupleKey("group:3", "blocked", "group:1#member"),
			})
		require.NoError(t, err)

		typesys, err := typesystem.NewAndValidate(
			context.Background(),
			model,
		)
		require.NoError(t, err)

		ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)
		ctx = typesystem.ContextWithTypesystem(ctx, typesys)
		resp, err := trackChecker.ResolveCheck(ctx, &ResolveCheckRequest{
			AuthorizationModelID: ulid.Make().String(),
			StoreID:              storeID,
			TupleKey:             tuple.NewTupleKey("group:1", "blocked", "user:jon"),
			RequestMetadata:      NewCheckRequestMetadata(25),
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
	})

	t.Run("tracker_delegates_request", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		mockLocalChecker := NewMockCheckResolver(ctrl)
		mockLocalChecker.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Return(&ResolveCheckResponse{
			Allowed: true,
		}, nil).Times(1)
		trackChecker.SetDelegate(mockLocalChecker)

		resp, err := trackChecker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:         ulid.Make().String(),
			TupleKey:        tuple.NewTupleKey("document:1", "viewer", "user:will"),
			RequestMetadata: NewCheckRequestMetadata(defaultResolveNodeLimit),
			VisitedPaths:    map[string]struct{}{},
		})

		require.NoError(t, err)
		require.True(t, resp.GetAllowed())
	})

	t.Run("tracker_user_type", func(t *testing.T) {
		userType := trackChecker.userType("group:1#member")
		require.Equal(t, "userset", userType)

		userType = trackChecker.userType("user:ann")
		require.Equal(t, "user", userType)

		userType = trackChecker.userType("user:*")
		require.Equal(t, "userset", userType)
	})

	t.Run("traacker_expires_paths", func(t *testing.T) {
		r := resolutionTree{tm: time.Now().Add(-trackerInterval)}
		require.True(t, r.expired())

		r = resolutionTree{tm: time.Now().Add(trackerInterval)}
		require.False(t, r.expired())
	})

	t.Run("tracker_prints_and_delete_path", func(t *testing.T) {
		path := "user#member#group:1"
		sm := &sync.Map{}
		sm.Store(
			path,
			&resolutionTree{
				tm:   time.Now().Add(-trackerInterval),
				hits: &atomic.Uint64{},
			},
		)

		model := ulid.Make().String()
		trackChecker.nodes.Store(model, sm)
		trackChecker.logExecutionPaths(false)

		_, ok := sm.Load(path)
		require.False(t, ok)
	})
}
