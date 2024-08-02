package graph

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"

	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestTrackerCheckResolver(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("tracker_closes", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		trackerChecker := NewTrackCheckResolver(WithTrackerContext(ctx))
		trackerChecker.Close()
	})

	t.Run("tracker_delegates_request", func(t *testing.T) {
		checker := NewTrackCheckResolver()
		t.Cleanup(checker.Close)

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		mockLocalChecker := NewMockCheckResolver(ctrl)
		mockLocalChecker.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).
			Return(&ResolveCheckResponse{
				Allowed: false,
			}, nil).
			Times(1)
		checker.SetDelegate(mockLocalChecker)

		resp, err := checker.ResolveCheck(context.Background(), &ResolveCheckRequest{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:will"),
			RequestMetadata:      NewCheckRequestMetadata(defaultResolveNodeLimit),
			VisitedPaths:         map[string]struct{}{},
		})

		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
	})

	t.Run("resolution_node_expiration", func(t *testing.T) {
		randomInterval := rand.Intn(trackerMaxInterval-trackerMinInterval+1) + trackerMinInterval
		randomDuration := time.Duration(randomInterval) * time.Second

		r := resolutionNode{tm: time.Now().Add(-randomDuration)}
		require.True(t, r.expired(randomDuration))

		r = resolutionNode{tm: time.Now().Add(randomDuration)}
		require.False(t, r.expired(randomDuration))
	})

	t.Run("tracker_logs_path_correctly", func(t *testing.T) {
		observerLogger, logs := observer.New(zap.DebugLevel)
		logger := &logger.ZapLogger{
			Logger: zap.New(observerLogger),
		}

		resolvers := NewOrderedCheckResolvers(
			WithTrackerCheckResolverOpts(true, WithTrackerLogger(logger)),
		)

		_, checkResolverCloser := resolvers.Build()
		t.Cleanup(checkResolverCloser)

		var trackChecker *TrackerCheckResolver
		for _, resolver := range resolvers.resolvers {
			switch r := resolver.(type) {
			case *TrackerCheckResolver:
				trackChecker = r
			default:
				continue
			}
		}

		r := &ResolveCheckRequest{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey:             tuple.NewTupleKey("document:abc", "viewer", "user:somebody"),
			RequestMetadata:      NewCheckRequestMetadata(20),
		}
		value, ok := trackChecker.loadModel(r)
		require.NotNil(t, value)
		require.False(t, ok)

		path := "group:1#member@user"
		sm := &sync.Map{}
		sm.Store(
			path,
			&resolutionNode{
				tm:   time.Now().Add(-trackChecker.tickerInterval),
				hits: &atomic.Uint64{},
			},
		)

		trackChecker.nodes.Store(trackerKey{store: r.StoreID, model: r.AuthorizationModelID}, sm)
		trackChecker.logExecutionPaths(false)

		_, ok = sm.Load(path)
		require.False(t, ok)

		actualLogs := logs.All()
		require.Len(t, actualLogs, 1)

		fields := actualLogs[0].ContextMap()
		require.Equal(t, r.StoreID, fields["store_id"])
		require.Equal(t, r.AuthorizationModelID, fields["model_id"])
		require.Equal(t, path, fields["path"])
		require.Equal(t, uint64(0), fields["hits"])
	})

	t.Run("tracker_success_launch_flush", func(t *testing.T) {
		observerLogger, logs := observer.New(zap.DebugLevel)
		logger := &logger.ZapLogger{
			Logger: zap.New(observerLogger),
		}

		resolvers := NewOrderedCheckResolvers(
			WithTrackerCheckResolverOpts(true,
				WithTrackerLogger(logger),
				WithTrackerInterval(time.Nanosecond),
			),
		)

		_, checkResolverCloser := resolvers.Build()
		t.Cleanup(checkResolverCloser)

		var trackChecker *TrackerCheckResolver
		for _, resolver := range resolvers.resolvers {
			switch r := resolver.(type) {
			case *TrackerCheckResolver:
				trackChecker = r
			default:
				continue
			}
		}

		storeID := ulid.Make().String()
		modelID := ulid.Make().String()

		tk := tuple.NewTupleKey("group:1", "member", "user:somebody")
		path := getTupleKeyAsPath(tk)

		trackChecker.addPathHits(&ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: modelID,
			TupleKey:             tk,
			RequestMetadata:      NewCheckRequestMetadata(20),
		})

		trackChecker.ticker.Reset(time.Duration(2) * time.Millisecond)
		trackChecker.launchFlush()
		time.Sleep(time.Duration(10) * time.Millisecond)

		actualLogs := logs.All()

		fields := actualLogs[0].ContextMap()
		require.Equal(t, uint64(1), fields["hits"])
		require.Equal(t, storeID, fields["store_id"])
		require.Equal(t, modelID, fields["model_id"])
		require.Equal(t, path, fields["path"])
	})

	t.Run("success_loadModel_and_loadPaths", func(t *testing.T) {
		resolvers := NewOrderedCheckResolvers(
			WithTrackerCheckResolverOpts(true),
		)

		_, checkResolverCloser := resolvers.Build()
		t.Cleanup(checkResolverCloser)

		var trackChecker *TrackerCheckResolver
		for _, resolver := range resolvers.resolvers {
			switch r := resolver.(type) {
			case *TrackerCheckResolver:
				trackChecker = r
			default:
				continue
			}
		}

		r := &ResolveCheckRequest{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey:             tuple.NewTupleKey("document:abc", "viewer", "user:somebody"),
			RequestMetadata:      NewCheckRequestMetadata(20),
		}
		value, ok := trackChecker.loadModel(r)
		require.NotNil(t, value)
		require.False(t, ok)

		trackChecker.addPathHits(r)

		tk := tuple.NewTupleKey("folder:test", "reader", "user:somebody")
		path := getTupleKeyAsPath(tk)
		trackChecker.addPathHits(&ResolveCheckRequest{
			StoreID:              r.StoreID,
			AuthorizationModelID: r.AuthorizationModelID,
			TupleKey:             tk,
			RequestMetadata:      NewCheckRequestMetadata(20),
		})

		paths, ok := value.(*sync.Map)
		require.True(t, ok)
		_, ok = paths.Load(path)
		require.True(t, ok)

		paths, ok = value.(*sync.Map)
		require.True(t, ok)
		_, ok = paths.Load(path)
		require.True(t, ok)
	})

	t.Run("success_limiter_disallow", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		observerLogger, logs := observer.New(zap.DebugLevel)
		logger := &logger.ZapLogger{
			Logger: zap.New(observerLogger),
		}

		resolvers := NewOrderedCheckResolvers(
			WithTrackerCheckResolverOpts(true, WithTrackerContext(ctx), WithTrackerLogger(logger)),
		)

		_, checkResolverCloser := resolvers.Build()
		t.Cleanup(checkResolverCloser)

		var trackChecker *TrackerCheckResolver
		for _, resolver := range resolvers.resolvers {
			switch r := resolver.(type) {
			case *TrackerCheckResolver:
				trackChecker = r
			default:
				continue
			}
		}

		r := &ResolveCheckRequest{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey:             tuple.NewTupleKey("document:okta", "viewer", "user:alice"),
			RequestMetadata:      NewCheckRequestMetadata(20),
		}
		value, ok := trackChecker.loadModel(r)
		require.NotNil(t, value)
		require.False(t, ok)

		path := getTupleKeyAsPath(r.GetTupleKey())
		paths, _ := value.(*sync.Map)

		val, _ := paths.Load(path)

		rn := val.(*resolutionNode)
		rn.tm = time.Now().Add(-trackChecker.tickerInterval)
		paths.Store(path, rn)

		trackChecker.limiter.SetBurst(0)
		trackChecker.logExecutionPaths(false)

		_, ok = paths.Load(path)
		require.True(t, ok)

		actualLogs := logs.All()
		require.Empty(t, actualLogs)
	})

	t.Run("tracker_counter_is_correct", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		observerLogger, _ := observer.New(zap.DebugLevel)
		logger := &logger.ZapLogger{
			Logger: zap.New(observerLogger),
		}

		resolvers := NewOrderedCheckResolvers(
			WithTrackerCheckResolverOpts(true, WithTrackerContext(ctx), WithTrackerLogger(logger)),
		)

		_, checkResolverCloser := resolvers.Build()
		t.Cleanup(checkResolverCloser)

		var trackChecker *TrackerCheckResolver
		for _, resolver := range resolvers.resolvers {
			switch r := resolver.(type) {
			case *TrackerCheckResolver:
				trackChecker = r
			default:
				continue
			}
		}

		r := &ResolveCheckRequest{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey:             tuple.NewTupleKey("document:99", "viewer", "user:somebody"),
			RequestMetadata:      NewCheckRequestMetadata(20),
		}
		value, ok := trackChecker.loadModel(r)
		require.NotNil(t, value)
		require.False(t, ok)

		paths, ok := value.(*sync.Map)
		require.True(t, ok)

		path := getTupleKeyAsPath(r.GetTupleKey())
		trackChecker.addPathHits(&ResolveCheckRequest{
			StoreID:              r.StoreID,
			AuthorizationModelID: r.AuthorizationModelID,
			TupleKey:             r.GetTupleKey(),
			RequestMetadata:      NewCheckRequestMetadata(20),
		})

		value, ok = paths.Load(path)
		require.True(t, ok)

		rn, ok := value.(*resolutionNode)
		require.True(t, ok)
		require.Equal(t, uint64(1), rn.hits.Load())
	})
}
