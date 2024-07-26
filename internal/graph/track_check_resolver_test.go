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
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

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

	t.Run("tracker_integrates_with_cycle_and_local_checker", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		resolvers := NewOrderedCheckResolvers(
			WithTrackerCheckResolverOpts(true, WithTrackerContext(ctx)),
		)

		checker, checkResolverCloser := resolvers.Build()
		t.Cleanup(checkResolverCloser)

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
		resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
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
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		resolvers := NewOrderedCheckResolvers(
			WithTrackerCheckResolverOpts(true, WithTrackerContext(ctx)),
		)

		checker, checkResolverCloser := resolvers.Build()
		t.Cleanup(checkResolverCloser)

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		mockLocalChecker := NewMockCheckResolver(ctrl)
		mockLocalChecker.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Return(&ResolveCheckResponse{
			Allowed: true,
		}, nil).Times(1)
		checker.SetDelegate(mockLocalChecker)

		resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:         ulid.Make().String(),
			TupleKey:        tuple.NewTupleKey("document:1", "viewer", "user:will"),
			RequestMetadata: NewCheckRequestMetadata(defaultResolveNodeLimit),
			VisitedPaths:    map[string]struct{}{},
		})

		require.NoError(t, err)
		require.True(t, resp.GetAllowed())
	})

	t.Run("tracker_user_type", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		resolvers := NewOrderedCheckResolvers(
			WithTrackerCheckResolverOpts(true, WithTrackerContext(ctx)),
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

		userType := trackChecker.userType("group:1#member")
		require.Equal(t, "userset", userType)

		userType = trackChecker.userType("user:ann")
		require.Equal(t, "user", userType)

		userType = trackChecker.userType("user:*")
		require.Equal(t, "userset", userType)
	})

	t.Run("tracker_expiration", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		resolvers := NewOrderedCheckResolvers(
			WithTrackerCheckResolverOpts(true, WithTrackerContext(ctx)),
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

		r := resolutionNode{tm: time.Now().Add(-trackChecker.tickerInterval)}
		require.True(t, r.expired(trackChecker.tickerInterval))

		r = resolutionNode{tm: time.Now().Add(trackChecker.tickerInterval)}
		require.False(t, r.expired(trackChecker.tickerInterval))
	})

	t.Run("tracker_prints_and_delete_path", func(t *testing.T) {
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
		require.Equal(t, uint64(0), fields["hits"])
		require.Equal(t, r.StoreID, fields["store"])
		require.Equal(t, r.AuthorizationModelID, fields["model"])
		require.Equal(t, path, fields["path"])
	})

	t.Run("tracker_success_launch_flush", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		observerLogger, logs := observer.New(zap.DebugLevel)
		logger := &logger.ZapLogger{
			Logger: zap.New(observerLogger),
		}

		resolvers := NewOrderedCheckResolvers(
			WithTrackerCheckResolverOpts(true,
				WithTrackerContext(ctx),
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

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(10) * time.Millisecond)
		}()

		storeID := ulid.Make().String()
		modelID := ulid.Make().String()

		tk := tuple.NewTupleKey("group:1", "member", "user:somebody")
		path := trackChecker.getFormattedNode(tk)

		trackChecker.addPathHits(&ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: modelID,
			TupleKey:             tk,
			RequestMetadata:      NewCheckRequestMetadata(20),
		})

		trackChecker.ticker.Reset(time.Duration(2) * time.Millisecond)
		trackChecker.launchFlush()
		wg.Wait()

		actualLogs := logs.All()

		fields := actualLogs[0].ContextMap()
		require.Equal(t, uint64(1), fields["hits"])
		require.Equal(t, storeID, fields["store"])
		require.Equal(t, modelID, fields["model"])
		require.Equal(t, path, fields["path"])
	})

	t.Run("success_loadModel_and_loadPaths", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		resolvers := NewOrderedCheckResolvers(
			WithTrackerCheckResolverOpts(true, WithTrackerContext(ctx)),
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
		path := trackChecker.getFormattedNode(tk)
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

		path := trackChecker.getFormattedNode(r.GetTupleKey())
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

	t.Run("tracker_records_hits_per_path", func(t *testing.T) {
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

		path := trackChecker.getFormattedNode(r.GetTupleKey())
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
