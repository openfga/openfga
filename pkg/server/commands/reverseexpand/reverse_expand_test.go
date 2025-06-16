package reverseexpand

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/internal/throttler/threshold"
	"github.com/openfga/openfga/pkg/dispatch"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	storagetest "github.com/openfga/openfga/pkg/storage/test"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestNewReverseExpandQuery(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1

		type user
		type document
			relations
				define viewer: [user]`)

	typeSystem, err := typesystem.New(model)
	require.NoError(t, err)
	t.Run("local_check_resolver_not_initialized", func(t *testing.T) {
		reverseExpandQuery := NewReverseExpandQuery(mockDatastore, typeSystem)
		require.NotNil(t, reverseExpandQuery.localCheckResolver)
		testLocalChecker := graph.NewLocalChecker()
		require.Equal(t, testLocalChecker, reverseExpandQuery.localCheckResolver)
	})
	t.Run("local_check_resolver_initialized", func(t *testing.T) {
		builder := graph.NewOrderedCheckResolvers([]graph.CheckResolverOrderedBuilderOpt{
			graph.WithDispatchThrottlingCheckResolverOpts(true),
			graph.WithLocalCheckerOpts(graph.WithOptimizations(true)),
		}...)
		checkResolver, _, err := builder.Build()
		require.NoError(t, err)
		reverseExpandQuery := NewReverseExpandQuery(mockDatastore, typeSystem, WithCheckResolver(checkResolver))
		require.NotNil(t, reverseExpandQuery.localCheckResolver)
		testLocalChecker := graph.NewLocalChecker()
		require.NotEqual(t, testLocalChecker, reverseExpandQuery.localCheckResolver)
	})
}

func TestReverseExpandResultChannelClosed(t *testing.T) {
	defer goleak.VerifyNone(t)

	store := ulid.Make().String()

	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1

		type user
		type document
			relations
				define viewer: [user]`)

	typeSystem, err := typesystem.New(model)
	require.NoError(t, err)
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	var tuples []*openfgav1.Tuple

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), store, gomock.Any(), gomock.Any()).
		Times(1).
		DoAndReturn(func(_ context.Context, _ string, _ storage.ReadStartingWithUserFilter, _ storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
			iterator := storage.NewStaticTupleIterator(tuples)
			return iterator, nil
		})

	ctx := context.Background()

	resultChan := make(chan *ReverseExpandResult)
	errChan := make(chan error, 1)

	// process query in one goroutine, but it will be cancelled almost right away
	go func() {
		reverseExpandQuery := NewReverseExpandQuery(mockDatastore, typeSystem)
		err := reverseExpandQuery.Execute(ctx, &ReverseExpandRequest{
			StoreID:    store,
			ObjectType: "document",
			Relation:   "viewer",
			User: &UserRefObject{
				Object: &openfgav1.Object{
					Type: "user",
					Id:   "maria",
				},
			},
			ContextualTuples: []*openfgav1.TupleKey{},
		}, resultChan, NewResolutionMetadata())

		if err != nil {
			errChan <- err
		}
	}()

	select {
	case _, open := <-resultChan:
		if open {
			require.FailNow(t, "expected immediate closure of result channel")
		}
	case err := <-errChan:
		require.FailNowf(t, "unexpected error received on error channel :%v", err.Error())
	case <-time.After(30 * time.Millisecond):
		require.FailNow(t, "unexpected timeout on channel receive, expected receive on error channel")
	}
}

func TestReverseExpandRespectsContextCancellation(t *testing.T) {
	defer goleak.VerifyNone(t)

	store := ulid.Make().String()

	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1

		type user
		type document
			relations
				define viewer: [user]`)

	typeSystem, err := typesystem.New(model)
	require.NoError(t, err)
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	var tuples []*openfgav1.Tuple
	for i := 0; i < 100; i++ {
		obj := fmt.Sprintf("document:%s", strconv.Itoa(i))
		tuples = append(tuples, &openfgav1.Tuple{Key: tuple.NewTupleKey(obj, "viewer", "user:maria")})
	}

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), store, gomock.Any(), gomock.Any()).
		Times(1).
		DoAndReturn(func(_ context.Context, _ string, _ storage.ReadStartingWithUserFilter, _ storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
			// simulate many goroutines trying to write to the results channel
			iterator := storage.NewStaticTupleIterator(tuples)
			return iterator, nil
		})
	ctx, cancelFunc := context.WithCancel(context.Background())

	resultChan := make(chan *ReverseExpandResult)
	errChan := make(chan error, 1)

	// process query in one goroutine, but it will be cancelled almost right away
	go func() {
		reverseExpandQuery := NewReverseExpandQuery(mockDatastore, typeSystem)
		err := reverseExpandQuery.Execute(ctx, &ReverseExpandRequest{
			StoreID:    store,
			ObjectType: "document",
			Relation:   "viewer",
			User: &UserRefObject{
				Object: &openfgav1.Object{
					Type: "user",
					Id:   "maria",
				},
			},
			ContextualTuples: []*openfgav1.TupleKey{},
		}, resultChan, NewResolutionMetadata())

		if err != nil {
			errChan <- err
		}
	}()
	go func() {
		// simulate max_results=1
		res := <-resultChan

		// send cancellation to the other goroutine
		cancelFunc()

		// this check it not the goal of this test, it's here just as sanity check
		if res.Object == "" {
			panic("expected object, got nil")
		}
	}()

	select {
	case err := <-errChan:
		require.ErrorContains(t, err, "context canceled")
	case <-time.After(30 * time.Millisecond):
		require.FailNow(t, "unexpected timeout on channel receive, expected receive on error channel")
	}
}

func TestReverseExpandRespectsContextTimeout(t *testing.T) {
	defer goleak.VerifyNone(t)

	store := ulid.Make().String()

	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1

		type user
		type document
			relations
				define allowed: [user]
				define viewer: [user] and allowed`)

	typeSystem, err := typesystem.New(model)
	require.NoError(t, err)
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), store, gomock.Any(), gomock.Any()).
		MaxTimes(2) // we expect it to be 0 most of the time

	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	resultChan := make(chan *ReverseExpandResult)
	errChan := make(chan error, 1)

	go func() {
		reverseExpandQuery := NewReverseExpandQuery(mockDatastore, typeSystem)
		err := reverseExpandQuery.Execute(timeoutCtx, &ReverseExpandRequest{
			StoreID:    store,
			ObjectType: "document",
			Relation:   "viewer",
			User: &UserRefObject{
				Object: &openfgav1.Object{
					Type: "user",
					Id:   "maria",
				},
			},
			ContextualTuples: []*openfgav1.TupleKey{},
		}, resultChan, NewResolutionMetadata())

		if err != nil {
			errChan <- err
		}
	}()
	select {
	case _, open := <-resultChan:
		if !open {
			require.FailNow(t, "unexpected closure of result channel")
		}
	case err := <-errChan:
		require.Error(t, err)
	case <-time.After(1 * time.Second):
		require.FailNow(t, "unexpected timeout encountered, expected other receive")
	}
}

func TestReverseExpandErrorInTuples(t *testing.T) {
	defer goleak.VerifyNone(t)

	store := ulid.Make().String()

	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1

		type user
		type document
			relations
				define viewer: [user]`)

	typeSystem, err := typesystem.New(model)
	require.NoError(t, err)
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	var tuples []*openfgav1.Tuple
	for i := 0; i < 100; i++ {
		obj := fmt.Sprintf("document:%s", strconv.Itoa(i))
		tuples = append(tuples, &openfgav1.Tuple{Key: tuple.NewTupleKey(obj, "viewer", "user:maria")})
	}

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), store, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ storage.ReadStartingWithUserFilter, _ storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
			iterator := mocks.NewErrorTupleIterator(tuples)
			return iterator, nil
		})

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	resultChan := make(chan *ReverseExpandResult)
	errChan := make(chan error, 1)

	// process query in one goroutine, but it will be cancelled almost right away
	go func() {
		reverseExpandQuery := NewReverseExpandQuery(mockDatastore, typeSystem)
		err := reverseExpandQuery.Execute(ctx, &ReverseExpandRequest{
			StoreID:    store,
			ObjectType: "document",
			Relation:   "viewer",
			User: &UserRefObject{
				Object: &openfgav1.Object{
					Type: "user",
					Id:   "maria",
				},
			},
			ContextualTuples: []*openfgav1.TupleKey{},
		}, resultChan, NewResolutionMetadata())
		if err != nil {
			errChan <- err
		}
	}()

ConsumerLoop:
	for {
		select {
		case _, open := <-resultChan:
			if !open {
				require.FailNow(t, "unexpected closure of result channel")
			}

			cancelFunc()
		case err := <-errChan:
			require.Error(t, err)
			break ConsumerLoop
		case <-time.After(30 * time.Millisecond):
			require.FailNow(t, "unexpected timeout waiting for channel receive, expected an error on the error channel")
		}
	}
}

func TestReverseExpandSendsAllErrorsThroughChannel(t *testing.T) {
	defer goleak.VerifyNone(t)

	store := ulid.Make().String()

	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1

		type user
		type document
			relations
				define viewer: [user]`)

	mockDatastore := mocks.NewMockSlowDataStorage(memory.New(), 1*time.Second)

	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(1*time.Nanosecond))
		t.Cleanup(cancel)

		resultChan := make(chan *ReverseExpandResult)
		errChan := make(chan error, 1)

		go func() {
			ts, err := typesystem.New(model)
			if err != nil {
				t.Error("unexpected error creating model", err)
				return
			}
			reverseExpandQuery := NewReverseExpandQuery(mockDatastore, ts)
			err = reverseExpandQuery.Execute(ctx, &ReverseExpandRequest{
				StoreID:    store,
				ObjectType: "document",
				Relation:   "viewer",
				User: &UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "maria",
					},
				},
				ContextualTuples: []*openfgav1.TupleKey{},
			}, resultChan, NewResolutionMetadata())

			if err != nil {
				errChan <- err
			}
		}()

		select {
		case _, channelOpen := <-resultChan:
			if !channelOpen {
				require.FailNow(t, "unexpected closure of result channel")
			}
		case err := <-errChan:
			require.Error(t, err)
		case <-time.After(3 * time.Second):
			require.FailNow(t, "unexpected timeout waiting for channel receive, expected an error on the error channel")
		}
	}
}

func TestReverseExpandIgnoresInvalidTuples(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	storeID := ulid.Make().String()

	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1

		type user
		type group
			relations
				define member: [user, group#member]`)

	mockController := gomock.NewController(t)
	t.Cleanup(func() {
		mockController.Finish()
	})

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	gomock.InAnyOrder([]*gomock.Call{
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "member",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:anne"}},
		}, gomock.Any()).
			Times(1).
			DoAndReturn(func(_ context.Context, _ string, _ storage.ReadStartingWithUserFilter, _ storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
				return storage.NewStaticTupleIterator([]*openfgav1.Tuple{
					{Key: tuple.NewTupleKey("group:fga", "member", "user:anne")},
				}), nil
			}),

		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "member",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "group:fga", Relation: "member"}},
		}, gomock.Any()).
			Times(1).
			DoAndReturn(func(_ context.Context, _ string, _ storage.ReadStartingWithUserFilter, _ storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
				return storage.NewStaticTupleIterator([]*openfgav1.Tuple{
					// NOTE this tuple is invalid
					{Key: tuple.NewTupleKey("group:eng", "member", "fail:fga#member")},
				}), nil
			}),
	},
	)

	ctx := context.Background()

	resultChan := make(chan *ReverseExpandResult, 2)
	errChan := make(chan error, 1)

	go func() {
		ts, err := typesystem.New(model)
		if err != nil {
			t.Error("unexpected error creating model", err)
			return
		}
		reverseExpandQuery := NewReverseExpandQuery(mockDatastore, ts)
		err = reverseExpandQuery.Execute(ctx, &ReverseExpandRequest{
			StoreID:          storeID,
			ObjectType:       "group",
			Relation:         "member",
			User:             &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "anne"}},
			ContextualTuples: []*openfgav1.TupleKey{},
		}, resultChan, NewResolutionMetadata())

		if err != nil {
			errChan <- err
		}
	}()

	var results []string

	for {
		select {
		case res, open := <-resultChan:
			if !open {
				require.ElementsMatch(t, []string{"group:fga"}, results)
				return
			}
			results = append(results, res.Object)
		case err := <-errChan:
			require.FailNowf(t, "unexpected error received on error channel :%v", err.Error())
			return
		case <-ctx.Done():
			return
		}
	}
}

func TestReverseExpandThrottle(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1

		type user

		type document
			relations
				define viewer: [user]`)
	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	ctx := context.Background()
	typesys, err := typesystem.NewAndValidate(ctx, model)
	require.NoError(t, err)

	t.Run("dispatch_below_threshold_doesnt_call_throttle", func(t *testing.T) {
		mockThrottler := mocks.NewMockThrottler(mockController)
		reverseExpandQuery := NewReverseExpandQuery(
			mockDatastore,
			typesys,
			WithDispatchThrottlerConfig(threshold.Config{
				Throttler:    mockThrottler,
				Threshold:    200,
				MaxThreshold: 200,
			}),
		)
		mockThrottler.EXPECT().Throttle(gomock.Any()).Times(0)
		dispatchCountValue := uint32(190)
		metadata := NewResolutionMetadata()
		metadata.DispatchCounter.Store(dispatchCountValue)

		reverseExpandQuery.throttle(ctx, dispatchCountValue, metadata)
		require.False(t, metadata.WasThrottled.Load())
	})

	t.Run("above_threshold_should_call_throttle", func(t *testing.T) {
		mockThrottler := mocks.NewMockThrottler(mockController)
		reverseExpandQuery := NewReverseExpandQuery(
			mockDatastore,
			typesys,
			WithDispatchThrottlerConfig(threshold.Config{
				Throttler:    mockThrottler,
				Threshold:    200,
				MaxThreshold: 200,
			}),
		)
		mockThrottler.EXPECT().Throttle(gomock.Any()).Times(1)
		dispatchCountValue := uint32(201)
		metadata := NewResolutionMetadata()
		metadata.DispatchCounter.Store(dispatchCountValue)

		reverseExpandQuery.throttle(ctx, dispatchCountValue, metadata)
		require.True(t, metadata.WasThrottled.Load())
	})

	t.Run("zero_max_should_interpret_as_default", func(t *testing.T) {
		mockThrottler := mocks.NewMockThrottler(mockController)
		reverseExpandQuery := NewReverseExpandQuery(
			mockDatastore,
			typesys,
			WithDispatchThrottlerConfig(threshold.Config{
				Throttler:    mockThrottler,
				Threshold:    200,
				MaxThreshold: 0,
			}),
		)
		mockThrottler.EXPECT().Throttle(gomock.Any()).Times(0)
		dispatchCountValue := uint32(190)
		metadata := NewResolutionMetadata()
		metadata.DispatchCounter.Store(dispatchCountValue)

		reverseExpandQuery.throttle(ctx, dispatchCountValue, metadata)
		require.False(t, metadata.WasThrottled.Load())
	})

	t.Run("dispatch_should_use_request_threshold_if_available", func(t *testing.T) {
		mockThrottler := mocks.NewMockThrottler(mockController)
		reverseExpandQuery := NewReverseExpandQuery(
			mockDatastore,
			typesys,
			WithDispatchThrottlerConfig(threshold.Config{
				Throttler:    mockThrottler,
				Threshold:    0,
				MaxThreshold: 210,
			}),
		)
		mockThrottler.EXPECT().Throttle(gomock.Any()).Times(1)
		dispatchCountValue := uint32(201)
		ctx := context.Background()
		ctx = dispatch.ContextWithThrottlingThreshold(ctx, 200)
		metadata := NewResolutionMetadata()
		metadata.DispatchCounter.Store(dispatchCountValue)

		reverseExpandQuery.throttle(ctx, dispatchCountValue, metadata)
		require.True(t, metadata.WasThrottled.Load())
	})

	t.Run("should_respect_max_threshold", func(t *testing.T) {
		mockThrottler := mocks.NewMockThrottler(mockController)
		reverseExpandQuery := NewReverseExpandQuery(
			mockDatastore,
			typesys,
			WithDispatchThrottlerConfig(threshold.Config{
				Throttler:    mockThrottler,
				Threshold:    200,
				MaxThreshold: 300,
			}),
		)
		mockThrottler.EXPECT().Throttle(gomock.Any()).Times(1)
		dispatchCountValue := uint32(301)
		ctx := context.Background()
		ctx = dispatch.ContextWithThrottlingThreshold(ctx, 1000)
		metadata := NewResolutionMetadata()

		reverseExpandQuery.throttle(ctx, dispatchCountValue, metadata)
		require.True(t, metadata.WasThrottled.Load())
	})
}

func TestReverseExpandDispatchCount(t *testing.T) {
	ds := memory.New()
	t.Cleanup(ds.Close)
	tests := []struct {
		name                    string
		model                   string
		tuples                  []string
		objectType              string
		relation                string
		user                    *UserRefObject
		throttlingEnabled       bool
		expectedDispatchCount   uint32
		expectedThrottlingValue int
		expectedWasThrottled    bool
	}{
		{
			name: "should_throttle",
			model: `
				model
					schema 1.1

				type user

				type folder
					relations
						define editor: [user]
						define viewer: [user] or editor 
			`,
			tuples: []string{
				"folder:C#editor@user:jon",
				"folder:B#viewer@user:jon",
				"folder:A#viewer@user:jon",
			},
			objectType:              "folder",
			relation:                "viewer",
			user:                    &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "jon"}},
			throttlingEnabled:       true,
			expectedWasThrottled:    true,
			expectedDispatchCount:   4,
			expectedThrottlingValue: 1,
		},
		{
			name: "should_not_throttle",
			model: `
				model
					schema 1.1
			
				type user
			
				type folder
					relations
						define editor: [user]
						define viewer: [user] or editor
			`,
			tuples: []string{
				"folder:C#editor@user:jon",
				"folder:B#viewer@user:jon",
				"folder:A#viewer@user:jon",
			},
			objectType:              "folder",
			relation:                "viewer",
			user:                    &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "jon"}},
			throttlingEnabled:       false,
			expectedWasThrottled:    false,
			expectedDispatchCount:   4,
			expectedThrottlingValue: 0,
		},
		{
			name: "should_not_throttle_if_there_are_not_enough_dispatches",
			model: `
				model
					schema 1.1
			
				type user
			
				type document
					relations
						define editor: [user]
						define viewer: editor
			`,
			tuples: []string{
				"document:1#editor@user:jon",
			},
			objectType:              "document",
			relation:                "viewer",
			user:                    &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "jon"}},
			throttlingEnabled:       true,
			expectedWasThrottled:    false,
			expectedDispatchCount:   2,
			expectedThrottlingValue: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			storeID, model := storagetest.BootstrapFGAStore(t, ds, test.model, test.tuples)
			resultChan := make(chan *ReverseExpandResult)
			errChan := make(chan error, 1)
			typesys, err := typesystem.NewAndValidate(
				context.Background(),
				model,
			)
			require.NoError(t, err)
			ctx := storage.ContextWithRelationshipTupleReader(context.Background(), ds)
			ctrl := gomock.NewController(t)
			ctx = typesystem.ContextWithTypesystem(ctx, typesys)
			resolutionMetadata := NewResolutionMetadata()

			mockThrottler := mocks.NewMockThrottler(ctrl)
			t.Cleanup(ctrl.Finish)
			mockThrottler.EXPECT().Throttle(gomock.Any()).Times(test.expectedThrottlingValue)

			go func() {
				q := NewReverseExpandQuery(
					ds,
					typesys,
					WithDispatchThrottlerConfig(threshold.Config{
						Throttler:    mockThrottler,
						Enabled:      test.throttlingEnabled,
						Threshold:    3,
						MaxThreshold: 0,
					}),
				)

				err = q.Execute(ctx, &ReverseExpandRequest{
					StoreID:    storeID,
					ObjectType: test.objectType,
					Relation:   test.relation,
					User:       test.user,
				}, resultChan, resolutionMetadata)

				if err != nil {
					errChan <- err
				}
			}()

		ConsumerLoop:
			for {
				select {
				case _, open := <-resultChan:
					if !open {
						break ConsumerLoop
					}
				case err := <-errChan:
					require.FailNowf(t, "unexpected error received on error channel :%v", err.Error())
					break ConsumerLoop
				case <-ctx.Done():
					break ConsumerLoop
				}
			}
			require.Equal(t, test.expectedDispatchCount, resolutionMetadata.DispatchCounter.Load())
			require.Equal(t, test.expectedWasThrottled, resolutionMetadata.WasThrottled.Load())
		})
	}
}

func TestReverseExpandHonorsConsistency(t *testing.T) {
	defer goleak.VerifyNone(t)

	store := ulid.Make().String()

	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1

		type user
		type document
			relations
				define viewer: [user]`)

	typeSystem, err := typesystem.New(model)
	require.NoError(t, err)
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	ctx := context.Background()
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	// run once with no consistency specified
	unspecifiedConsistency := storage.ReadStartingWithUserOptions{
		Consistency: storage.ConsistencyOptions{Preference: openfgav1.ConsistencyPreference_UNSPECIFIED},
	}
	mockDatastore.EXPECT().
		ReadStartingWithUser(gomock.Any(), store, gomock.Any(), unspecifiedConsistency).
		Times(1).
		Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil)

	request := &ReverseExpandRequest{
		StoreID:    store,
		ObjectType: "document",
		Relation:   "viewer",
		User: &UserRefObject{
			Object: &openfgav1.Object{
				Type: "user",
				Id:   "maria",
			},
		},
		ContextualTuples: []*openfgav1.TupleKey{},
	}

	reverseExpandQuery := NewReverseExpandQuery(mockDatastore, typeSystem)
	resultChan := make(chan *ReverseExpandResult)

	err = reverseExpandQuery.Execute(ctx, request, resultChan, NewResolutionMetadata())
	require.NoError(t, err)

	// now do it again with specified consistency
	highConsistency := storage.ReadStartingWithUserOptions{
		Consistency: storage.ConsistencyOptions{Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY},
	}

	request.Consistency = openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY
	mockDatastore.EXPECT().
		ReadStartingWithUser(gomock.Any(), store, gomock.Any(), highConsistency).
		Times(1).
		Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil)

	resultChanTwo := make(chan *ReverseExpandResult)
	err = reverseExpandQuery.Execute(ctx, request, resultChanTwo, NewResolutionMetadata())
	require.NoError(t, err)

	// Make sure we didn't leave channels open
	select {
	case _, open := <-resultChan:
		if open {
			require.FailNow(t, "results channels should be closed")
		}
	case _, open := <-resultChanTwo:
		if open {
			require.FailNow(t, "results channels should be closed")
		}
	}
}

func TestShouldCheckPublicAssignable(t *testing.T) {
	tests := []struct {
		name            string
		model           string
		targetReference *openfgav1.RelationReference
		userRef         IsUserRef
		expectedResult  bool
		expectedError   bool
	}{
		{
			name: "model_non_public",
			model: `
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]
				type document
					relations
						define viewer: [user, group#member]
`,
			targetReference: typesystem.DirectRelationReference("document", "viewer"),
			userRef: &UserRefObject{
				Object: &openfgav1.Object{Type: "user", Id: "bob"},
			},
			expectedResult: false,
		},
		{
			name: "model_public",
			model: `
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]
				type document
					relations
						define viewer: [user, user:*, group#member]
`,
			targetReference: typesystem.DirectRelationReference("document", "viewer"),
			userRef: &UserRefObject{
				Object: &openfgav1.Object{Type: "user", Id: "bob"},
			},
			expectedResult: true,
		},
		{
			name: "model_public_but_request_userset",
			model: `
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]
				type document
					relations
						define viewer: [user, user:*, group#member]
`,
			targetReference: typesystem.DirectRelationReference("document", "viewer"),
			userRef: &UserRefObjectRelation{
				ObjectRelation: &openfgav1.ObjectRelation{
					Object:   "group",
					Relation: "member",
				},
			},
			expectedResult: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockController := gomock.NewController(t)
			defer mockController.Finish()

			typeSystem, err := typesystem.New(testutils.MustTransformDSLToProtoWithID(test.model))
			require.NoError(t, err)

			mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
			dut := NewReverseExpandQuery(mockDatastore, typeSystem)
			publiclyAssignable, err := dut.shouldCheckPublicAssignable(test.targetReference, test.userRef)
			if test.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expectedResult, publiclyAssignable)
			}
		})
	}
}

func TestReverseExpandNew(t *testing.T) {
	ds := memory.New()
	t.Cleanup(ds.Close)
	tests := []struct {
		name            string
		model           string
		tuples          []string
		objectType      string
		relation        string
		user            *UserRefObject
		expectedObjects []string
	}{
		{
			name: "direct_and_algebraic",
			model: `model
			  schema 1.1
		
			type user
			type repo
			  relations
				define member: [user]
				define computed_member: member
				define owner: [user]
				define admin: [user] or computed_member
				define or_admin: owner or admin
		`,
			tuples: []string{
				"repo:fga#member@user:justin",
				"repo:fga#owner@user:z",
			},
			objectType:      "repo",
			relation:        "or_admin",
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedObjects: []string{"repo:fga"},
		},
		{
			name: "simple_ttu",
			model: `model
				  schema 1.1
		
				type organization
				  relations
					define member: [user]
					define repo_admin: [organization#member]
				type repo
				  relations
					define admin: repo_admin from owner
					define owner: [organization]
				type user
		`,
			tuples: []string{
				"repo:fga#owner@organization:jz",
				"organization:jz#repo_admin@organization:j#member",
				"organization:j#member@user:justin",
			},
			objectType:      "repo",
			relation:        "admin",
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedObjects: []string{"repo:fga"},
		},
		{
			name: "ttu_from_union",
			model: `model
				  schema 1.1
		
				type organization
				  relations
					define member: [user]
					define repo_admin: [user, organization#member]
				type repo
				  relations
					define admin: [user, team#member] or repo_admin from owner
					define owner: [organization]
				type team
				  relations
					define member: [user, team#member]
		
				type user
		`,
			tuples: []string{
				"repo:fga#owner@organization:justin_and_zee",
				"organization:justin_and_zee#repo_admin@user:justin",
			},
			objectType:      "repo",
			relation:        "admin",
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedObjects: []string{"repo:fga"},
		},
		{
			name: "ttu_multiple_types_with_rewrites",
			model: `model
				  schema 1.1
		
				type organization
				  relations
					define member: [user]
					define repo_admin: [team#member] or member
				type repo
				  relations
					define admin: [team#member] or repo_admin from owner
					define owner: [organization]
				type team
				  relations
				    define member: [user]
				type user
		`,
			tuples: []string{
				"team:jz#member@user:justin",
				"organization:jz#repo_admin@team:jz#member",
				"repo:fga#owner@organization:jz",
			},
			objectType:      "repo",
			relation:        "admin",
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedObjects: []string{"repo:fga"},
		},
		{
			name: "ttu_recursive",
			model: `model
				  schema 1.1
		
				type user
				type org
				  relations
					define parent: [org]
					define ttu_recursive: [user] or ttu_recursive from parent
		`,
			tuples: []string{
				"org:a#ttu_recursive@user:justin",
				"org:b#parent@org:a", // org:a is parent of b
				"org:c#parent@org:b", // org:b is parent of org:c
				"org:d#parent@org:c", // org:c is parent of org:d
			},
			objectType:      "org",
			relation:        "ttu_recursive",
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedObjects: []string{"org:a", "org:b", "org:c", "org:d"},
		},
		{
			name: "ttu_with_cycle",
			model: `model
				  schema 1.1
		
				type user
				type org
				  relations
					define parent: [company]
					define org_cycle: [user] or company_cycle from parent
				type company
				  relations
					define parent: [org]
					define company_cycle: [user] or org_cycle from parent
		`,
			tuples: []string{
				"company:a#parent@org:a",
				"org:a#parent@company:b",
				"company:b#parent@org:b",
				"org:b#parent@company:c",
				"company:c#company_cycle@user:bob",
				//"org:b#org_cycle@user:anne",
			},
			objectType: "org",
			relation:   "org_cycle",
			//user:       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "anne"}},
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedObjects: []string{"org:a", "org:b"},
		},
		{
			name: "intersection_both_side_infinite_weight_ttu_debug",
			model: `model
				    schema 1.1
		
					type user
					type team
						relations
							define parent: [team]
							define member: [user] or member from parent
					type org
						relations
							define member: [team#member]
		`,
			tuples: []string{
				"team:a#member@user:bob",
				"team:b#parent@team:a",
				"team:c#parent@team:b",
				"org:a#member@team:c#member",
			},
			objectType:      "org",
			relation:        "member",
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedObjects: []string{"org:a"},
		},
		{
			name: "intersection_both_side_infinite_weight_ttu_debug2",
			model: `model
				    schema 1.1
		
					type user
					type team
						relations
							define parent: [team]
							define member: [user] or member from parent
					type org
						relations
							define team: [team]
							define member: member from team
		`,
			tuples: []string{
				"team:a#member@user:bob",
				"team:b#parent@team:a",
				"team:c#parent@team:b",
				"org:a#team@team:c",
			},
			objectType:      "org",
			relation:        "member",
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedObjects: []string{"org:a"},
		},
		{
			name: "simple_userset",
			model: `model
				  schema 1.1
		
				type user
				type team
				  relations
					define member: [user]
				type org
				  relations
					define teammate: [user, team#member]
		`,
			tuples: []string{
				"team:fga#member@user:justin",
				"org:j#teammate@team:fga#member",
				"org:z#teammate@user:justin",
			},
			objectType:      "org",
			relation:        "teammate",
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedObjects: []string{"org:j", "org:z"},
		},
		{
			name: "userset_to_union",
			model: `model
				  schema 1.1
		
				type user
				type team
				  relations
					define member: admin or boss
					define admin: [user]
					define boss: [user]
				type org
				  relations
					define teammate: [team#member]
		`,
			tuples: []string{
				"team:fga#admin@user:justin",
				"org:j#teammate@team:fga#member",
			},
			objectType:      "org",
			relation:        "teammate",
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedObjects: []string{"org:j"},
		},
		{
			name: "recursive_userset",
			model: `model
				  schema 1.1
		
				type user
				type team
				  relations
					define member: [user, team#member]
		`,
			tuples: []string{
				"team:fga#member@user:justin",
				"team:cncf#member@team:fga#member",
				"team:lnf#member@team:cncf#member",
			},
			objectType:      "team",
			relation:        "member",
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedObjects: []string{"team:fga", "team:cncf", "team:lnf"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			storeID, model := storagetest.BootstrapFGAStore(t, ds, test.model, test.tuples)
			resultChan := make(chan *ReverseExpandResult)
			errChan := make(chan error, 1)
			typesys, err := typesystem.NewAndValidate(
				context.Background(),
				model,
			)
			require.NoError(t, err)
			ctx := storage.ContextWithRelationshipTupleReader(context.Background(), ds)
			ctx = typesystem.ContextWithTypesystem(ctx, typesys)
			resolutionMetadata := NewResolutionMetadata()
			go func() {
				q := NewReverseExpandQuery(
					ds,
					typesys,
					WithListObjectOptimizationsEnabled(true),
				)

				err = q.Execute(ctx, &ReverseExpandRequest{
					StoreID:    storeID,
					ObjectType: test.objectType,
					Relation:   test.relation,
					User:       test.user,
				}, resultChan, resolutionMetadata)

				if err != nil {
					errChan <- err
				}
			}()

			var results []string
		ConsumerLoop:
			for {
				select {
				case result, open := <-resultChan:
					if !open {
						break ConsumerLoop
					}
					results = append(results, result.Object)
				case err := <-errChan:
					require.FailNowf(t, "unexpected error received on error channel :%v", err.Error())
					break ConsumerLoop
				case <-ctx.Done():
					break ConsumerLoop
				}
			}
			require.ElementsMatch(t, test.expectedObjects, results)
		})
	}
}
