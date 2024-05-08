package reverseexpand

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"go.uber.org/goleak"

	"github.com/openfga/openfga/internal/throttler/threshold"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"

	storagetest "github.com/openfga/openfga/pkg/storage/test"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestReverseExpandResultChannelClosed(t *testing.T) {
	defer goleak.VerifyNone(t)

	store := ulid.Make().String()

	model := testutils.MustTransformDSLToProtoWithID(`model
 schema 1.1
type user
type document
 relations
	define viewer: [user]`)

	typeSystem := typesystem.New(model)
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	var tuples []*openfgav1.Tuple

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), store, gomock.Any()).
		Times(1).
		DoAndReturn(func(_ context.Context, _ string, _ storage.ReadStartingWithUserFilter) (storage.TupleIterator, error) {
			iterator := storage.NewStaticTupleIterator(tuples)
			return iterator, nil
		})

	ctx := context.Background()

	resultChan := make(chan *ReverseExpandResult)
	errChan := make(chan error, 1)

	// process query in one goroutine, but it will be cancelled almost right away
	go func() {
		reverseExpandQuery := NewReverseExpandQuery(mockDatastore, typeSystem)
		t.Logf("before execute reverse expand")
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
		t.Logf("after execute reverse expand")

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
		require.FailNow(t, "unexpected error received on error channel :%v", err)
	case <-time.After(30 * time.Millisecond):
		require.FailNow(t, "unexpected timeout on channel receive, expected receive on error channel")
	}
}

func TestReverseExpandRespectsContextCancellation(t *testing.T) {
	defer goleak.VerifyNone(t)

	store := ulid.Make().String()

	model := testutils.MustTransformDSLToProtoWithID(`model
 schema 1.1
type user
type document
 relations
	define viewer: [user]`)

	typeSystem := typesystem.New(model)
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	var tuples []*openfgav1.Tuple
	for i := 0; i < 100; i++ {
		obj := fmt.Sprintf("document:%s", strconv.Itoa(i))
		tuples = append(tuples, &openfgav1.Tuple{Key: tuple.NewTupleKey(obj, "viewer", "user:maria")})
	}

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), store, gomock.Any()).
		Times(1).
		DoAndReturn(func(_ context.Context, _ string, _ storage.ReadStartingWithUserFilter) (storage.TupleIterator, error) {
			// simulate many goroutines trying to write to the results channel
			iterator := storage.NewStaticTupleIterator(tuples)
			t.Logf("returning tuple iterator")
			return iterator, nil
		})
	ctx, cancelFunc := context.WithCancel(context.Background())

	resultChan := make(chan *ReverseExpandResult)
	errChan := make(chan error, 1)

	// process query in one goroutine, but it will be cancelled almost right away
	go func() {
		reverseExpandQuery := NewReverseExpandQuery(mockDatastore, typeSystem)
		t.Logf("before execute reverse expand")
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
		t.Logf("after execute reverse expand")

		if err != nil {
			errChan <- err
		}
	}()
	go func() {
		// simulate max_results=1
		t.Logf("before receive one result")
		res := <-resultChan
		t.Logf("after receive one result")

		// send cancellation to the other goroutine
		cancelFunc()

		// this check it not the goal of this test, it's here just as sanity check
		if res.Object == "" {
			panic("expected object, got nil")
		}
		t.Logf("received object %s ", res.Object)
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

	model := testutils.MustTransformDSLToProtoWithID(`model
 schema 1.1
type user
type document
 relations
	define allowed: [user]
	define viewer: [user] and allowed`)

	typeSystem := typesystem.New(model)
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), store, gomock.Any()).
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

	model := testutils.MustTransformDSLToProtoWithID(`model
 schema 1.1
type user
type document
 relations
	define viewer: [user]`)

	typeSystem := typesystem.New(model)
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	var tuples []*openfgav1.Tuple
	for i := 0; i < 100; i++ {
		obj := fmt.Sprintf("document:%s", strconv.Itoa(i))
		tuples = append(tuples, &openfgav1.Tuple{Key: tuple.NewTupleKey(obj, "viewer", "user:maria")})
	}

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), store, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ storage.ReadStartingWithUserFilter) (storage.TupleIterator, error) {
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

	model := testutils.MustTransformDSLToProtoWithID(`model
 schema 1.1
type user
type document
 relations
   define viewer: [user]`)

	mockDatastore := mocks.NewMockSlowDataStorage(memory.New(), 1*time.Second)

	for i := 0; i < 50; i++ {
		t.Logf("iteration %d", i)
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(1*time.Nanosecond))
		t.Cleanup(cancel)

		resultChan := make(chan *ReverseExpandResult)
		errChan := make(chan error, 1)

		go func() {
			reverseExpandQuery := NewReverseExpandQuery(mockDatastore, typesystem.New(model))
			t.Logf("before produce")
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
			t.Logf("after produce")

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
		}).
			Times(1).
			DoAndReturn(func(_ context.Context, _ string, _ storage.ReadStartingWithUserFilter) (storage.TupleIterator, error) {
				return storage.NewStaticTupleIterator([]*openfgav1.Tuple{
					{Key: tuple.NewTupleKey("group:fga", "member", "user:anne")},
				}), nil
			}),

		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "member",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "group:fga", Relation: "member"}},
		}).
			Times(1).
			DoAndReturn(func(_ context.Context, _ string, _ storage.ReadStartingWithUserFilter) (storage.TupleIterator, error) {
				return storage.NewStaticTupleIterator([]*openfgav1.Tuple{
					// NOTE this tuple is invalid
					{Key: tuple.NewTupleKey("group:eng#member", "member", "group:fga#member")},
				}), nil
			}),
	},
	)

	ctx := context.Background()

	resultChan := make(chan *ReverseExpandResult, 2)
	errChan := make(chan error, 1)

	go func() {
		reverseExpandQuery := NewReverseExpandQuery(mockDatastore, typesystem.New(model))
		err := reverseExpandQuery.Execute(ctx, &ReverseExpandRequest{
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
			require.FailNow(t, "unexpected error received on error channel :%v", err)
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

	model := testutils.MustTransformDSLToProtoWithID(`model
	schema 1.1

	type user

	type document
	  relations
		define viewer: [user]`)
	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	ctx := context.Background()
	ts, err := typesystem.NewAndValidate(ctx, model)
	require.NoError(t, err)

	t.Run("dispatch_below_threshold_doesnt_call_throttle", func(t *testing.T) {
		mockThrottler := mocks.NewMockThrottler(mockController)
		reverseExpandQuery := NewReverseExpandQuery(
			mockDatastore,
			ts,
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
	})

	t.Run("above_threshold_should_call_throttle", func(t *testing.T) {
		mockThrottler := mocks.NewMockThrottler(mockController)
		reverseExpandQuery := NewReverseExpandQuery(
			mockDatastore,
			ts,
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
	})

	t.Run("zero_max_should_interpret_as_default", func(t *testing.T) {
		mockThrottler := mocks.NewMockThrottler(mockController)
		reverseExpandQuery := NewReverseExpandQuery(
			mockDatastore,
			ts,
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
	})

	t.Run("dispatch_should_use_request_threshold_if_available", func(t *testing.T) {
		mockThrottler := mocks.NewMockThrottler(mockController)
		reverseExpandQuery := NewReverseExpandQuery(
			mockDatastore,
			ts,
			WithDispatchThrottlerConfig(threshold.Config{
				Throttler:    mockThrottler,
				Threshold:    0,
				MaxThreshold: 210,
			}),
		)
		mockThrottler.EXPECT().Throttle(gomock.Any()).Times(1)
		dispatchCountValue := uint32(201)
		ctx := context.Background()
		ctx = threshold.ContextWithThrottlingThreshold(ctx, 200)
		metadata := NewResolutionMetadata()
		metadata.DispatchCounter.Store(dispatchCountValue)

		reverseExpandQuery.throttle(ctx, dispatchCountValue, metadata)
	})

	t.Run("should_respect_max_threshold", func(t *testing.T) {
		mockThrottler := mocks.NewMockThrottler(mockController)
		reverseExpandQuery := NewReverseExpandQuery(
			mockDatastore,
			ts,
			WithDispatchThrottlerConfig(threshold.Config{
				Throttler:    mockThrottler,
				Threshold:    200,
				MaxThreshold: 300,
			}),
		)
		mockThrottler.EXPECT().Throttle(gomock.Any()).Times(1)
		dispatchCountValue := uint32(301)
		ctx := context.Background()
		ctx = threshold.ContextWithThrottlingThreshold(ctx, 1000)
		metadata := NewResolutionMetadata()

		reverseExpandQuery.throttle(ctx, dispatchCountValue, metadata)
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
			model: `model
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
			model: `model
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
			model: `model
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
					require.FailNow(t, "unexpected error received on error channel :%v", err)
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
