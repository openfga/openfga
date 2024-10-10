package graph

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/openfga/openfga/internal/mocks"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestResolveCheckFromCache(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ctx := context.Background()

	req := &ResolveCheckRequest{
		StoreID:              "12",
		AuthorizationModelID: "33",
		TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
		RequestMetadata:      NewCheckRequestMetadata(20),
	}
	result := &ResolveCheckResponse{Allowed: true}

	// if the tuple is different, it should result in fetching from cache
	tests := []struct {
		name                string
		initialReq          *ResolveCheckRequest
		subsequentReq       *ResolveCheckRequest
		setInitialResult    func(mock *MockCheckResolver, request *ResolveCheckRequest)
		setTestExpectations func(mock *MockCheckResolver, request *ResolveCheckRequest)
	}{
		{
			name: "same_request_returns_results_from_cache",
			subsequentReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
				RequestMetadata:      NewCheckRequestMetadata(20),
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(0).Return(result, nil)
			},
		},
		{
			name: "same_request_returns_results_from_cache_when_minimize_latency_requested",
			subsequentReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
				RequestMetadata:      NewCheckRequestMetadata(20),
				Consistency:          openfgav1.ConsistencyPreference_MINIMIZE_LATENCY,
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(0).Return(result, nil)
			},
		},
		{
			name: "same_request_returns_results_from_cache_when_no_consistency_requested",
			subsequentReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
				RequestMetadata:      NewCheckRequestMetadata(20),
				Consistency:          openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(0).Return(result, nil)
			},
		},
		{
			name: "same_request_does_not_use_cache_if_higher_consistency_requested",
			subsequentReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
				RequestMetadata:      NewCheckRequestMetadata(20),
				Consistency:          openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
		},
		{
			name: "result_added_to_cache_when_higher_consistency_requested",
			initialReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
				RequestMetadata:      NewCheckRequestMetadata(20),
				Consistency:          openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
			},
			subsequentReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
				RequestMetadata:      NewCheckRequestMetadata(20),
				Consistency:          openfgav1.ConsistencyPreference_MINIMIZE_LATENCY,
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(0).Return(result, nil)
			},
		},
		{
			name: "request_for_different_store_does_not_return_results_from_cache",
			subsequentReq: &ResolveCheckRequest{
				StoreID:              "22",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
				RequestMetadata:      NewCheckRequestMetadata(20),
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
		},
		{
			name: "request_for_different_model_id_does_not_return_results_from_cache",
			subsequentReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "34",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
				RequestMetadata:      NewCheckRequestMetadata(20),
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
		},
		{
			name: "request_for_different_tuple_object_does_not_return_results_from_cache",
			subsequentReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abcd", "reader", "user:XYZ"),
				RequestMetadata:      NewCheckRequestMetadata(20),
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
		},
		{
			name: "request_for_different_tuple_relation_does_not_return_results_from_cache",
			subsequentReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "owner", "user:XYZ"),
				RequestMetadata:      NewCheckRequestMetadata(20),
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
		},
		{
			name: "request_for_different_tuple_user_does_not_return_results_from_cache",
			subsequentReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:AAA"),
				RequestMetadata:      NewCheckRequestMetadata(20),
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
		},
		{
			name: "request_with_different_contextual_tuple_does_not_return_results_from_cache",
			subsequentReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
				ContextualTuples: []*openfgav1.TupleKey{
					{
						Object:   "document:xxx",
						Relation: "reader",
						User:     "user:XYZ",
					},
				},
				RequestMetadata: NewCheckRequestMetadata(20),
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
		},
		{
			name: "response_with_error_not_cached",
			subsequentReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
				RequestMetadata:      NewCheckRequestMetadata(20),
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(nil, fmt.Errorf("Mock error"))
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
		},
		{
			name: "identical_contextual_tuples_return_results_from_cache",
			initialReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
				ContextualTuples: []*openfgav1.TupleKey{
					{
						Object:   "document:aaa",
						Relation: "reader",
						User:     "user:XYZ",
					},
					{
						Object:   "document:xxx",
						Relation: "reader",
						User:     "user:XYZ",
					},
				},
				RequestMetadata: NewCheckRequestMetadata(20),
			},
			subsequentReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
				ContextualTuples: []*openfgav1.TupleKey{
					{
						Object:   "document:aaa",
						Relation: "reader",
						User:     "user:XYZ",
					},
					{
						Object:   "document:xxx",
						Relation: "reader",
						User:     "user:XYZ",
					},
				},
				RequestMetadata: NewCheckRequestMetadata(20),
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(0).Return(result, nil)
			},
		},
		{
			name: "different_order_contextual_tuples_results_in_cache_hit",
			initialReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
				ContextualTuples: []*openfgav1.TupleKey{
					{
						Object:   "document:xxx",
						Relation: "reader",
						User:     "user:XYZ",
					},
					{
						Object:   "document:aaa",
						Relation: "reader",
						User:     "user:XYZ",
					},
				},
				RequestMetadata: NewCheckRequestMetadata(20),
			},
			subsequentReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
				ContextualTuples: []*openfgav1.TupleKey{
					{
						Object:   "document:aaa",
						Relation: "reader",
						User:     "user:XYZ",
					},
					{
						Object:   "document:xxx",
						Relation: "reader",
						User:     "user:XYZ",
					},
				},
				RequestMetadata: NewCheckRequestMetadata(20),
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(0)
			},
		},
		{
			name: "separates_tuple_key_and_contextual_tuples",
			initialReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:pre"),
				ContextualTuples: []*openfgav1.TupleKey{
					{
						Object:   "fix:1",
						Relation: "reader",
						User:     "user:XYZ",
					},
				},
				RequestMetadata: NewCheckRequestMetadata(20),
			},
			subsequentReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:prefi"),
				ContextualTuples: []*openfgav1.TupleKey{
					{
						Object:   "x:1",
						Relation: "reader",
						User:     "user:XYZ",
					},
				},
				RequestMetadata: NewCheckRequestMetadata(20),
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
		},
		{
			name: "extra_contextual_tuples_does_not_return_results_from_cache",
			initialReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
				ContextualTuples: []*openfgav1.TupleKey{
					{
						Object:   "document:aaa",
						Relation: "reader",
						User:     "user:XYZ",
					},
					{
						Object:   "document:xxx",
						Relation: "reader",
						User:     "user:XYZ",
					},
				},
				RequestMetadata: NewCheckRequestMetadata(20),
			},
			subsequentReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
				ContextualTuples: []*openfgav1.TupleKey{
					{
						Object:   "document:aaa",
						Relation: "reader",
						User:     "user:XYZ",
					},
					{
						Object:   "document:xxx",
						Relation: "reader",
						User:     "user:XYZ",
					},
					{
						Object:   "document:yyy",
						Relation: "reader",
						User:     "user:XYZ",
					},
				},
				RequestMetadata: NewCheckRequestMetadata(20),
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
		},
		{
			name: "first_contextual_tuples_then_no_contextual_tuples_does_not_return_results_from_cache",
			initialReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
				ContextualTuples: []*openfgav1.TupleKey{
					{
						Object:   "document:aaa",
						Relation: "reader",
						User:     "user:XYZ",
					},
					{
						Object:   "document:xxx",
						Relation: "reader",
						User:     "user:XYZ",
					},
				},
				RequestMetadata: NewCheckRequestMetadata(20),
			},
			subsequentReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
				ContextualTuples:     []*openfgav1.TupleKey{},
				RequestMetadata:      NewCheckRequestMetadata(20),
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(gomock.Any(), request).Times(1).Return(result, nil)
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// build cached resolver
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockResolver := NewMockCheckResolver(ctrl)
			dut := NewCachedCheckResolver()
			defer dut.Close()
			dut.SetDelegate(mockResolver)

			// make assertions on initial call
			initialReq := req
			if test.initialReq != nil {
				initialReq = test.initialReq
			}
			test.setInitialResult(mockResolver, initialReq)
			actualResult, err := dut.ResolveCheck(ctx, initialReq)
			if err == nil {
				require.Equal(t, result.Allowed, actualResult.Allowed)
			}

			// make assertions on subsequent call
			test.setTestExpectations(mockResolver, test.subsequentReq)
			actualResult, err = dut.ResolveCheck(ctx, test.subsequentReq)
			require.NoError(t, err)
			require.Equal(t, result.Allowed, actualResult.Allowed)
		})
	}
}

func TestResolveCheck_ConcurrentCachedReadsAndWrites(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockCheckResolver := NewMockCheckResolver(ctrl)

	dut := NewCachedCheckResolver(WithCacheTTL(10 * time.Second))
	t.Cleanup(dut.Close)

	dut.SetDelegate(mockCheckResolver)

	mockCheckResolver.EXPECT().
		ResolveCheck(gomock.Any(), gomock.Any()).
		Return(&ResolveCheckResponse{
			Allowed: true,
			ResolutionMetadata: &ResolveCheckResponseMetadata{
				DatastoreQueryCount: 1,
				CycleDetected:       false,
			},
		}, nil)

	_, err := dut.ResolveCheck(context.Background(), &ResolveCheckRequest{})
	require.NoError(t, err)

	// run multiple times to increase probability of ensuring we detect the race
	for i := 0; i < 100; i++ {
		var wg sync.WaitGroup
		wg.Add(2)

		var resp1, resp2 *ResolveCheckResponse
		var err1, err2 error
		go func() {
			defer wg.Done()
			resp1, err1 = dut.ResolveCheck(context.Background(), &ResolveCheckRequest{})
			resp1.GetResolutionMetadata().DatastoreQueryCount = 0
		}()

		var datastoreQueryCount uint32
		go func() {
			defer wg.Done()
			resp2, err2 = dut.ResolveCheck(context.Background(), &ResolveCheckRequest{})
			datastoreQueryCount = resp2.GetResolutionMetadata().DatastoreQueryCount
		}()

		wg.Wait()

		require.NoError(t, err1)
		require.NoError(t, err2)
		require.NotNil(t, resp1)
		require.NotNil(t, resp2)
		require.Equal(t, uint32(0), datastoreQueryCount)
		require.False(t, resp1.GetCycleDetected())
		require.False(t, resp2.GetCycleDetected())
	}
}

func TestResolveCheckExpired(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	req := &ResolveCheckRequest{
		StoreID:              "12",
		AuthorizationModelID: "33",
		TupleKey: &openfgav1.TupleKey{
			Object:   "document:abc",
			Relation: "reader",
			User:     "user:XYZ",
		},
		RequestMetadata: NewCheckRequestMetadata(20),
	}

	result := &ResolveCheckResponse{Allowed: true}
	initialMockResolver := NewMockCheckResolver(ctrl)
	initialMockResolver.EXPECT().ResolveCheck(gomock.Any(), req).Times(2).Return(result, nil)

	// expect first call to result in actual resolve call
	dut := NewCachedCheckResolver(WithCacheTTL(1 * time.Microsecond))
	defer dut.Close()

	dut.SetDelegate(initialMockResolver)

	actualResult, err := dut.ResolveCheck(ctx, req)
	require.Equal(t, result.Allowed, actualResult.Allowed)
	require.NoError(t, err)

	// subsequent call would have cache timeout and result in new ResolveCheck
	time.Sleep(5 * time.Microsecond)

	actualResult, err = dut.ResolveCheck(ctx, req)
	require.Equal(t, result.Allowed, actualResult.Allowed)
	require.NoError(t, err)
}

func TestCachedCheckResolver_FieldsInResponse(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	cachedCheckResolver := NewCachedCheckResolver()
	defer cachedCheckResolver.Close()

	mockCtrl := gomock.NewController(t)
	t.Cleanup(mockCtrl.Finish)

	mockCheckResolver := NewMockCheckResolver(mockCtrl)
	cachedCheckResolver.SetDelegate(mockCheckResolver)

	mockCheckResolver.EXPECT().
		ResolveCheck(gomock.Any(), gomock.Any()).
		Return(&ResolveCheckResponse{
			Allowed: false,
			ResolutionMetadata: &ResolveCheckResponseMetadata{
				DatastoreQueryCount: 1,
				CycleDetected:       true,
			},
		}, nil)

	resp, err := cachedCheckResolver.ResolveCheck(context.Background(), &ResolveCheckRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint32(1), resp.GetResolutionMetadata().DatastoreQueryCount)
	require.True(t, resp.GetResolutionMetadata().CycleDetected)

	resp, err = cachedCheckResolver.ResolveCheck(context.Background(), &ResolveCheckRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint32(0), resp.GetResolutionMetadata().DatastoreQueryCount)
	require.True(t, resp.GetResolutionMetadata().CycleDetected)
}

func TestCachedCheckDatastoreQueryCount(t *testing.T) {
	t.Parallel()

	ds := memory.New()
	defer ds.Close()

	storeID := ulid.Make().String()

	err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:x", "parent", "org:fga"),
		tuple.NewTupleKey("org:fga", "member", "user:maria"),
	})
	require.NoError(t, err)

	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1

		type user

		type org
			relations
				define member: [user]

		type document
			relations
				define ttu: member from parent
				define parent: [org]`)

	ts, err := typesystem.New(model)
	require.NoError(t, err)
	ctx := typesystem.ContextWithTypesystem(
		context.Background(),
		ts,
	)

	ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockCache := mocks.NewMockInMemoryCache[any](mockController)

	cachedCheckResolver := NewCachedCheckResolver(
		WithExistingCache(mockCache),
		WithCacheTTL(10*time.Hour),
	)
	defer cachedCheckResolver.Close()

	localCheckResolver := NewLocalChecker(
		WithMaxConcurrentReads(1),
		WithOptimizations(true),
	)
	defer localCheckResolver.Close()

	cachedCheckResolver.SetDelegate(localCheckResolver)
	localCheckResolver.SetDelegate(cachedCheckResolver)

	req := &ResolveCheckRequest{
		StoreID:          storeID,
		TupleKey:         tuple.NewTupleKey("org:fga", "member", "user:maria"),
		ContextualTuples: nil,
		RequestMetadata:  NewCheckRequestMetadata(25),
	}
	reqKey, err := CheckRequestCacheKey(req)
	require.NoError(t, err)

	// The first check is a cache miss, so goes to DB and populates cache.
	mockCache.EXPECT().Get(reqKey).Times(1).Return(nil)
	mockCache.EXPECT().Set(reqKey, gomock.Any(), gomock.Any()).Times(1)
	res, err := cachedCheckResolver.ResolveCheck(ctx, req)

	require.NoError(t, err)
	require.Equal(t, uint32(1), res.GetResolutionMetadata().DatastoreQueryCount)

	// The second check is a cache hit.
	mockCache.EXPECT().Get(reqKey).Times(1).Return(&ResolveCheckResponse{Allowed: true})
	res, err = cachedCheckResolver.ResolveCheck(ctx, req)

	require.NoError(t, err)
	require.Equal(t, uint32(0), res.GetResolutionMetadata().DatastoreQueryCount)

	// For TTU fastpath, we no longer call ResolveCheck to get the parent / child.
	// As such, it should not have called the cache.
	mockCache.EXPECT().Get(reqKey).Times(0).Return(&ResolveCheckResponse{Allowed: true})
	res, err = localCheckResolver.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:          storeID,
		TupleKey:         tuple.NewTupleKey("document:x", "ttu", "user:maria"),
		ContextualTuples: nil,
		RequestMetadata:  NewCheckRequestMetadata(25),
	})

	require.NoError(t, err)
	require.Equal(t, uint32(2), res.GetResolutionMetadata().DatastoreQueryCount)
}

func TestCachedCheckResolver_ResolveCheck_After_Stop_DoesNotPanic(t *testing.T) {
	cachedCheckResolver := NewCachedCheckResolver(WithExistingCache(nil)) // create cache inside

	mockCtrl := gomock.NewController(t)
	t.Cleanup(mockCtrl.Finish)

	mockCheckResolver := NewMockCheckResolver(mockCtrl)
	cachedCheckResolver.SetDelegate(mockCheckResolver)

	mockCheckResolver.EXPECT().
		ResolveCheck(gomock.Any(), gomock.Any()).
		Times(1).
		Return(&ResolveCheckResponse{
			Allowed: false,
			ResolutionMetadata: &ResolveCheckResponseMetadata{
				DatastoreQueryCount: 1,
				CycleDetected:       true,
			},
		}, nil)

	cachedCheckResolver.Close()
	resp, err := cachedCheckResolver.ResolveCheck(context.Background(), &ResolveCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, uint32(1), resp.GetResolutionMetadata().DatastoreQueryCount)
}

func TestCheckCacheKeyDoNotOverlap(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	key1, err := CheckRequestCacheKey(&ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:x", "viewer", "user:jon"),
		RequestMetadata:      NewCheckRequestMetadata(25),
	})
	require.NoError(t, err)

	key2, err := CheckRequestCacheKey(&ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:x", "viewer", "user:jon"),
		ContextualTuples: []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		},
		RequestMetadata: NewCheckRequestMetadata(25),
	})
	require.NoError(t, err)

	contextStruct, err := structpb.NewStruct(map[string]interface{}{
		"key1": true,
	})
	require.NoError(t, err)

	key3, err := CheckRequestCacheKey(&ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:x", "viewer", "user:jon"),
		Context:              contextStruct,
		RequestMetadata:      NewCheckRequestMetadata(25),
	})
	require.NoError(t, err)

	// two Check request cache keys should not overlap if contextual tuples are
	// provided in one and not the other and/or if context is provided in one
	// and not the other
	require.NotEqual(t, key1, key2)
	require.NotEqual(t, key2, key3)
	require.NotEqual(t, key1, key3)
}

func TestCheckCacheKey_ContextualTuplesOrdering(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	tuples1 := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:anne"),
		tuple.NewTupleKey("document:2", "admin", "user:jon"),
	}

	tuples2 := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:2", "admin", "user:jon"),
		tuple.NewTupleKey("document:1", "viewer", "user:anne"),
	}

	tupleKey := tuple.NewTupleKey("document:x", "viewer", "user:jon")

	key1, err := CheckRequestCacheKey(&ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     tuples1,
		RequestMetadata:      NewCheckRequestMetadata(25),
	})
	require.NoError(t, err)

	key2, err := CheckRequestCacheKey(&ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     tuples2,
		RequestMetadata:      NewCheckRequestMetadata(25),
	})
	require.NoError(t, err)

	require.Equal(t, key1, key2)
}

func TestCheckCacheKey_ContextualTuplesWithConditionsOrdering(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	tuples1 := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:anne"),
		tuple.NewTupleKeyWithCondition("document:2", "admin", "user:jon", "some_condition", nil),
		tuple.NewTupleKeyWithCondition("document:2", "admin", "user:jon", "some_other_condition", nil),
	}

	tuples2 := []*openfgav1.TupleKey{
		tuple.NewTupleKeyWithCondition("document:2", "admin", "user:jon", "some_other_condition", nil),
		tuple.NewTupleKeyWithCondition("document:2", "admin", "user:jon", "some_condition", nil),
		tuple.NewTupleKey("document:1", "viewer", "user:anne"),
	}

	tupleKey := tuple.NewTupleKey("document:x", "viewer", "user:jon")

	key1, err := CheckRequestCacheKey(&ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     tuples1,
		RequestMetadata:      NewCheckRequestMetadata(25),
	})
	require.NoError(t, err)

	key2, err := CheckRequestCacheKey(&ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     tuples2,
		RequestMetadata:      NewCheckRequestMetadata(25),
	})
	require.NoError(t, err)

	require.Equal(t, key1, key2)
}

func TestCheckCacheKeyWithContext(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	struct1, err := structpb.NewStruct(map[string]interface{}{
		"key1": "foo",
		"key2": "bar",
	})
	require.NoError(t, err)

	struct2, err := structpb.NewStruct(map[string]interface{}{
		"key2": "bar",
		"key1": "foo",
	})
	require.NoError(t, err)

	struct3, err := structpb.NewStruct(map[string]interface{}{
		"key2": "x",
		"key1": "foo",
	})
	require.NoError(t, err)

	struct4, err := structpb.NewStruct(map[string]interface{}{
		"key2": "x",
		"key1": true,
	})
	require.NoError(t, err)

	key1, err := CheckRequestCacheKey(&ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		Context:              struct1,
		RequestMetadata:      NewCheckRequestMetadata(25),
	})
	require.NoError(t, err)

	key2, err := CheckRequestCacheKey(&ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		Context:              struct2,
		RequestMetadata:      NewCheckRequestMetadata(25),
	})
	require.NoError(t, err)

	key3, err := CheckRequestCacheKey(&ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		Context:              struct3,
		RequestMetadata:      NewCheckRequestMetadata(25),
	})
	require.NoError(t, err)

	key4, err := CheckRequestCacheKey(&ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		Context:              struct4,
		RequestMetadata:      NewCheckRequestMetadata(25),
	})
	require.NoError(t, err)

	require.Equal(t, key1, key2)
	require.NotEqual(t, key1, key3)
	require.NotEqual(t, key1, key4)
	require.NotEqual(t, key3, key4)
}

func BenchmarkCheckRequestCacheKey(b *testing.B) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	var err error

	for n := 0; n < b.N; n++ {
		_, err = CheckRequestCacheKey(&ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: modelID,
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			RequestMetadata:      NewCheckRequestMetadata(25),
		})
		require.NoError(b, err)
	}
}

func BenchmarkCheckRequestCacheKeyWithContextualTuples(b *testing.B) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	var err error

	tuples := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:x", "viewer", "user:x"),
		tuple.NewTupleKey("document:y", "viewer", "user:y"),
		tuple.NewTupleKey("document:z", "viewer", "user:z"),
	}

	for n := 0; n < b.N; n++ {
		_, err = CheckRequestCacheKey(&ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: modelID,
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			ContextualTuples:     tuples,
			RequestMetadata:      NewCheckRequestMetadata(25),
		})
		require.NoError(b, err)
	}
}

func BenchmarkCheckRequestCacheKeyWithContext(b *testing.B) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	var err error

	contextStruct, err := structpb.NewStruct(map[string]interface{}{
		"boolKey":   true,
		"stringKey": "hello",
		"numberKey": 1.2,
		"nullKey":   nil,
		"structKey": map[string]interface{}{
			"key1": "value1",
		},
		"listKey": []interface{}{"item1", "item2"},
	})
	require.NoError(b, err)

	for n := 0; n < b.N; n++ {
		_, err = CheckRequestCacheKey(&ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: modelID,
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			Context:              contextStruct,
			RequestMetadata:      NewCheckRequestMetadata(25),
		})
		require.NoError(b, err)
	}
}
