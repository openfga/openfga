package graph

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
)

func TestResolveCheckFromCache(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ctx := context.Background()

	req, err := NewResolveCheckRequest(ResolveCheckRequestParams{
		StoreID:              "12",
		AuthorizationModelID: "33",
		TupleKey: &openfgav1.TupleKey{
			Object:   "document:abc",
			Relation: "reader",
			User:     "user:XYZ",
		},
	})
	require.NoError(t, err)

	result := &ResolveCheckResponse{Allowed: true}

	// if the tuple is different, it should result in fetching from cache
	tests := []struct {
		name                string
		initialReqParams    *ResolveCheckRequestParams
		subsequentReqParams *ResolveCheckRequestParams
		setInitialResult    func(mock *MockCheckResolver, request *ResolveCheckRequest)
		setTestExpectations func(mock *MockCheckResolver, request *ResolveCheckRequest)
	}{
		{
			name: "same_request_returns_results_from_cache",
			subsequentReqParams: &ResolveCheckRequestParams{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
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
			subsequentReqParams: &ResolveCheckRequestParams{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
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
			subsequentReqParams: &ResolveCheckRequestParams{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
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
			subsequentReqParams: &ResolveCheckRequestParams{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
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
			initialReqParams: &ResolveCheckRequestParams{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
				Consistency:          openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
			},
			subsequentReqParams: &ResolveCheckRequestParams{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
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
			subsequentReqParams: &ResolveCheckRequestParams{
				StoreID:              "22",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
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
			subsequentReqParams: &ResolveCheckRequestParams{
				StoreID:              "12",
				AuthorizationModelID: "34",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
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
			subsequentReqParams: &ResolveCheckRequestParams{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abcd", "reader", "user:XYZ"),
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
			subsequentReqParams: &ResolveCheckRequestParams{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "owner", "user:XYZ"),
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
			subsequentReqParams: &ResolveCheckRequestParams{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:AAA"),
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
			subsequentReqParams: &ResolveCheckRequestParams{
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
			subsequentReqParams: &ResolveCheckRequestParams{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
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
			initialReqParams: &ResolveCheckRequestParams{
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
			},
			subsequentReqParams: &ResolveCheckRequestParams{
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
			initialReqParams: &ResolveCheckRequestParams{
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
			},
			subsequentReqParams: &ResolveCheckRequestParams{
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
			initialReqParams: &ResolveCheckRequestParams{
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
			},
			subsequentReqParams: &ResolveCheckRequestParams{
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
			initialReqParams: &ResolveCheckRequestParams{
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
			},
			subsequentReqParams: &ResolveCheckRequestParams{
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
			initialReqParams: &ResolveCheckRequestParams{
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
			},
			subsequentReqParams: &ResolveCheckRequestParams{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
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
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// build cached resolver
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockResolver := NewMockCheckResolver(ctrl)
			dut, err := NewCachedCheckResolver()
			require.NoError(t, err)
			defer dut.Close()
			dut.SetDelegate(mockResolver)

			// make assertions on initial call
			initialReq := req
			if test.initialReqParams != nil {
				initialReq, err = NewResolveCheckRequest(*test.initialReqParams)
				require.NoError(t, err)
			}
			test.setInitialResult(mockResolver, initialReq)
			actualResult, err := dut.ResolveCheck(ctx, initialReq)
			if err == nil {
				require.Equal(t, result.Allowed, actualResult.Allowed)
			}

			subsequentParams := *test.subsequentReqParams

			// Ensure cached entry is newer than the most recent non-zero invalidation time
			subsequentParams.LastCacheInvalidationTime = time.Now().Add(-1 * time.Minute)

			subsequentRequest, err := NewResolveCheckRequest(subsequentParams)
			require.NoError(t, err)

			test.setTestExpectations(mockResolver, subsequentRequest)
			actualResult, err = dut.ResolveCheck(ctx, subsequentRequest)
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

	dut, err := NewCachedCheckResolver(WithCacheTTL(10 * time.Second))
	require.NoError(t, err)
	t.Cleanup(dut.Close)

	dut.SetDelegate(mockCheckResolver)

	mockCheckResolver.EXPECT().
		ResolveCheck(gomock.Any(), gomock.Any()).
		Return(&ResolveCheckResponse{
			Allowed: true,
		}, nil)

	_, err = dut.ResolveCheck(context.Background(), &ResolveCheckRequest{})
	require.NoError(t, err)

	// run multiple times to increase probability of ensuring we detect the race
	for i := 0; i < 100; i++ {
		var wg sync.WaitGroup
		wg.Add(2)

		var resp1, resp2 *ResolveCheckResponse
		var err1, err2 error
		go func() {
			defer wg.Done()
			resp1, err1 = dut.ResolveCheck(context.Background(), &ResolveCheckRequest{LastCacheInvalidationTime: time.Now().Add(-1 * time.Minute)})
		}()

		go func() {
			defer wg.Done()
			resp2, err2 = dut.ResolveCheck(context.Background(), &ResolveCheckRequest{LastCacheInvalidationTime: time.Now().Add(-1 * time.Minute)})
		}()

		wg.Wait()

		require.NoError(t, err1)
		require.NoError(t, err2)
		require.NotNil(t, resp1)
		require.NotNil(t, resp2)
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
		TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
		RequestMetadata:      NewCheckRequestMetadata(),
	}

	result := &ResolveCheckResponse{Allowed: true}
	initialMockResolver := NewMockCheckResolver(ctrl)
	initialMockResolver.EXPECT().ResolveCheck(gomock.Any(), req).Times(2).Return(result, nil)

	// expect first call to result in actual resolve call
	dut, err := NewCachedCheckResolver(WithCacheTTL(1 * time.Microsecond))
	require.NoError(t, err)
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

func TestResolveCheckLastChangelogRecent(t *testing.T) {
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
		RequestMetadata:           NewCheckRequestMetadata(),
		LastCacheInvalidationTime: time.Now().Add(5 * time.Minute),
	}

	result := &ResolveCheckResponse{Allowed: true}
	initialMockResolver := NewMockCheckResolver(ctrl)
	initialMockResolver.EXPECT().ResolveCheck(gomock.Any(), req).Times(2).Return(result, nil)

	// expect first call to result in actual resolve call
	dut, err := NewCachedCheckResolver(WithCacheTTL(1 * time.Hour))
	require.NoError(t, err)
	defer dut.Close()

	dut.SetDelegate(initialMockResolver)

	actualResult, err := dut.ResolveCheck(ctx, req)
	require.Equal(t, result.Allowed, actualResult.Allowed)
	require.NoError(t, err)

	actualResult, err = dut.ResolveCheck(ctx, req)
	require.Equal(t, result.Allowed, actualResult.Allowed)
	require.NoError(t, err)
}

func TestCachedCheckResolver_FieldsInResponse(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	cachedCheckResolver, err := NewCachedCheckResolver()
	require.NoError(t, err)
	defer cachedCheckResolver.Close()

	mockCtrl := gomock.NewController(t)
	t.Cleanup(mockCtrl.Finish)

	mockCheckResolver := NewMockCheckResolver(mockCtrl)
	cachedCheckResolver.SetDelegate(mockCheckResolver)

	mockCheckResolver.EXPECT().
		ResolveCheck(gomock.Any(), gomock.Any()).
		Return(&ResolveCheckResponse{
			Allowed: false,
			ResolutionMetadata: ResolveCheckResponseMetadata{
				CycleDetected: true,
			},
		}, nil).Times(2)

	resp, err := cachedCheckResolver.ResolveCheck(context.Background(), &ResolveCheckRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.GetResolutionMetadata().CycleDetected)

	// we expect the underlying resolve check to be called twice because we are not saving the response.
	resp, err = cachedCheckResolver.ResolveCheck(context.Background(), &ResolveCheckRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.GetResolutionMetadata().CycleDetected)
}

func TestBuildCacheKey(t *testing.T) {
	req, err := NewResolveCheckRequest(ResolveCheckRequestParams{
		StoreID: "abc123",
		TupleKey: &openfgav1.TupleKey{
			Object:   "document:abc",
			Relation: "reader",
			User:     "user:XYZ",
		},
		AuthorizationModelID: "def456",
	})
	require.NoError(t, err)

	result := BuildCacheKey(*req)
	require.NotEmpty(t, result)
}
