package graph

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/stretchr/testify/require"
)

func TestResolveCheckFromCache(t *testing.T) {
	ctx := context.Background()

	req := &ResolveCheckRequest{
		StoreID:              "12",
		AuthorizationModelID: "33",
		TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
	}
	result := &ResolveCheckResponse{Allowed: true}

	// if the tuple is different, it should result in fetching from cache
	tests := []struct {
		name                string
		req                 *ResolveCheckRequest
		setInitialResult    func(mock *MockCheckResolver, request *ResolveCheckRequest)
		setTestExpectations func(mock *MockCheckResolver, request *ResolveCheckRequest)
	}{
		{
			// same signature means data will be taken from cache
			name: "same_request_returns_results_from_cache",
			req: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(0).Return(result, nil)
			},
		},
		{
			// different store means data is not from cache
			name: "request_for_different_store_does_not_return_results_from_cache",
			req: &ResolveCheckRequest{
				StoreID:              "22",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
		},
		{
			// different model id means data is not from cache
			name: "request_for_different_model_id_does_not_return_results_from_cache",
			req: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "34",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
		},
		{
			// different tuple means data is not from cache
			name: "request_for_different_tuple_object_does_not_return_results_from_cache",
			req: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abcd", "reader", "user:XYZ"),
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
		},
		{
			// different tuple means data is not from cache
			name: "request_for_different_tuple_relation_does_not_return_results_from_cache",
			req: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "owner", "user:XYZ"),
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
		},
		{
			// different tuple means data is not from cache
			name: "request_for_different_tuple_user_does_not_return_results_from_cache",
			req: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:AAA"),
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
		},
		{
			// contextual tuples should result in a different request
			name: "request_with_different_contextual_tuple_does_not_return_results_from_cache",
			req: &ResolveCheckRequest{
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
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
		},
		{
			// error result should not be cached
			name: "response_with_error_not_cached",
			req: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
			},
			setInitialResult: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(nil, fmt.Errorf("Mock error"))
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			initialMockResolver := NewMockCheckResolver(ctrl)
			test.setInitialResult(initialMockResolver, req)

			// expect first call to result in actual resolve call
			dut := NewCachedCheckResolver(initialMockResolver,
				WithLogger(logger.NewNoopLogger()),
				WithMaxCacheSize(10))
			_, _ = dut.ResolveCheck(ctx, req)

			newCtrl := gomock.NewController(t)
			defer newCtrl.Finish()

			newResolver := NewMockCheckResolver(newCtrl)
			test.setTestExpectations(newResolver, test.req)
			dut2 := NewCachedCheckResolver(newResolver, WithExistingCache(dut.cache))
			actualResult, err := dut2.ResolveCheck(ctx, test.req)
			require.Equal(t, result.Allowed, actualResult.Allowed)
			require.Nil(t, err)

		})
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
	}

	result := &ResolveCheckResponse{Allowed: true}
	initialMockResolver := NewMockCheckResolver(ctrl)
	initialMockResolver.EXPECT().ResolveCheck(ctx, req).Times(2).Return(result, nil)

	// expect first call to result in actual resolve call
	dut := NewCachedCheckResolver(initialMockResolver, WithCacheTTL(1*time.Microsecond))
	actualResult, err := dut.ResolveCheck(ctx, req)
	require.Equal(t, result.Allowed, actualResult.Allowed)
	require.Equal(t, nil, err)

	// subsequent call would have cache timeout and result in new ResolveCheck
	time.Sleep(5 * time.Microsecond)

	actualResult, err = dut.ResolveCheck(ctx, req)
	require.Equal(t, result.Allowed, actualResult.Allowed)
	require.Nil(t, err)
}
