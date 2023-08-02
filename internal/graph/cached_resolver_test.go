package graph

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
)

func TestResolveCheckFromCache(t *testing.T) {
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

	// if the tuple is different, it should result in fetching from cache
	tests := []struct {
		_name               string
		req                 *ResolveCheckRequest
		setTestExpectations func(mock *MockCheckResolver, request *ResolveCheckRequest)
	}{
		{
			// same signature means data will be taken from cache
			_name: "same_request_returns_results_from_cache",
			req: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey: &openfgav1.TupleKey{
					Object:   "document:abc",
					Relation: "reader",
					User:     "user:XYZ",
				},
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(0).Return(result, nil)
			},
		},
		{
			// different store means data is not from cache
			_name: "request_for_different_store_does_not_return_results_from_cache",
			req: &ResolveCheckRequest{
				StoreID:              "22",
				AuthorizationModelID: "33",
				TupleKey: &openfgav1.TupleKey{
					Object:   "document:abc",
					Relation: "reader",
					User:     "user:XYZ",
				},
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
		},
		{
			// different model id means data is not from cache
			_name: "request_for_different_model_id_does_not_return_results_from_cache",
			req: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "34",
				TupleKey: &openfgav1.TupleKey{
					Object:   "document:abc",
					Relation: "reader",
					User:     "user:XYZ",
				},
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
		},
		{
			// different tuple means data is not from cache
			_name: "request_for_different_tuple_object_does_not_return_results_from_cache",
			req: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey: &openfgav1.TupleKey{
					Object:   "document:abcd",
					Relation: "reader",
					User:     "user:XYZ",
				},
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
		},
		{
			// different tuple means data is not from cache
			_name: "request_for_different_tuple_relation_does_not_return_results_from_cache",
			req: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey: &openfgav1.TupleKey{
					Object:   "document:abc",
					Relation: "owner",
					User:     "user:XYZ",
				},
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
		},
		{
			// different tuple means data is not from cache
			_name: "request_for_different_tuple_user_does_not_return_results_from_cache",
			req: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey: &openfgav1.TupleKey{
					Object:   "document:abc",
					Relation: "reader",
					User:     "user:AAA",
				},
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
		},
		{
			// contextual tuples should result in a different request
			_name: "request_with_different_contextual_tuple_does_not_return_results_from_cache",
			req: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey: &openfgav1.TupleKey{
					Object:   "document:abc",
					Relation: "reader",
					User:     "user:XYZ",
				},
				ContextualTuples: []*openfgav1.TupleKey{
					&openfgav1.TupleKey{
						Object:   "document:xxx",
						Relation: "reader",
						User:     "user:XYZ",
					},
				},
			},
			setTestExpectations: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test._name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			initialMockResolver := NewMockCheckResolver(ctrl)
			initialMockResolver.EXPECT().ResolveCheck(ctx, req).Times(1).Return(result, nil)

			// expect first call to result in actual resolve call
			dut := NewCachedCheckResolver(initialMockResolver)
			actualResult, err := dut.ResolveCheck(ctx, req)
			require.Equal(t, result, actualResult)
			require.Equal(t, nil, err)

			newCtrl := gomock.NewController(t)
			defer newCtrl.Finish()

			newResolver := NewMockCheckResolver(newCtrl)
			test.setTestExpectations(newResolver, test.req)
			dut2 := NewCachedCheckResolver(newResolver, WithExistingCache(dut.cache))
			actualResult, err = dut2.ResolveCheck(ctx, test.req)
			require.Equal(t, result, actualResult)
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
	require.Equal(t, result, actualResult)
	require.Equal(t, nil, err)

	// subsequent call would have cache timeout and result in new ResolveCheck
	time.Sleep(5 * time.Microsecond)

	actualResult, err = dut.ResolveCheck(ctx, req)
	require.Equal(t, result, actualResult)
	require.Nil(t, err)
}
