package graph

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
)

func TestResolveCheck(t *testing.T) {
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
		_name   string
		req     *ResolveCheckRequest
		prepare func(mock *MockCheckResolver, request *ResolveCheckRequest)
	}{
		{
			// same signature means data will be taken from cache
			_name: "same_signature",
			req: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey: &openfgav1.TupleKey{
					Object:   "document:abc",
					Relation: "reader",
					User:     "user:XYZ",
				},
			},
			prepare: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				// there should be no call
			},
		},
		{
			// different store means data is not from cache
			_name: "different_store",
			req: &ResolveCheckRequest{
				StoreID:              "22",
				AuthorizationModelID: "33",
				TupleKey: &openfgav1.TupleKey{
					Object:   "document:abc",
					Relation: "reader",
					User:     "user:XYZ",
				},
			},
			prepare: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
		},
		{
			// different model id means data is not from cache
			_name: "different_model_id",
			req: &ResolveCheckRequest{
				StoreID:              "11",
				AuthorizationModelID: "34",
				TupleKey: &openfgav1.TupleKey{
					Object:   "document:abc",
					Relation: "reader",
					User:     "user:XYZ",
				},
			},
			prepare: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
		},
		{
			// different tuple means data is not from cache
			_name: "different_tuple_object",
			req: &ResolveCheckRequest{
				StoreID:              "11",
				AuthorizationModelID: "33",
				TupleKey: &openfgav1.TupleKey{
					Object:   "document:abcd",
					Relation: "reader",
					User:     "user:XYZ",
				},
			},
			prepare: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
		},
		{
			// different tuple means data is not from cache
			_name: "different_tuple_relation",
			req: &ResolveCheckRequest{
				StoreID:              "11",
				AuthorizationModelID: "33",
				TupleKey: &openfgav1.TupleKey{
					Object:   "document:abc",
					Relation: "owner",
					User:     "user:XYZ",
				},
			},
			prepare: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
				mock.EXPECT().ResolveCheck(ctx, request).Times(1).Return(result, nil)
			},
		},
		{
			// different tuple means data is not from cache
			_name: "different_tuple_user",
			req: &ResolveCheckRequest{
				StoreID:              "11",
				AuthorizationModelID: "33",
				TupleKey: &openfgav1.TupleKey{
					Object:   "document:abc",
					Relation: "reader",
					User:     "user:AAA",
				},
			},
			prepare: func(mock *MockCheckResolver, request *ResolveCheckRequest) {
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
			test.prepare(newResolver, test.req)
			dut2 := NewCachedCheckResolver(newResolver, WithExistingCache(dut.cache))
			actualResult, err = dut2.ResolveCheck(ctx, test.req)
			require.Equal(t, result, actualResult)
			require.Equal(t, nil, err)

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
	require.Equal(t, nil, err)
}
