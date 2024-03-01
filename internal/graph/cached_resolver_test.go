package graph

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/karlseguin/ccache/v3"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"

	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestResolveCheckFromCache(t *testing.T) {
	ctx := context.Background()

	req := &ResolveCheckRequest{
		StoreID:              "12",
		AuthorizationModelID: "33",
		TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
		DispatchCounter:      &atomic.Uint32{},
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
				DispatchCounter:      &atomic.Uint32{},
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
				DispatchCounter:      &atomic.Uint32{},
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
				DispatchCounter:      &atomic.Uint32{},
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
				DispatchCounter:      &atomic.Uint32{},
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
				DispatchCounter:      &atomic.Uint32{},
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
				DispatchCounter:      &atomic.Uint32{},
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
				DispatchCounter: &atomic.Uint32{},
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
				DispatchCounter:      &atomic.Uint32{},
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
				DispatchCounter: &atomic.Uint32{},
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
				DispatchCounter: &atomic.Uint32{},
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
				DispatchCounter: &atomic.Uint32{},
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
				DispatchCounter: &atomic.Uint32{},
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
				DispatchCounter: &atomic.Uint32{},
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
				DispatchCounter: &atomic.Uint32{},
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
				DispatchCounter: &atomic.Uint32{},
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
				DispatchCounter: &atomic.Uint32{},
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
				DispatchCounter: &atomic.Uint32{},
			},
			subsequentReq: &ResolveCheckRequest{
				StoreID:              "12",
				AuthorizationModelID: "33",
				TupleKey:             tuple.NewTupleKey("document:abc", "reader", "user:XYZ"),
				ContextualTuples:     []*openfgav1.TupleKey{},
				DispatchCounter:      &atomic.Uint32{},
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
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockResolver := NewMockCheckResolver(ctrl)
			initialReq := req
			if test.initialReq != nil {
				initialReq = test.initialReq
			}
			test.setInitialResult(mockResolver, initialReq)

			// expect first call to result in actual resolve call
			dut := NewCachedCheckResolver(WithMaxCacheSize(10))
			defer dut.Close()

			dut.SetDelegate(mockResolver)

			_, _ = dut.ResolveCheck(ctx, initialReq)

			test.setTestExpectations(mockResolver, test.subsequentReq)

			dut2 := NewCachedCheckResolver(WithExistingCache(dut.cache))
			defer dut2.Close()

			dut2.SetDelegate(dut)

			actualResult, err := dut2.ResolveCheck(ctx, test.subsequentReq)
			require.Equal(t, result.Allowed, actualResult.Allowed)
			require.NoError(t, err)
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
		DispatchCounter: &atomic.Uint32{},
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

func TestCachedCheckDatastoreQueryCount(t *testing.T) {
	t.Parallel()

	ds := memory.New()
	defer ds.Close()

	storeID := ulid.Make().String()

	err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:x", "a", "user:jon"),
		tuple.NewTupleKey("document:x", "a", "user:maria"),
		tuple.NewTupleKey("document:x", "b", "user:maria"),
		tuple.NewTupleKey("document:x", "parent", "org:fga"),
		tuple.NewTupleKey("org:fga", "member", "user:maria"),
	})
	require.NoError(t, err)

	model := testutils.MustTransformDSLToProtoWithID(`model
	schema 1.1
type user

type org
  relations
	define member: [user]

type document
  relations
	define a: [user]
	define b: [user]
	define union: a or b
	define union_rewrite: union
	define intersection: a and b
	define difference: a but not b
	define ttu: member from parent
	define union_and_ttu: union and ttu
	define union_or_ttu: union or ttu or union_rewrite
	define intersection_of_ttus: union_or_ttu and union_and_ttu
	define parent: [org]`)

	ctx := typesystem.ContextWithTypesystem(
		context.Background(),
		typesystem.New(model),
	)

	ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

	checkCache := ccache.New(
		ccache.Configure[*CachedResolveCheckResponse]().MaxSize(100),
	)
	defer checkCache.Stop()

	cachedCheckResolver := NewCachedCheckResolver(
		WithExistingCache(checkCache),
		WithCacheTTL(10*time.Hour),
	)
	defer cachedCheckResolver.Close()

	// Running the first check
	localCheckResolver := NewLocalChecker(
		WithMaxConcurrentReads(1),
	)
	defer localCheckResolver.Close()

	cachedCheckResolver.SetDelegate(localCheckResolver)
	localCheckResolver.SetDelegate(cachedCheckResolver)

	res, err := cachedCheckResolver.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:            storeID,
		TupleKey:           tuple.NewTupleKey("org:fga", "member", "user:maria"),
		ContextualTuples:   nil,
		ResolutionMetadata: &ResolutionMetadata{Depth: 25},
		DispatchCounter:    &atomic.Uint32{},
	})

	require.NoError(t, err)
	require.Equal(t, uint32(1), res.GetResolutionMetadata().DatastoreQueryCount)

	res, err = cachedCheckResolver.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:            storeID,
		TupleKey:           tuple.NewTupleKey("org:fga", "member", "user:maria"),
		ContextualTuples:   nil,
		ResolutionMetadata: &ResolutionMetadata{Depth: 25},
		DispatchCounter:    &atomic.Uint32{},
	})

	require.NoError(t, err)
	require.Equal(t, uint32(0), res.GetResolutionMetadata().DatastoreQueryCount)

	// The ttuLocalChecker will use partial result from the cache and partial result from the local checker

	ttuLocalChecker := NewLocalChecker(
		WithMaxConcurrentReads(1),
	)
	ttuLocalChecker.SetDelegate(cachedCheckResolver)

	res, err = ttuLocalChecker.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:            storeID,
		TupleKey:           tuple.NewTupleKey("document:x", "ttu", "user:maria"),
		ContextualTuples:   nil,
		ResolutionMetadata: &ResolutionMetadata{Depth: 25},
		DispatchCounter:    &atomic.Uint32{},
	})

	ttuLocalChecker.Close()

	require.NoError(t, err)
	require.Equal(t, uint32(1), res.GetResolutionMetadata().DatastoreQueryCount)
}

func TestCheckCacheKeyDoNotOverlap(t *testing.T) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	key1, err := CheckRequestCacheKey(&ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:x", "viewer", "user:jon"),
		DispatchCounter:      &atomic.Uint32{},
	})
	require.NoError(t, err)

	key2, err := CheckRequestCacheKey(&ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:x", "viewer", "user:jon"),
		ContextualTuples: []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		},
		DispatchCounter: &atomic.Uint32{},
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
		DispatchCounter:      &atomic.Uint32{},
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
		DispatchCounter:      &atomic.Uint32{},
	})
	require.NoError(t, err)

	key2, err := CheckRequestCacheKey(&ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     tuples2,
		DispatchCounter:      &atomic.Uint32{},
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
		DispatchCounter:      &atomic.Uint32{},
	})
	require.NoError(t, err)

	key2, err := CheckRequestCacheKey(&ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tupleKey,
		ContextualTuples:     tuples2,
		DispatchCounter:      &atomic.Uint32{},
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
		DispatchCounter:      &atomic.Uint32{},
	})
	require.NoError(t, err)

	key2, err := CheckRequestCacheKey(&ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		Context:              struct2,
		DispatchCounter:      &atomic.Uint32{},
	})
	require.NoError(t, err)

	key3, err := CheckRequestCacheKey(&ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		Context:              struct3,
		DispatchCounter:      &atomic.Uint32{},
	})
	require.NoError(t, err)

	key4, err := CheckRequestCacheKey(&ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: modelID,
		TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		Context:              struct4,
		DispatchCounter:      &atomic.Uint32{},
	})
	require.NoError(t, err)

	require.Equal(t, key1, key2)
	require.NotEqual(t, key1, key3)
	require.NotEqual(t, key1, key4)
	require.NotEqual(t, key3, key4)
}

var checkCacheKey string

func BenchmarkCheckRequestCacheKey(b *testing.B) {
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	var err error

	for n := 0; n < b.N; n++ {
		checkCacheKey, err = CheckRequestCacheKey(&ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: modelID,
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			DispatchCounter:      &atomic.Uint32{},
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
		checkCacheKey, err = CheckRequestCacheKey(&ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: modelID,
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			ContextualTuples:     tuples,
			DispatchCounter:      &atomic.Uint32{},
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
		checkCacheKey, err = CheckRequestCacheKey(&ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: modelID,
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			Context:              contextStruct,
			DispatchCounter:      &atomic.Uint32{},
		})
		require.NoError(b, err)
	}
}
