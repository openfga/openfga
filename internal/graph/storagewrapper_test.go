package graph

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestReadUsersetTuples(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	mockController := gomock.NewController(t)
	mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	maxSize := int64(10)
	ttl := 5 * time.Hour
	ds := NewCachedDatastore(mockDatastore, mockCache, maxSize, ttl)

	storeID := ulid.Make().String()

	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("company:1", "viewer", "user:1"),
		tuple.NewTupleKey("company:1", "viewer", "user:2"),
		tuple.NewTupleKey("company:1", "viewer", "user:3"),
		tuple.NewTupleKey("company:1", "viewer", "user:4"),
		tuple.NewTupleKey("company:1", "viewer", "user:5"),
		tuple.NewTupleKey("license:1", "viewer", "company:1#viewer"),
	}
	tuples := []*openfgav1.Tuple{}
	for _, tk := range tks {
		tuples = append(tuples, &openfgav1.Tuple{Key: tk})
	}

	options := storage.ReadUsersetTuplesOptions{}
	filter := storage.ReadUsersetTuplesFilter{
		Object:   "document:1",
		Relation: "viewer",
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			typesystem.DirectRelationReference("company", "viewer"),
			typesystem.WildcardRelationReference("user"),
		},
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	gomock.InOrder(
		mockDatastore.EXPECT().
			Write(ctx, storeID, nil, tks).Return(nil),
		mockDatastore.EXPECT().
			ReadUsersetTuples(ctx, storeID, filter, options).
			Return(storage.NewStaticTupleIterator(tuples), nil),
		mockCache.EXPECT().Get(gomock.Any()),
		mockCache.EXPECT().Set(gomock.Any(), tuples, ttl).
			Do(func(arg0, arg1, arg2 interface{}) {
				defer wg.Done()
			}),
	)

	err := ds.Write(ctx, storeID, nil, tks)
	require.NoError(t, err)

	iter, err := ds.ReadUsersetTuples(ctx, storeID, filter, options)
	require.NoError(t, err)

	tp, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, tuples[0], tp)

	iter.Stop()
	wg.Wait()
}

func TestRead(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	mockController := gomock.NewController(t)
	mockController.Finish()

	mockCache := mocks.NewMockInMemoryCache[any](mockController)
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	maxSize := int64(10)
	ttl := 5 * time.Hour
	ds := NewCachedDatastore(mockDatastore, mockCache, maxSize, ttl)

	storeID := ulid.Make().String()

	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("company:1", "viewer", "user:1"),
		tuple.NewTupleKey("license:1", "owner", "company:1"),
		tuple.NewTupleKey("company:1", "viewer", "user:3"),
		tuple.NewTupleKey("company:1", "viewer", "user:4"),
		tuple.NewTupleKey("company:1", "viewer", "user:5"),
	}
	tuples := []*openfgav1.Tuple{}
	for _, tk := range tks {
		tuples = append(tuples, &openfgav1.Tuple{Key: tk})
	}

	tk := tuple.NewTupleKey("license:1", "owner", "")

	wg := sync.WaitGroup{}
	wg.Add(1)

	gomock.InOrder(
		mockDatastore.EXPECT().
			Write(ctx, storeID, nil, tks).Return(nil),
		mockDatastore.EXPECT().
			Read(ctx, storeID, tk, storage.ReadOptions{}).
			Return(storage.NewStaticTupleIterator(tuples), nil),
		mockCache.EXPECT().Get(gomock.Any()),
		mockCache.EXPECT().Set(gomock.Any(), tuples, ttl).
			Do(func(arg0, arg1, arg2 interface{}) {
				defer wg.Done()
			}),
	)

	err := ds.Write(ctx, storeID, nil, tks)
	require.NoError(t, err)

	iter, err := ds.Read(ctx, storeID, tk, storage.ReadOptions{})
	require.NoError(t, err)

	tp, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, tuples[0], tp)

	iter.Stop()
	wg.Wait()
}
