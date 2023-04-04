package test

import (
	"context"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func StoreTest(t *testing.T, datastore storage.OpenFGADatastore) {

	ctx := context.Background()

	// Create some stores
	numStores := 10
	var stores []*openfgapb.Store
	for i := 0; i < numStores; i++ {
		store := &openfgapb.Store{
			Id:        ulid.Make().String(),
			Name:      testutils.CreateRandomString(10),
			CreatedAt: timestamppb.New(time.Now()),
		}

		_, err := datastore.CreateStore(ctx, store)
		require.NoError(t, err)

		stores = append(stores, store)
	}

	t.Run("inserting_store_in_twice_fails", func(t *testing.T) {
		_, err := datastore.CreateStore(ctx, stores[0])
		require.ErrorIs(t, err, storage.ErrCollision)
	})

	t.Run("list_stores_succeeds", func(t *testing.T) {
		gotStores, ct, err := datastore.ListStores(ctx, storage.PaginationOptions{PageSize: 1})
		require.NoError(t, err)

		require.Equal(t, 1, len(gotStores))
		require.NotEmpty(t, len(ct))

		_, ct, err = datastore.ListStores(ctx, storage.PaginationOptions{PageSize: 100, From: string(ct)})
		require.NoError(t, err)

		// This will fail if there are actually over 101 stores in the DB at the time of running
		require.Zero(t, len(ct))
	})

	t.Run("get_store_succeeds", func(t *testing.T) {
		store := stores[0]
		gotStore, err := datastore.GetStore(ctx, store.Id)
		require.NoError(t, err)
		require.Equal(t, store.Id, gotStore.Id)
		require.Equal(t, store.Name, gotStore.Name)
	})

	t.Run("get_non-existent_store_returns_not_found", func(t *testing.T) {
		_, err := datastore.GetStore(ctx, "foo")
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("delete_store_succeeds", func(t *testing.T) {
		store := stores[1]
		err := datastore.DeleteStore(ctx, store.Id)
		require.NoError(t, err)

		// Should not be able to get the store now
		_, err = datastore.GetStore(ctx, store.Id)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("deleted_store_does_not_appear_in_list", func(t *testing.T) {
		store := stores[2]
		err := datastore.DeleteStore(ctx, store.Id)
		require.NoError(t, err)

		// Store id should not appear in the list of store ids
		gotStores, _, err := datastore.ListStores(ctx, storage.PaginationOptions{PageSize: storage.DefaultPageSize})
		require.NoError(t, err)

		for _, s := range gotStores {
			require.NotEqual(t, store.Id, s.Id)
		}
	})
}
