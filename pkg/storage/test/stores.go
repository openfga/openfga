package test

import (
	"context"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
)

func StoreTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	entropy := ulid.DefaultEntropy()

	// Create some stores.
	numStores := 10
	var stores []*openfgav1.Store
	for i := 0; i < numStores; i++ {
		store := &openfgav1.Store{
			Id:   ulid.MustNew(ulid.Timestamp(time.Now()), entropy).String(),
			Name: testutils.CreateRandomString(10),
		}

		resp, err := datastore.CreateStore(ctx, store)
		require.NoError(t, err)
		require.NotEmpty(t, resp.GetCreatedAt())
		require.NotEmpty(t, resp.GetUpdatedAt())

		stores = append(stores, store)
	}

	t.Run("inserting_store_in_twice_fails", func(t *testing.T) {
		_, err := datastore.CreateStore(ctx, stores[0])
		require.ErrorIs(t, err, storage.ErrCollision)
	})

	t.Run("list_stores_succeeds", func(t *testing.T) {
		opts := storage.ListStoresOptions{
			Pagination: storage.NewPaginationOptions(1, ""),
		}
		gotStores, ct, err := datastore.ListStores(ctx, opts)
		require.NoError(t, err)

		require.Len(t, gotStores, 1)
		require.NotEmpty(t, ct)

		opts = storage.ListStoresOptions{
			Pagination: storage.NewPaginationOptions(100, ct),
		}
		_, ct, err = datastore.ListStores(ctx, opts)
		require.NoError(t, err)

		// This will fail if there are actually over 101 stores in the DB at the time of running.
		require.Empty(t, ct)
	})

	t.Run("list_stores_succeeds_with_filter_no_match", func(t *testing.T) {
		opts := storage.ListStoresOptions{
			Pagination: storage.NewPaginationOptions(1, ""),
			IDs:        []string{"unknown"},
		}
		gotStores, ct, err := datastore.ListStores(ctx, opts)
		require.NoError(t, err)
		require.Empty(t, gotStores)
		require.Empty(t, ct)
	})

	t.Run("list_stores_succeeds_with_filter_match", func(t *testing.T) {
		t.Skip("fix me")
		opts := storage.ListStoresOptions{
			Pagination: storage.NewPaginationOptions(1, ""),
			IDs:        []string{stores[0].GetId()},
		}
		gotStores, ct, err := datastore.ListStores(ctx, opts)
		require.NoError(t, err)
		require.Len(t, gotStores, 1)
		require.Empty(t, ct)
	})

	t.Run("get_store_succeeds", func(t *testing.T) {
		store := stores[0]
		gotStore, err := datastore.GetStore(ctx, store.GetId())
		require.NoError(t, err)
		require.Equal(t, store.GetId(), gotStore.GetId())
		require.Equal(t, store.GetName(), gotStore.GetName())
	})

	t.Run("get_non-existent_store_returns_not_found", func(t *testing.T) {
		_, err := datastore.GetStore(ctx, "foo")
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("delete_store_succeeds", func(t *testing.T) {
		store := stores[1]
		err := datastore.DeleteStore(ctx, store.GetId())
		require.NoError(t, err)

		// Should not be able to get the store now.
		_, err = datastore.GetStore(ctx, store.GetId())
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("delete_store_if_not_found_succeeds", func(t *testing.T) {
		err := datastore.DeleteStore(ctx, "unknown")
		require.NoError(t, err)
	})

	t.Run("deleted_store_does_not_appear_in_list", func(t *testing.T) {
		store := stores[2]
		err := datastore.DeleteStore(ctx, store.GetId())
		require.NoError(t, err)

		// Store id should not appear in the list of store ids.
		opts := storage.ListStoresOptions{
			Pagination: storage.NewPaginationOptions(storage.DefaultPageSize, ""),
		}
		gotStores, _, err := datastore.ListStores(ctx, opts)
		require.NoError(t, err)

		for _, s := range gotStores {
			require.NotEqual(t, store.GetId(), s.GetId())
		}
	})
}
