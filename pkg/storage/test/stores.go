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

	const sharedStoreName = "shared"

	var (
		numStores               = 10
		numStoresWithSharedName = 3
	)

	stores := make([]*openfgav1.Store, 0, numStores+numStoresWithSharedName)

	createStore := func(name string) *openfgav1.Store {
		store := &openfgav1.Store{
			Id:   ulid.MustNew(ulid.Timestamp(time.Now()), entropy).String(),
			Name: name,
		}

		resp, err := datastore.CreateStore(ctx, store)
		require.NoError(t, err)
		require.NotEmpty(t, resp.GetCreatedAt())
		require.NotEmpty(t, resp.GetUpdatedAt())
		return store
	}

	// Create some stores.
	for i := 0; i < numStores; i++ {
		stores = append(stores, createStore(testutils.CreateRandomString(10)))
	}

	// Stores that shares name are created after numStores so we have an easy way to access them.
	for i := 0; i < numStoresWithSharedName; i++ {
		stores = append(stores, createStore(sharedStoreName))
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

	t.Run("list_stores_succeeds_with_ids_filter_match", func(t *testing.T) {
		opts := storage.ListStoresOptions{
			Pagination: storage.NewPaginationOptions(1, ""),
			IDs:        []string{stores[0].GetId()},
		}
		gotStores, ct, err := datastore.ListStores(ctx, opts)
		require.NoError(t, err)
		require.Len(t, gotStores, 1)
		require.Empty(t, ct)
	})

	verifyStore := func(t *testing.T, expected, got *openfgav1.Store) {
		require.Equal(t, expected.GetId(), got.GetId())
		require.Equal(t, expected.GetName(), got.GetName())
	}

	t.Run("list_stores_succeeds_with_name_filter_match", func(t *testing.T) {
		gotStores, ct, err := datastore.ListStores(ctx, storage.ListStoresOptions{
			Pagination: storage.NewPaginationOptions(10, ""),
			Name:       stores[1].GetName(),
		})

		require.NoError(t, err)
		require.Len(t, gotStores, 1)
		require.Empty(t, ct)
		verifyStore(t, stores[1], gotStores[0])
	})

	t.Run("list_stores_with_name_filter_no_match", func(t *testing.T) {
		gotStores, ct, err := datastore.ListStores(ctx, storage.ListStoresOptions{
			Pagination: storage.NewPaginationOptions(10, ""),
			Name:       "unlikely-to-match",
		})

		require.NoError(t, err)
		require.Empty(t, gotStores)
		require.Empty(t, ct)
	})

	t.Run("list_stores_succeeds_with_name_filter_match_shared_name", func(t *testing.T) {
		// filter out stores that shares name
		gotStores, ct, err := datastore.ListStores(ctx, storage.ListStoresOptions{
			Pagination: storage.NewPaginationOptions(2, ""),
			Name:       sharedStoreName,
		})

		require.NoError(t, err)
		require.Len(t, gotStores, 2)
		require.NotEmpty(t, ct)

		verifyStore(t, stores[numStores], gotStores[0])
		verifyStore(t, stores[numStores+1], gotStores[1])

		// paginate stores that shares name
		gotStores, ct, err = datastore.ListStores(ctx, storage.ListStoresOptions{
			Pagination: storage.NewPaginationOptions(2, ct),
			Name:       sharedStoreName,
		})

		require.NoError(t, err)
		require.Len(t, gotStores, 1)
		require.Empty(t, ct)
		verifyStore(t, stores[numStores+2], gotStores[0])
	})

	t.Run("list_stores_succeeds_with_all_filters", func(t *testing.T) {
		expected1 := stores[numStores]
		expected2 := stores[numStores+2]

		gotStores, ct, err := datastore.ListStores(ctx, storage.ListStoresOptions{
			Pagination: storage.NewPaginationOptions(10, ""),
			Name:       sharedStoreName,
			IDs:        []string{expected1.GetId(), expected2.GetId()},
		})

		require.NoError(t, err)
		require.Len(t, gotStores, 2)
		require.Empty(t, ct)
		verifyStore(t, expected1, gotStores[0])
		verifyStore(t, expected2, gotStores[1])
	})

	t.Run("get_store_succeeds", func(t *testing.T) {
		store := stores[0]
		gotStore, err := datastore.GetStore(ctx, store.GetId())
		require.NoError(t, err)
		verifyStore(t, store, gotStore)
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
