package test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStore(t *testing.T, dbTester DatastoreTester[storage.OpenFGADatastore]) {

	require := require.New(t)
	ctx := context.Background()

	datastore, err := dbTester.New()
	require.NoError(err)

	// Create some stores
	numStores := 10
	var stores []*openfgapb.Store
	for i := 0; i < numStores; i++ {
		id, err := id.NewString()
		if err != nil {
			t.Fatal(err)
		}

		store := &openfgapb.Store{
			Id:        id,
			Name:      testutils.CreateRandomString(10),
			CreatedAt: timestamppb.New(time.Now()),
		}

		if _, err := datastore.CreateStore(ctx, store); err != nil {
			t.Fatal(err)
		}

		stores = append(stores, store)
	}

	t.Run("inserting store in twice fails", func(t *testing.T) {
		if _, err := datastore.CreateStore(ctx, stores[0]); !errors.Is(err, storage.ErrCollision) {
			t.Fatalf("got '%v', expected '%v'", err, storage.ErrCollision)
		}
	})

	t.Run("list stores succeeds", func(t *testing.T) {
		gotStores, ct, err := datastore.ListStores(ctx, storage.PaginationOptions{PageSize: 1})
		if err != nil {
			t.Fatal(err)
		}

		if len(gotStores) != 1 {
			t.Fatalf("expected one store, got %d", len(gotStores))
		}
		if len(ct) == 0 {
			t.Fatal("expected a continuation token but did not get one")
		}

		_, ct, err = datastore.ListStores(ctx, storage.PaginationOptions{PageSize: 100, From: string(ct)})
		if err != nil {
			t.Fatal(err)
		}

		// This will fail if there are actually over 101 stores in the DB at the time of running
		if len(ct) != 0 {
			t.Fatalf("did not expect a continuation token but got: %s", string(ct))
		}
	})

	t.Run("get store succeeds", func(t *testing.T) {
		store := stores[0]
		gotStore, err := datastore.GetStore(ctx, store.Id)
		if err != nil {
			t.Fatal(err)
		}

		if gotStore.Id != store.Id || gotStore.Name != store.Name {
			t.Errorf("got '%v', expected '%v'", gotStore, store)
		}
	})

	t.Run("get non-existent store returns not found", func(t *testing.T) {
		_, err := datastore.GetStore(ctx, "foo")
		if !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("got '%v', expected '%v'", err, storage.ErrNotFound)
		}
	})

	t.Run("delete store succeeds", func(t *testing.T) {
		store := stores[1]
		err := datastore.DeleteStore(ctx, store.Id)
		if err != nil {
			t.Fatal(err)
		}

		// Should not be able to get the store now
		_, err = datastore.GetStore(ctx, store.Id)
		if !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("got '%v', expected '%v'", err, storage.ErrNotFound)
		}
	})

	t.Run("deleted store does not appear in list", func(t *testing.T) {
		store := stores[2]
		err := datastore.DeleteStore(ctx, store.Id)
		if err != nil {
			t.Fatal(err)
		}

		// Store id should not appear in the list of store ids
		gotStores, _, err := datastore.ListStores(ctx, storage.PaginationOptions{PageSize: storage.DefaultPageSize})
		if err != nil {
			t.Fatal(err)
		}

		for _, s := range gotStores {
			if s.Id == store.Id {
				t.Errorf("deleted store '%s' appears in ListStores", s)
			}
		}
	})
}
