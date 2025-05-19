package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/openfga/openfga/pkg/storage/test"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestSQLiteDatastore(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "sqlite")

	uri := testDatastore.GetConnectionURI(true)
	ds, err := New(uri, sqlcommon.NewConfig())
	require.NoError(t, err)
	defer ds.Close()
	test.RunAllTests(t, ds)
}

func TestSQLiteDatastoreAfterCloseIsNotReady(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "sqlite")

	uri := testDatastore.GetConnectionURI(true)
	ds, err := New(uri, sqlcommon.NewConfig())
	require.NoError(t, err)
	ds.Close()
	status, err := ds.IsReady(context.Background())
	require.Error(t, err)
	require.False(t, status.IsReady)
}

// TestReadEnsureNoOrder asserts that the read response is not ordered by ulid.
func TestReadEnsureNoOrder(t *testing.T) {
	tests := []struct {
		name  string
		mixed bool
	}{
		{
			name:  "nextOnly",
			mixed: false,
		},
		{
			name:  "mixed",
			mixed: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDatastore := storagefixtures.RunDatastoreTestContainer(t, "sqlite")

			uri := testDatastore.GetConnectionURI(true)
			ds, err := New(uri, sqlcommon.NewConfig())
			require.NoError(t, err)
			defer ds.Close()

			ctx := context.Background()

			store := "store"
			firstTuple := tuple.NewTupleKey("doc:object_id_1", "relation", "user:user_1")
			secondTuple := tuple.NewTupleKey("doc:object_id_2", "relation", "user:user_2")
			thirdTuple := tuple.NewTupleKey("doc:object_id_3", "relation", "user:user_3")

			err = ds.write(ctx,
				store,
				[]*openfgav1.TupleKeyWithoutCondition{},
				[]*openfgav1.TupleKey{firstTuple},
				time.Now())
			require.NoError(t, err)

			// Tweak time so that ULID is smaller.
			err = ds.write(ctx,
				store,
				[]*openfgav1.TupleKeyWithoutCondition{},
				[]*openfgav1.TupleKey{secondTuple},
				time.Now().Add(time.Minute*-1))
			require.NoError(t, err)

			err = ds.write(ctx,
				store,
				[]*openfgav1.TupleKeyWithoutCondition{},
				[]*openfgav1.TupleKey{thirdTuple},
				time.Now().Add(time.Minute*-2))
			require.NoError(t, err)

			iter, err := ds.Read(
				ctx,
				store,
				tuple.NewTupleKey("doc:", "relation", ""),
				storage.ReadOptions{},
			)
			defer iter.Stop()
			require.NoError(t, err)

			// We expect that objectID1 will return first because it is inserted first.
			if tt.mixed {
				curTuple, err := iter.Head(ctx)
				require.NoError(t, err)
				require.Equal(t, firstTuple, curTuple.GetKey())

				// calling head should not move the item
				curTuple, err = iter.Head(ctx)
				require.NoError(t, err)
				require.Equal(t, firstTuple, curTuple.GetKey())
			}

			curTuple, err := iter.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, firstTuple, curTuple.GetKey())

			curTuple, err = iter.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, secondTuple, curTuple.GetKey())

			if tt.mixed {
				curTuple, err = iter.Head(ctx)
				require.NoError(t, err)
				require.Equal(t, thirdTuple, curTuple.GetKey())
			}

			curTuple, err = iter.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, thirdTuple, curTuple.GetKey())

			if tt.mixed {
				_, err = iter.Head(ctx)
				require.ErrorIs(t, err, storage.ErrIteratorDone)
			}

			_, err = iter.Next(ctx)
			require.ErrorIs(t, err, storage.ErrIteratorDone)
		})
	}
}

// TestReadPageEnsureNoOrder asserts that the read page is ordered by ulid.
func TestReadPageEnsureOrder(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "sqlite")

	uri := testDatastore.GetConnectionURI(true)
	ds, err := New(uri, sqlcommon.NewConfig())
	require.NoError(t, err)
	defer ds.Close()

	ctx := context.Background()

	store := "store"
	firstTuple := tuple.NewTupleKey("doc:object_id_1", "relation", "user:user_1")
	secondTuple := tuple.NewTupleKey("doc:object_id_2", "relation", "user:user_2")

	err = ds.write(ctx,
		store,
		[]*openfgav1.TupleKeyWithoutCondition{},
		[]*openfgav1.TupleKey{firstTuple},
		time.Now())
	require.NoError(t, err)

	// Tweak time so that ULID is smaller.
	err = ds.write(ctx,
		store,
		[]*openfgav1.TupleKeyWithoutCondition{},
		[]*openfgav1.TupleKey{secondTuple},
		time.Now().Add(time.Minute*-1))
	require.NoError(t, err)

	opts := storage.ReadPageOptions{
		Pagination: storage.NewPaginationOptions(storage.DefaultPageSize, ""),
	}
	tuples, _, err := ds.ReadPage(ctx,
		store,
		tuple.NewTupleKey("doc:", "relation", ""),
		opts)
	require.NoError(t, err)

	require.Len(t, tuples, 2)
	// We expect that objectID2 will return first because it has a smaller ulid.
	require.Equal(t, secondTuple, tuples[0].GetKey())
	require.Equal(t, firstTuple, tuples[1].GetKey())
}
