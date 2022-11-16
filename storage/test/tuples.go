package test

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func ReadChangesTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	t.Run("read changes with continuation token", func(t *testing.T) {
		storeID := ulid.Make().String()

		tk1 := &openfgapb.TupleKey{
			Object:   tuple.BuildObject("folder", "folder1"),
			Relation: "viewer",
			User:     "bob",
		}
		tk2 := &openfgapb.TupleKey{
			Object:   tuple.BuildObject("folder", "folder2"),
			Relation: "viewer",
			User:     "bill",
		}

		err := datastore.Write(ctx, storeID, nil, []*openfgapb.TupleKey{tk1, tk2})
		require.NoError(t, err)

		changes, continuationToken, err := datastore.ReadChanges(ctx, storeID, "", storage.PaginationOptions{PageSize: 1}, 0)
		require.NoError(t, err)
		require.NotEmpty(t, continuationToken)

		expectedChanges := []*openfgapb.TupleChange{
			{
				TupleKey:  tk1,
				Operation: openfgapb.TupleOperation_TUPLE_OPERATION_WRITE,
			},
		}

		if diff := cmp.Diff(expectedChanges, changes, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-got +want):\n%s", diff)
		}

		changes, continuationToken, err = datastore.ReadChanges(ctx, storeID, "", storage.PaginationOptions{
			PageSize: 2,
			From:     string(continuationToken),
		},
			0,
		)
		require.NoError(t, err)
		require.NotEmpty(t, continuationToken)

		expectedChanges = []*openfgapb.TupleChange{
			{
				TupleKey:  tk2,
				Operation: openfgapb.TupleOperation_TUPLE_OPERATION_WRITE,
			},
		}
		if diff := cmp.Diff(expectedChanges, changes, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-got +want):\n%s", diff)
		}
	})

	t.Run("read changes with no changes should return not found", func(t *testing.T) {
		storeID := ulid.Make().String()

		_, _, err := datastore.ReadChanges(ctx, storeID, "", storage.PaginationOptions{PageSize: storage.DefaultPageSize}, 0)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("read changes with horizon offset should return not found (no changes)", func(t *testing.T) {
		storeID := ulid.Make().String()

		tk1 := &openfgapb.TupleKey{
			Object:   tuple.BuildObject("folder", "folder1"),
			Relation: "viewer",
			User:     "bob",
		}
		tk2 := &openfgapb.TupleKey{
			Object:   tuple.BuildObject("folder", "folder2"),
			Relation: "viewer",
			User:     "bill",
		}

		err := datastore.Write(ctx, storeID, nil, []*openfgapb.TupleKey{tk1, tk2})
		require.NoError(t, err)

		_, _, err = datastore.ReadChanges(ctx, storeID, "", storage.PaginationOptions{PageSize: storage.DefaultPageSize}, 1*time.Minute)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("read changes with non-empty object type should only read that object type", func(t *testing.T) {
		storeID := ulid.Make().String()

		tk1 := &openfgapb.TupleKey{
			Object:   tuple.BuildObject("folder", "1"),
			Relation: "viewer",
			User:     "bob",
		}
		tk2 := &openfgapb.TupleKey{
			Object:   tuple.BuildObject("document", "1"),
			Relation: "viewer",
			User:     "bill",
		}

		err := datastore.Write(ctx, storeID, nil, []*openfgapb.TupleKey{tk1, tk2})
		require.NoError(t, err)

		changes, continuationToken, err := datastore.ReadChanges(ctx, storeID, "folder", storage.PaginationOptions{PageSize: storage.DefaultPageSize}, 0)
		require.NoError(t, err)
		require.NotEmpty(t, continuationToken)

		expectedChanges := []*openfgapb.TupleChange{
			{
				TupleKey:  tk1,
				Operation: openfgapb.TupleOperation_TUPLE_OPERATION_WRITE,
			},
		}
		if diff := cmp.Diff(expectedChanges, changes, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-got +want):\n%s", diff)
		}
	})
}

func TupleWritingAndReadingTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	t.Run("deletes would succeed and write would fail, fails and introduces no changes", func(t *testing.T) {
		storeID := ulid.Make().String()
		tks := []*openfgapb.TupleKey{
			{
				Object:   "doc:readme",
				Relation: "owner",
				User:     "org:openfga#member",
			},
			{
				Object:   "doc:readme",
				Relation: "owner",
				User:     "domain:iam#member",
			},
			{
				Object:   "doc:readme",
				Relation: "viewer",
				User:     "org:openfgapb#viewer",
			},
		}
		expectedError := storage.InvalidWriteInputError(tks[2], openfgapb.TupleOperation_TUPLE_OPERATION_WRITE)

		// Write tks
		err := datastore.Write(ctx, storeID, nil, tks)
		require.NoError(t, err)

		// Try to delete tks[0,1], and at the same time write tks[2]. It should fail with expectedError.
		err = datastore.Write(ctx, storeID, []*openfgapb.TupleKey{tks[0], tks[1]}, []*openfgapb.TupleKey{tks[2]})
		require.EqualError(t, err, expectedError.Error())

		tuples, _, err := datastore.ReadByStore(ctx, storeID, storage.PaginationOptions{PageSize: 50})
		require.NoError(t, err)

		require.Equal(t, len(tks), len(tuples))
	})

	t.Run("delete fails if the tuple does not exist", func(t *testing.T) {
		storeID := ulid.Make().String()
		tk := &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}
		expectedError := storage.InvalidWriteInputError(tk, openfgapb.TupleOperation_TUPLE_OPERATION_DELETE)

		err := datastore.Write(ctx, storeID, []*openfgapb.TupleKey{tk}, nil)
		require.EqualError(t, err, expectedError.Error())
	})

	t.Run("deleting a tuple which exists succeeds", func(t *testing.T) {
		storeID := ulid.Make().String()
		tk := &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}

		// Write
		err := datastore.Write(ctx, storeID, nil, []*openfgapb.TupleKey{tk})
		require.NoError(t, err)

		// Then delete
		err = datastore.Write(ctx, storeID, []*openfgapb.TupleKey{tk}, nil)
		require.NoError(t, err)

		// Ensure it is not there
		_, err = datastore.ReadUserTuple(ctx, storeID, tk)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("inserting a tuple twice fails", func(t *testing.T) {
		storeID := ulid.Make().String()
		tk := &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}
		expectedError := storage.InvalidWriteInputError(tk, openfgapb.TupleOperation_TUPLE_OPERATION_WRITE)

		// First write should succeed.
		err := datastore.Write(ctx, storeID, nil, []*openfgapb.TupleKey{tk})
		require.NoError(t, err)

		// Second write of the same tuple should fail.
		err = datastore.Write(ctx, storeID, nil, []*openfgapb.TupleKey{tk})
		require.EqualError(t, err, expectedError.Error())
	})

	t.Run("reading a tuple that exists succeeds", func(t *testing.T) {
		storeID := ulid.Make().String()
		tuple1 := &openfgapb.Tuple{Key: &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}}
		tuple2 := &openfgapb.Tuple{Key: &openfgapb.TupleKey{Object: "doc:readme", Relation: "viewer", User: "doc:other#viewer"}}

		err := datastore.Write(ctx, storeID, nil, []*openfgapb.TupleKey{tuple1.Key, tuple2.Key})
		require.NoError(t, err)

		gotTuple, err := datastore.ReadUserTuple(ctx, storeID, tuple1.Key)
		require.NoError(t, err)

		if diff := cmp.Diff(gotTuple, tuple1, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-got +want):\n%s", diff)
		}

		gotTuple, err = datastore.ReadUserTuple(ctx, storeID, tuple2.Key)
		require.NoError(t, err)

		if diff := cmp.Diff(gotTuple, tuple2, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-got +want):\n%s", diff)
		}
	})

	t.Run("reading a tuple that does not exist returns not found", func(t *testing.T) {
		storeID := ulid.Make().String()
		tk := &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}

		_, err := datastore.ReadUserTuple(ctx, storeID, tk)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("reading userset tuples that exists succeeds", func(t *testing.T) {
		storeID := ulid.Make().String()
		tks := []*openfgapb.TupleKey{
			{
				Object:   "doc:readme",
				Relation: "owner",
				User:     "org:openfga#member",
			},
			{
				Object:   "doc:readme",
				Relation: "owner",
				User:     "domain:iam#member",
			},
			{
				Object:   "doc:readme",
				Relation: "viewer",
				User:     "org:openfgapb#viewer",
			},
		}

		err := datastore.Write(ctx, storeID, nil, tks)
		require.NoError(t, err)

		gotTuples, err := datastore.ReadUsersetTuples(ctx, storeID, &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner"})
		require.NoError(t, err)
		defer gotTuples.Stop()

		var gotTupleKeys []*openfgapb.TupleKey
		// We should find the first two tupleKeys
		for i := 0; i < 2; i++ {
			gotTuple, err := gotTuples.Next()
			if err != nil {
				t.Fatal(err)
			}

			gotTupleKeys = append(gotTupleKeys, gotTuple.Key)
		}

		// Then the iterator should run out
		_, err = gotTuples.Next()
		require.ErrorIs(t, err, storage.ErrIteratorDone)

		if diff := cmp.Diff(gotTupleKeys, tks[:2], cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-got +want):\n%s", diff)
		}
	})

	t.Run("reading userset tuples that don't exist should an empty iterator", func(t *testing.T) {
		storeID := ulid.Make().String()

		gotTuples, err := datastore.ReadUsersetTuples(ctx, storeID, &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner"})
		require.NoError(t, err)
		defer gotTuples.Stop()

		_, err = gotTuples.Next()
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})
}

func TuplePaginationOptionsTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	storeID := ulid.Make().String()
	tk0 := &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}
	tk1 := &openfgapb.TupleKey{Object: "doc:readme", Relation: "viewer", User: "11"}

	err := datastore.Write(ctx, storeID, nil, []*openfgapb.TupleKey{tk0, tk1})
	require.NoError(t, err)

	t.Run("readPage pagination works properly", func(t *testing.T) {
		tuples0, contToken0, err := datastore.ReadPage(ctx, storeID, &openfgapb.TupleKey{Object: "doc:readme"}, storage.PaginationOptions{PageSize: 1})
		require.NoError(t, err)
		require.Len(t, tuples0, 1)
		require.NotEmpty(t, contToken0)

		if diff := cmp.Diff(tuples0[0].Key, tk0, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-got +want):\n%s", diff)
		}

		tuples1, contToken1, err := datastore.ReadPage(ctx, storeID, &openfgapb.TupleKey{Object: "doc:readme"}, storage.PaginationOptions{PageSize: 1, From: string(contToken0)})
		require.NoError(t, err)
		require.Len(t, tuples1, 1)
		require.Empty(t, contToken1)

		if diff := cmp.Diff(tuples1[0].Key, tk1, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-got +want):\n%s", diff)
		}
	})

	t.Run("reading a page completely does not return a continuation token", func(t *testing.T) {
		tuples, contToken, err := datastore.ReadPage(ctx, storeID, &openfgapb.TupleKey{Object: "doc:readme"}, storage.PaginationOptions{PageSize: 2})
		require.NoError(t, err)
		require.Len(t, tuples, 2)
		require.Empty(t, contToken)
	})

	t.Run("reading a page partially returns a continuation token", func(t *testing.T) {
		tuples, contToken, err := datastore.ReadPage(ctx, storeID, &openfgapb.TupleKey{Object: "doc:readme"}, storage.PaginationOptions{PageSize: 1})
		require.NoError(t, err)
		require.Len(t, tuples, 1)
		require.NotEmpty(t, contToken)
	})

	t.Run("readByStore pagination works properly", func(t *testing.T) {
		tuple0, contToken0, err := datastore.ReadByStore(ctx, storeID, storage.PaginationOptions{PageSize: 1})
		require.NoError(t, err)
		require.Len(t, tuple0, 1)
		require.NotEmpty(t, contToken0)

		if diff := cmp.Diff(tuple0[0].Key, tk0, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-got +want):\n%s", diff)
		}

		tuple1, contToken1, err := datastore.ReadByStore(ctx, storeID, storage.PaginationOptions{PageSize: 1, From: string(contToken0)})
		require.NoError(t, err)
		require.Len(t, tuple1, 1)
		require.Empty(t, contToken1)

		if diff := cmp.Diff(tuple1[0].Key, tk1, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-got +want):\n%s", diff)
		}
	})

	t.Run("reading by storeID completely does not return a continuation token", func(t *testing.T) {
		tuples, contToken, err := datastore.ReadByStore(ctx, storeID, storage.PaginationOptions{PageSize: 2})
		require.NoError(t, err)
		require.Len(t, tuples, 2)
		require.Empty(t, contToken)
	})

	t.Run("reading by storeID partially returns a continuation token", func(t *testing.T) {
		tuples, contToken, err := datastore.ReadByStore(ctx, storeID, storage.PaginationOptions{PageSize: 1})
		require.NoError(t, err)
		require.Len(t, tuples, 1)
		require.NotEmpty(t, contToken)
	})
}

func ListObjectsByTypeTest(t *testing.T, ds storage.OpenFGADatastore) {
	expected := []string{"document:doc1", "document:doc2"}
	storeID := ulid.Make().String()

	err := ds.Write(context.Background(), storeID, nil, []*openfgapb.TupleKey{
		tuple.NewTupleKey("document:doc1", "viewer", "jon"),
		tuple.NewTupleKey("document:doc1", "viewer", "elbuo"),
		tuple.NewTupleKey("document:doc2", "editor", "maria"),
	})
	require.NoError(t, err)

	iter, err := ds.ListObjectsByType(context.Background(), storeID, "document")
	require.NoError(t, err)

	var actual []string
	for {
		obj, err := iter.Next()
		if err != nil {
			if err == storage.ErrIteratorDone {
				break
			}
		}

		actual = append(actual, tuple.ObjectKey(obj))
	}

	require.Equal(t, expected, actual)
}

func ReadStartingWithUserTest(t *testing.T, datastore storage.OpenFGADatastore) {
	require := require.New(t)
	ctx := context.Background()

	tuples := []*openfgapb.TupleKey{
		tuple.NewTupleKey("document:doc1", "viewer", "user:jon"),
		tuple.NewTupleKey("document:doc2", "viewer", "group:eng#member"),
		tuple.NewTupleKey("document:doc3", "editor", "user:jon"),
		tuple.NewTupleKey("folder:folder1", "viewer", "user:jon"),
	}

	t.Run("returns results with two user filters", func(t *testing.T) {
		storeID := ulid.Make().String()

		err := datastore.Write(ctx, storeID, nil, tuples)
		require.NoError(err)

		tupleIterator, err := datastore.ReadStartingWithUser(
			ctx,
			storeID,
			storage.ReadStartingWithUserFilter{
				ObjectType: "document",
				Relation:   "viewer",
				UserFilter: []*openfgapb.ObjectRelation{
					{
						Object: "user:jon",
					},
					{
						Object:   "group:eng",
						Relation: "member",
					},
				},
			},
		)
		require.NoError(err)

		objects := getObjects(tupleIterator, require)

		require.ElementsMatch([]string{"document:doc1", "document:doc2"}, objects)
	})

	t.Run("returns no results if the input users do not match the tuples", func(t *testing.T) {
		storeID := ulid.Make().String()

		err := datastore.Write(ctx, storeID, nil, tuples)
		require.NoError(err)

		tupleIterator, err := datastore.ReadStartingWithUser(
			ctx,
			storeID,
			storage.ReadStartingWithUserFilter{
				ObjectType: "document",
				Relation:   "viewer",
				UserFilter: []*openfgapb.ObjectRelation{
					{
						Object: "user:maria",
					},
				},
			},
		)
		require.NoError(err)

		objects := getObjects(tupleIterator, require)

		require.Empty(objects)
	})

	t.Run("returns no results if the input relation does not match any tuples", func(t *testing.T) {
		storeID := ulid.Make().String()

		err := datastore.Write(ctx, storeID, nil, tuples)
		require.NoError(err)

		tupleIterator, err := datastore.ReadStartingWithUser(
			ctx,
			storeID,
			storage.ReadStartingWithUserFilter{
				ObjectType: "document",
				Relation:   "non-existing",
				UserFilter: []*openfgapb.ObjectRelation{
					{
						Object: "user:jon",
					},
				},
			},
		)
		require.NoError(err)

		objects := getObjects(tupleIterator, require)

		require.Empty(objects)
	})

	t.Run("returns no results if the input object type does not match any tuples", func(t *testing.T) {
		storeID := ulid.Make().String()

		err := datastore.Write(ctx, storeID, nil, tuples)
		require.NoError(err)

		tupleIterator, err := datastore.ReadStartingWithUser(
			ctx,
			storeID,
			storage.ReadStartingWithUserFilter{
				ObjectType: "nonexisting",
				Relation:   "viewer",
				UserFilter: []*openfgapb.ObjectRelation{
					{
						Object: "user:jon",
					},
				},
			},
		)
		require.NoError(err)

		objects := getObjects(tupleIterator, require)

		require.Empty(objects)
	})
}

func getObjects(tupleIterator storage.TupleIterator, require *require.Assertions) []string {
	var objects []string
	for {
		tp, err := tupleIterator.Next()
		if err != nil {
			if err == storage.ErrIteratorDone {
				break
			}

			require.Fail(err.Error())
		}

		objects = append(objects, tp.GetKey().GetObject())
	}
	return objects
}
