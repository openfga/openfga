package test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
)

func ReadChangesTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	t.Run("read_changes_with_continuation_token", func(t *testing.T) {
		storeID := ulid.Make().String()

		tk1 := &openfgav1.TupleKey{
			Object:   tuple.BuildObject("folder", "folder1"),
			Relation: "viewer",
			User:     "bob",
		}
		tk2 := &openfgav1.TupleKey{
			Object:   tuple.BuildObject("folder", "folder2"),
			Relation: "viewer",
			User:     "bill",
		}

		err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk1, tk2})
		require.NoError(t, err)

		changes, continuationToken, err := datastore.ReadChanges(ctx, storeID, "", storage.PaginationOptions{PageSize: 1}, 0)
		require.NoError(t, err)
		require.NotEmpty(t, continuationToken)

		expectedChanges := []*openfgav1.TupleChange{
			{
				TupleKey:  tk1,
				Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
			},
		}

		if diff := cmp.Diff(expectedChanges, changes, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}

		changes, continuationToken, err = datastore.ReadChanges(ctx, storeID, "", storage.PaginationOptions{
			PageSize: 2,
			From:     string(continuationToken),
		},
			0,
		)
		require.NoError(t, err)
		require.NotEmpty(t, continuationToken)

		expectedChanges = []*openfgav1.TupleChange{
			{
				TupleKey:  tk2,
				Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
			},
		}
		if diff := cmp.Diff(expectedChanges, changes, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("read_changes_with_no_changes_should_return_not_found", func(t *testing.T) {
		storeID := ulid.Make().String()

		_, _, err := datastore.ReadChanges(ctx, storeID, "", storage.PaginationOptions{PageSize: storage.DefaultPageSize}, 0)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("read_changes_with_horizon_offset_should_return_not_found_(no_changes)", func(t *testing.T) {
		storeID := ulid.Make().String()

		tk1 := &openfgav1.TupleKey{
			Object:   tuple.BuildObject("folder", "folder1"),
			Relation: "viewer",
			User:     "bob",
		}
		tk2 := &openfgav1.TupleKey{
			Object:   tuple.BuildObject("folder", "folder2"),
			Relation: "viewer",
			User:     "bill",
		}

		err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk1, tk2})
		require.NoError(t, err)

		_, _, err = datastore.ReadChanges(ctx, storeID, "", storage.PaginationOptions{PageSize: storage.DefaultPageSize}, 1*time.Minute)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("read_changes_with_non-empty_object_type_should_only_read_that_object_type", func(t *testing.T) {
		storeID := ulid.Make().String()

		tk1 := &openfgav1.TupleKey{
			Object:   tuple.BuildObject("folder", "1"),
			Relation: "viewer",
			User:     "bob",
		}
		tk2 := &openfgav1.TupleKey{
			Object:   tuple.BuildObject("document", "1"),
			Relation: "viewer",
			User:     "bill",
		}

		err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk1, tk2})
		require.NoError(t, err)

		changes, continuationToken, err := datastore.ReadChanges(ctx, storeID, "folder", storage.PaginationOptions{PageSize: storage.DefaultPageSize}, 0)
		require.NoError(t, err)
		require.NotEmpty(t, continuationToken)

		expectedChanges := []*openfgav1.TupleChange{
			{
				TupleKey:  tk1,
				Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
			},
		}
		if diff := cmp.Diff(expectedChanges, changes, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})
}

func TupleWritingAndReadingTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	t.Run("deletes_would_succeed_and_write_would_fail,_fails_and_introduces_no_changes", func(t *testing.T) {
		storeID := ulid.Make().String()
		tks := []*openfgav1.TupleKey{
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
		expectedError := storage.InvalidWriteInputError(tks[2], openfgav1.TupleOperation_TUPLE_OPERATION_WRITE)

		// Write tks
		err := datastore.Write(ctx, storeID, nil, tks)
		require.NoError(t, err)

		// Try to delete tks[0,1], and at the same time write tks[2]. It should fail with expectedError.
		err = datastore.Write(ctx, storeID, []*openfgav1.TupleKey{tks[0], tks[1]}, []*openfgav1.TupleKey{tks[2]})
		require.EqualError(t, err, expectedError.Error())

		tuples, _, err := datastore.ReadPage(ctx, storeID, nil, storage.PaginationOptions{PageSize: 50})
		require.NoError(t, err)
		require.Equal(t, len(tks), len(tuples))
	})

	t.Run("delete_fails_if_the_tuple_does_not_exist", func(t *testing.T) {
		storeID := ulid.Make().String()
		tk := &openfgav1.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}

		err := datastore.Write(ctx, storeID, []*openfgav1.TupleKey{tk}, nil)
		require.ErrorContains(t, err, "cannot delete a tuple which does not exist")
	})

	t.Run("deleting_a_tuple_which_exists_succeeds", func(t *testing.T) {
		storeID := ulid.Make().String()
		tk := &openfgav1.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}

		// Write
		err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk})
		require.NoError(t, err)

		// Then delete
		err = datastore.Write(ctx, storeID, []*openfgav1.TupleKey{tk}, nil)
		require.NoError(t, err)

		// Ensure it is not there
		_, err = datastore.ReadUserTuple(ctx, storeID, tk)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("inserting_a_tuple_twice_fails", func(t *testing.T) {
		storeID := ulid.Make().String()
		tk := &openfgav1.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}
		expectedError := storage.InvalidWriteInputError(tk, openfgav1.TupleOperation_TUPLE_OPERATION_WRITE)

		// First write should succeed.
		err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk})
		require.NoError(t, err)

		// Second write of the same tuple should fail.
		err = datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk})
		require.EqualError(t, err, expectedError.Error())
	})

	t.Run("reading_a_tuple_that_exists_succeeds", func(t *testing.T) {
		storeID := ulid.Make().String()
		tuple1 := tuple.NewTupleKey("doc:readme", "owner", "user:jon")
		tuple2 := tuple.NewTupleKey("doc:readme", "viewer", "doc:other#viewer")
		tuple3 := tuple.NewTupleKey("doc:readme", "viewer", "user:*")

		err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tuple1, tuple2, tuple3})
		require.NoError(t, err)

		gotTuple, err := datastore.ReadUserTuple(ctx, storeID, tuple1)
		require.NoError(t, err)

		if diff := cmp.Diff(tuple1, gotTuple.Key, cmpOpts...); diff != "" {
			require.FailNowf(t, "mismatch (-want +got):\n%s", diff)
		}

		gotTuple, err = datastore.ReadUserTuple(ctx, storeID, tuple2)
		require.NoError(t, err)

		if diff := cmp.Diff(tuple2, gotTuple.Key, cmpOpts...); diff != "" {
			require.FailNowf(t, "mismatch (-want +got):\n%s", diff)
		}

		gotTuple, err = datastore.ReadUserTuple(ctx, storeID, tuple3)
		require.NoError(t, err)

		if diff := cmp.Diff(tuple3, gotTuple.Key, cmpOpts...); diff != "" {
			require.FailNowf(t, "mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("reading_a_tuple_that_does_not_exist_returns_not_found", func(t *testing.T) {
		storeID := ulid.Make().String()
		tk := &openfgav1.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}

		_, err := datastore.ReadUserTuple(ctx, storeID, tk)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("reading_userset_tuples_that_exists_succeeds", func(t *testing.T) {
		storeID := ulid.Make().String()
		tks := []*openfgav1.TupleKey{
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
				Relation: "owner",
				User:     "user:*",
			},
			{
				Object:   "doc:readme",
				Relation: "viewer",
				User:     "org:openfgapb#viewer",
			},
		}

		err := datastore.Write(ctx, storeID, nil, tks)
		require.NoError(t, err)

		gotTuples, err := datastore.ReadUsersetTuples(ctx, storeID, storage.ReadUsersetTuplesFilter{
			Object:   "doc:readme",
			Relation: "owner",
		})
		require.NoError(t, err)

		iter := storage.NewTupleKeyIteratorFromTupleIterator(gotTuples)
		defer iter.Stop()

		var gotTupleKeys []*openfgav1.TupleKey
		for {
			tk, err := iter.Next()
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}

				require.Fail(t, "unexpected error encountered")
			}

			gotTupleKeys = append(gotTupleKeys, tk)
		}

		// Then the iterator should run out
		_, err = gotTuples.Next()
		require.ErrorIs(t, err, storage.ErrIteratorDone)

		require.Len(t, gotTupleKeys, 3)

		if diff := cmp.Diff(tks[:3], gotTupleKeys, cmpOpts...); diff != "" {
			require.FailNowf(t, "mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("reading_userset_tuples_that_don't_exist_should_an_empty_iterator", func(t *testing.T) {
		storeID := ulid.Make().String()

		gotTuples, err := datastore.ReadUsersetTuples(ctx, storeID, storage.ReadUsersetTuplesFilter{Object: "doc:readme", Relation: "owner"})
		require.NoError(t, err)
		defer gotTuples.Stop()

		_, err = gotTuples.Next()
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("reading_userset_tuples_with_filter_made_of_direct_relation_reference", func(t *testing.T) {
		storeID := ulid.Make().String()
		tks := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:*"),
			tuple.NewTupleKey("document:1", "viewer", "users:*"),
			tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			tuple.NewTupleKey("document:1", "viewer", "grouping:eng#member"),
		}

		err := datastore.Write(ctx, storeID, nil, tks)
		require.NoError(t, err)

		gotTuples, err := datastore.ReadUsersetTuples(ctx, storeID, storage.ReadUsersetTuplesFilter{
			Object:   "document:1",
			Relation: "viewer",
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
				typesystem.DirectRelationReference("group", "member"),
			},
		})
		require.NoError(t, err)

		iter := storage.NewTupleKeyIteratorFromTupleIterator(gotTuples)
		defer iter.Stop()

		gotTk, err := iter.Next()
		require.NoError(t, err)

		expected := tuple.NewTupleKey("document:1", "viewer", "group:eng#member")
		if diff := cmp.Diff(expected, gotTk, cmpOpts...); diff != "" {
			require.FailNowf(t, "mismatch (-want +got):\n%s", diff)
		}

		_, err = iter.Next()
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("reading_userset_tuples_with_filter_made_of_direct_relation_references", func(t *testing.T) {
		storeID := ulid.Make().String()
		tks := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:*"),
			tuple.NewTupleKey("document:1", "viewer", "users:*"),
			tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			tuple.NewTupleKey("document:1", "viewer", "grouping:eng#member"),
		}

		err := datastore.Write(ctx, storeID, nil, tks)
		require.NoError(t, err)

		gotTuples, err := datastore.ReadUsersetTuples(ctx, storeID, storage.ReadUsersetTuplesFilter{
			Object:   "document:1",
			Relation: "viewer",
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
				typesystem.DirectRelationReference("group", "member"),
				typesystem.DirectRelationReference("grouping", "member"),
			},
		})
		require.NoError(t, err)

		iter := storage.NewTupleKeyIteratorFromTupleIterator(gotTuples)
		defer iter.Stop()

		gotOne, err := iter.Next()
		require.NoError(t, err)
		gotTwo, err := iter.Next()
		require.NoError(t, err)

		expected := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			tuple.NewTupleKey("document:1", "viewer", "grouping:eng#member"),
		}
		if diff := cmp.Diff(expected, []*openfgav1.TupleKey{gotOne, gotTwo}, cmpOpts...); diff != "" {
			require.FailNowf(t, "mismatch (-want +got):\n%s", diff)
		}

		_, err = iter.Next()
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("reading_userset_tuples_with_filter_made_of_wildcard_relation_reference", func(t *testing.T) {
		storeID := ulid.Make().String()
		tks := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:*"),
			tuple.NewTupleKey("document:1", "viewer", "users:*"),
			tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			tuple.NewTupleKey("document:1", "viewer", "grouping:eng#member"),
		}

		err := datastore.Write(ctx, storeID, nil, tks)
		require.NoError(t, err)

		gotTuples, err := datastore.ReadUsersetTuples(ctx, storeID, storage.ReadUsersetTuplesFilter{
			Object:   "document:1",
			Relation: "viewer",
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
				typesystem.WildcardRelationReference("user"),
			},
		})
		require.NoError(t, err)

		iter := storage.NewTupleKeyIteratorFromTupleIterator(gotTuples)
		defer iter.Stop()

		got, err := iter.Next()
		require.NoError(t, err)

		expected := tuple.NewTupleKey("document:1", "viewer", "user:*")
		if diff := cmp.Diff(expected, got, cmpOpts...); diff != "" {
			require.FailNowf(t, "mismatch (-want +got):\n%s", diff)
		}

		_, err = iter.Next()
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("reading_userset_tuples_with_filter_made_of_mix_references", func(t *testing.T) {
		storeID := ulid.Make().String()
		tks := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:*"),
			tuple.NewTupleKey("document:1", "viewer", "users:*"),
			tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			tuple.NewTupleKey("document:1", "viewer", "grouping:eng#member"),
		}

		err := datastore.Write(ctx, storeID, nil, tks)
		require.NoError(t, err)

		gotTuples, err := datastore.ReadUsersetTuples(ctx, storeID, storage.ReadUsersetTuplesFilter{
			Object:   "document:1",
			Relation: "viewer",
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
				typesystem.DirectRelationReference("group", "member"),
				typesystem.WildcardRelationReference("user"),
			},
		})
		require.NoError(t, err)

		iter := storage.NewTupleKeyIteratorFromTupleIterator(gotTuples)
		defer iter.Stop()

		gotOne, err := iter.Next()
		require.NoError(t, err)
		gotTwo, err := iter.Next()
		require.NoError(t, err)

		expected := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			tuple.NewTupleKey("document:1", "viewer", "user:*"),
		}
		if diff := cmp.Diff(expected, []*openfgav1.TupleKey{gotOne, gotTwo}, cmpOpts...); diff != "" {
			require.FailNowf(t, "mismatch (-want +got):\n%s", diff)
		}

		_, err = iter.Next()
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})
}

func TuplePaginationOptionsTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	storeID := ulid.Make().String()
	tk0 := &openfgav1.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}
	tk1 := &openfgav1.TupleKey{Object: "doc:readme", Relation: "viewer", User: "11"}

	err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk0, tk1})
	require.NoError(t, err)

	t.Run("readPage_pagination_works_properly", func(t *testing.T) {
		tuples0, contToken0, err := datastore.ReadPage(ctx, storeID, &openfgav1.TupleKey{Object: "doc:readme"}, storage.PaginationOptions{PageSize: 1})
		require.NoError(t, err)
		require.Len(t, tuples0, 1)
		require.NotEmpty(t, contToken0)

		if diff := cmp.Diff(tk0, tuples0[0].Key, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}

		tuples1, contToken1, err := datastore.ReadPage(ctx, storeID, &openfgav1.TupleKey{Object: "doc:readme"}, storage.PaginationOptions{PageSize: 1, From: string(contToken0)})
		require.NoError(t, err)
		require.Len(t, tuples1, 1)
		require.Empty(t, contToken1)

		if diff := cmp.Diff(tk1, tuples1[0].Key, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("reading_a_page_completely_does_not_return_a_continuation_token", func(t *testing.T) {
		tuples, contToken, err := datastore.ReadPage(ctx, storeID, &openfgav1.TupleKey{Object: "doc:readme"}, storage.PaginationOptions{PageSize: 2})
		require.NoError(t, err)
		require.Len(t, tuples, 2)
		require.Empty(t, contToken)
	})

	t.Run("reading_a_page_partially_returns_a_continuation_token", func(t *testing.T) {
		tuples, contToken, err := datastore.ReadPage(ctx, storeID, &openfgav1.TupleKey{Object: "doc:readme"}, storage.PaginationOptions{PageSize: 1})
		require.NoError(t, err)
		require.Len(t, tuples, 1)
		require.NotEmpty(t, contToken)
	})

	t.Run("ReadPaginationWorks", func(t *testing.T) {
		tuple0, contToken0, err := datastore.ReadPage(ctx, storeID, nil, storage.PaginationOptions{PageSize: 1})
		require.NoError(t, err)
		require.Len(t, tuple0, 1)
		require.NotEmpty(t, contToken0)

		if diff := cmp.Diff(tk0, tuple0[0].Key, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}

		tuple1, contToken1, err := datastore.ReadPage(ctx, storeID, nil, storage.PaginationOptions{PageSize: 1, From: string(contToken0)})
		require.NoError(t, err)
		require.Len(t, tuple1, 1)
		require.Empty(t, contToken1)

		if diff := cmp.Diff(tk1, tuple1[0].Key, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("reading_by_storeID_completely_does_not_return_a_continuation_token", func(t *testing.T) {
		tuples, contToken, err := datastore.ReadPage(ctx, storeID, nil, storage.PaginationOptions{PageSize: 2})
		require.NoError(t, err)
		require.Len(t, tuples, 2)
		require.Empty(t, contToken)
	})

	t.Run("reading_by_storeID_partially_returns_a_continuation_token", func(t *testing.T) {
		tuples, contToken, err := datastore.ReadPage(ctx, storeID, nil, storage.PaginationOptions{PageSize: 1})
		require.NoError(t, err)
		require.Len(t, tuples, 1)
		require.NotEmpty(t, contToken)
	})
}

func ReadStartingWithUserTest(t *testing.T, datastore storage.OpenFGADatastore) {
	require := require.New(t)
	ctx := context.Background()

	tuples := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:doc1", "viewer", "user:jon"),
		tuple.NewTupleKey("document:doc2", "viewer", "group:eng#member"),
		tuple.NewTupleKey("document:doc3", "editor", "user:jon"),
		tuple.NewTupleKey("folder:folder1", "viewer", "user:jon"),
	}

	t.Run("returns_results_with_two_user_filters", func(t *testing.T) {
		storeID := ulid.Make().String()

		err := datastore.Write(ctx, storeID, nil, tuples)
		require.NoError(err)

		tupleIterator, err := datastore.ReadStartingWithUser(
			ctx,
			storeID,
			storage.ReadStartingWithUserFilter{
				ObjectType: "document",
				Relation:   "viewer",
				UserFilter: []*openfgav1.ObjectRelation{
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

	t.Run("returns_no_results_if_the_input_users_do_not_match_the_tuples", func(t *testing.T) {
		storeID := ulid.Make().String()

		err := datastore.Write(ctx, storeID, nil, tuples)
		require.NoError(err)

		tupleIterator, err := datastore.ReadStartingWithUser(
			ctx,
			storeID,
			storage.ReadStartingWithUserFilter{
				ObjectType: "document",
				Relation:   "viewer",
				UserFilter: []*openfgav1.ObjectRelation{
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

	t.Run("returns_no_results_if_the_input_relation_does_not_match_any_tuples", func(t *testing.T) {
		storeID := ulid.Make().String()

		err := datastore.Write(ctx, storeID, nil, tuples)
		require.NoError(err)

		tupleIterator, err := datastore.ReadStartingWithUser(
			ctx,
			storeID,
			storage.ReadStartingWithUserFilter{
				ObjectType: "document",
				Relation:   "non-existing",
				UserFilter: []*openfgav1.ObjectRelation{
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

	t.Run("returns_no_results_if_the_input_object_type_does_not_match_any_tuples", func(t *testing.T) {
		storeID := ulid.Make().String()

		err := datastore.Write(ctx, storeID, nil, tuples)
		require.NoError(err)

		tupleIterator, err := datastore.ReadStartingWithUser(
			ctx,
			storeID,
			storage.ReadStartingWithUserFilter{
				ObjectType: "nonexisting",
				Relation:   "viewer",
				UserFilter: []*openfgav1.ObjectRelation{
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

func ReadTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	tuples := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "reader", "user:anne"),
		tuple.NewTupleKey("document:1", "reader", "user:bob"),
		tuple.NewTupleKey("document:1", "writer", "user:bob"),
	}

	storeID := ulid.Make().String()

	err := datastore.Write(ctx, storeID, nil, tuples)
	require.NoError(t, err)

	t.Run("empty_filter_returns_all_tuples", func(t *testing.T) {
		tupleIterator, err := datastore.Read(
			ctx,
			storeID,
			tuple.NewTupleKey("", "", ""),
		)
		require.NoError(t, err)
		defer tupleIterator.Stop()

		expectedTupleKeys := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "reader", "user:anne"),
			tuple.NewTupleKey("document:1", "reader", "user:bob"),
			tuple.NewTupleKey("document:1", "writer", "user:bob"),
		}

		require.ElementsMatch(t, expectedTupleKeys, getTupleKeys(tupleIterator, t))
	})

	t.Run("filter_by_user_and_relation_and_objectID", func(t *testing.T) {
		tupleIterator, err := datastore.Read(
			ctx,
			storeID,
			tuple.NewTupleKey("document:1", "reader", "user:bob"),
		)
		require.NoError(t, err)
		defer tupleIterator.Stop()

		expectedTupleKeys := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "reader", "user:bob"),
		}

		require.ElementsMatch(t, expectedTupleKeys, getTupleKeys(tupleIterator, t))
	})

	t.Run("filter_by_user_and_relation_and_objectType", func(t *testing.T) {
		tupleIterator, err := datastore.Read(
			ctx,
			storeID,
			tuple.NewTupleKey("document:", "reader", "user:bob"),
		)
		require.NoError(t, err)
		defer tupleIterator.Stop()

		expectedTupleKeys := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "reader", "user:bob"),
		}

		require.ElementsMatch(t, expectedTupleKeys, getTupleKeys(tupleIterator, t))
	})

	t.Run("filter_by_relation_and_objectID", func(t *testing.T) {
		tupleIterator, err := datastore.Read(
			ctx,
			storeID,
			tuple.NewTupleKey("document:1", "reader", ""),
		)
		require.NoError(t, err)
		defer tupleIterator.Stop()

		expectedTupleKeys := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "reader", "user:bob"),
			tuple.NewTupleKey("document:1", "reader", "user:anne"),
		}

		require.ElementsMatch(t, expectedTupleKeys, getTupleKeys(tupleIterator, t))
	})

	t.Run("filter_by_objectID", func(t *testing.T) {
		tupleIterator, err := datastore.Read(
			ctx,
			storeID,
			tuple.NewTupleKey("document:1", "", ""),
		)
		require.NoError(t, err)
		defer tupleIterator.Stop()

		expectedTupleKeys := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "reader", "user:anne"),
			tuple.NewTupleKey("document:1", "reader", "user:bob"),
			tuple.NewTupleKey("document:1", "writer", "user:bob"),
		}

		require.ElementsMatch(t, expectedTupleKeys, getTupleKeys(tupleIterator, t))
	})

	t.Run("filter_by_objectID_and_user", func(t *testing.T) {
		tupleIterator, err := datastore.Read(
			ctx,
			storeID,
			tuple.NewTupleKey("document:1", "", "user:bob"),
		)
		require.NoError(t, err)
		defer tupleIterator.Stop()

		expectedTupleKeys := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "reader", "user:bob"),
			tuple.NewTupleKey("document:1", "writer", "user:bob"),
		}

		require.ElementsMatch(t, expectedTupleKeys, getTupleKeys(tupleIterator, t))
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

func getTupleKeys(tupleIterator storage.TupleIterator, t *testing.T) []*openfgav1.TupleKey {
	t.Helper()
	var tupleKeys []*openfgav1.TupleKey
	for {
		tp, err := tupleIterator.Next()
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}

			require.Fail(t, err.Error())
		}

		tupleKeys = append(tupleKeys, tp.GetKey())
	}
	return tupleKeys
}
