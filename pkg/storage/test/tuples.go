package test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
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

	t.Run("read_changes_returns_deterministic_ordering_and_no_duplicates", func(t *testing.T) {
		storeID := ulid.Make().String()

		for i := 0; i < 100; i++ {
			object := fmt.Sprintf("document:%d", i)
			tuple := []*openfgav1.TupleKey{tuple.NewTupleKey(object, "viewer", "user:jon")}
			err := datastore.Write(context.Background(), storeID, nil, tuple)
			require.NoError(t, err)
		}

		seenObjects := map[string]struct{}{}

		var changes []*openfgav1.TupleChange
		var continuationToken []byte
		var err error
		for {
			changes, continuationToken, err = datastore.ReadChanges(context.Background(), storeID, "", storage.PaginationOptions{
				PageSize: 10,
				From:     string(continuationToken),
			}, 1*time.Millisecond)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					break
				}
			}
			require.NoError(t, err)
			require.NotNil(t, continuationToken)

			for _, change := range changes {
				if _, ok := seenObjects[change.TupleKey.Object]; ok {
					require.FailNowf(t, "duplicate changelog entry encountered", change.TupleKey.Object)
				}

				seenObjects[change.TupleKey.Object] = struct{}{}
			}

			if string(continuationToken) == "" {
				break
			}
		}
	})

	t.Run("read_changes_with_conditions", func(t *testing.T) {
		storeID := ulid.Make().String()

		tk1 := &openfgav1.TupleKey{
			Object:   tuple.BuildObject("folder", "folder1"),
			Relation: "viewer",
			User:     "bob",
			Condition: &openfgav1.RelationshipCondition{
				Name: "condition",
			},
		}
		tk2 := &openfgav1.TupleKey{
			Object:   tuple.BuildObject("folder", "folder2"),
			Relation: "viewer",
			User:     "bill",
			Condition: &openfgav1.RelationshipCondition{
				Name:    "condition",
				Context: testutils.MustNewStruct(t, map[string]interface{}{"param1": "ok"}),
			},
		}

		err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk1, tk2})
		require.NoError(t, err)

		changes, continuationToken, err := datastore.ReadChanges(ctx, storeID, "", storage.PaginationOptions{PageSize: storage.DefaultPageSize}, 0)
		require.NoError(t, err)
		require.NotEmpty(t, continuationToken)

		expectedChanges := []*openfgav1.TupleChange{
			{
				TupleKey: &openfgav1.TupleKey{
					Object:   tuple.BuildObject("folder", "folder1"),
					Relation: "viewer",
					User:     "bob",
					Condition: &openfgav1.RelationshipCondition{
						Name:    "condition",
						Context: &structpb.Struct{},
					},
				},
				Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
			},
			{
				TupleKey:  tk2,
				Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
			},
		}

		if diff := cmp.Diff(expectedChanges, changes, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("tuple_with_condition_deleted", func(t *testing.T) {
		storeID := ulid.Make().String()

		tk1 := &openfgav1.TupleKey{
			Object:   tuple.BuildObject("document", "1"),
			Relation: "viewer",
			User:     "user:jon",
			Condition: &openfgav1.RelationshipCondition{
				Name: "mycond",
				Context: testutils.MustNewStruct(t, map[string]interface{}{
					"x": 10,
				}),
			},
		}
		err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk1})
		require.NoError(t, err)

		tk2 := &openfgav1.TupleKeyWithoutCondition{
			Object:   tuple.BuildObject("document", "1"),
			Relation: "viewer",
			User:     "user:jon",
		}

		err = datastore.Write(ctx, storeID, []*openfgav1.TupleKeyWithoutCondition{tk2}, nil)
		require.NoError(t, err)

		changes, continuationToken, err := datastore.ReadChanges(ctx, storeID, "", storage.PaginationOptions{PageSize: storage.DefaultPageSize}, 0)
		require.NoError(t, err)
		require.NotEmpty(t, continuationToken)

		expectedChanges := []*openfgav1.TupleChange{
			{
				TupleKey:  tk1,
				Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
			},
			{
				// Tuples with a condition that are deleted don't include the condition info
				// in the changelog entry.
				TupleKey:  tuple.NewTupleKey("document:1", "viewer", "user:jon"),
				Operation: openfgav1.TupleOperation_TUPLE_OPERATION_DELETE,
			},
		}

		if diff := cmp.Diff(expectedChanges, changes, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
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

		// Write tks.
		err := datastore.Write(ctx, storeID, nil, tks)
		require.NoError(t, err)

		// Try to delete tks[0,1], and at the same time write tks[2]. It should fail with expectedError.
		err = datastore.Write(
			ctx,
			storeID,
			[]*openfgav1.TupleKeyWithoutCondition{
				tuple.TupleKeyToTupleKeyWithoutCondition(tks[0]),
				tuple.TupleKeyToTupleKeyWithoutCondition(tks[1]),
			},
			[]*openfgav1.TupleKey{tks[2]},
		)
		require.EqualError(t, err, expectedError.Error())

		tuples, _, err := datastore.ReadPage(ctx, storeID, nil, storage.PaginationOptions{PageSize: 50})
		require.NoError(t, err)
		require.Equal(t, len(tks), len(tuples))
	})

	t.Run("delete_fails_if_the_tuple_does_not_exist", func(t *testing.T) {
		storeID := ulid.Make().String()
		tk := &openfgav1.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}

		err := datastore.Write(
			ctx,
			storeID,
			[]*openfgav1.TupleKeyWithoutCondition{
				tuple.TupleKeyToTupleKeyWithoutCondition(tk),
			},
			nil,
		)
		require.ErrorContains(t, err, "cannot delete a tuple which does not exist")
	})

	t.Run("deleting_a_tuple_which_exists_succeeds", func(t *testing.T) {
		storeID := ulid.Make().String()
		tk := &openfgav1.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}

		// Write.
		err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk})
		require.NoError(t, err)

		// Then delete.
		err = datastore.Write(
			ctx,
			storeID,
			[]*openfgav1.TupleKeyWithoutCondition{
				tuple.TupleKeyToTupleKeyWithoutCondition(tk),
			},
			nil,
		)
		require.NoError(t, err)

		// Ensure it is not there.
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

	t.Run("inserting_a_tuple_twice_either_conditioned_or_not_fails", func(t *testing.T) {
		storeID := ulid.Make().String()
		tk := &openfgav1.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}
		expectedError := storage.InvalidWriteInputError(tk, openfgav1.TupleOperation_TUPLE_OPERATION_WRITE)

		// First write should succeed.
		err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk})
		require.NoError(t, err)

		// Second write of the same tuple but conditioned should still fail.
		err = datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{
				Object:   tk.Object,
				Relation: tk.Relation,
				User:     tk.User,
				Condition: &openfgav1.RelationshipCondition{
					Name: "condition",
				},
			},
		})
		require.EqualError(t, err, expectedError.Error())
	})

	t.Run("inserting_conditioned_tuple_and_deleting_tuple_succeeds", func(t *testing.T) {
		storeID := ulid.Make().String()
		tk := &openfgav1.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}

		writes := []*openfgav1.TupleKey{
			{
				Object:   tk.Object,
				Relation: tk.Relation,
				User:     tk.User,
				Condition: &openfgav1.RelationshipCondition{
					Name: "condition",
				},
			},
		}

		deletes := []*openfgav1.TupleKeyWithoutCondition{
			{
				Object:   tk.Object,
				Relation: tk.Relation,
				User:     tk.User,
			},
		}

		err := datastore.Write(ctx, storeID, nil, writes)
		require.NoError(t, err)

		err = datastore.Write(ctx, storeID, deletes, nil)
		require.NoError(t, err)
	})

	t.Run("reading_a_tuple_that_exists_succeeds", func(t *testing.T) {
		storeID := ulid.Make().String()
		tuple1 := tuple.NewTupleKey("doc:readme", "owner", "user:jon")
		tuple2 := tuple.NewTupleKey("doc:readme", "viewer", "doc:other#viewer")
		tuple3 := tuple.NewTupleKey("doc:readme", "viewer", "user:*")
		tuple4 := &openfgav1.TupleKey{
			Object:   "doc:readme",
			Relation: "viewer",
			User:     "user:anne",
			Condition: &openfgav1.RelationshipCondition{
				Name:    "condition",
				Context: &structpb.Struct{},
			},
		}

		err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tuple1, tuple2, tuple3, tuple4})
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

		gotTuple, err = datastore.ReadUserTuple(ctx, storeID, tuple4)
		require.NoError(t, err)

		if diff := cmp.Diff(tuple4, gotTuple.Key, cmpOpts...); diff != "" {
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
			tk, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}

				require.Fail(t, "unexpected error encountered")
			}

			gotTupleKeys = append(gotTupleKeys, tk)
		}

		// Then the iterator should run out.
		_, err = gotTuples.Next(ctx)
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

		_, err = gotTuples.Next(ctx)
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

		gotTk, err := iter.Next(ctx)
		require.NoError(t, err)

		expected := tuple.NewTupleKey("document:1", "viewer", "group:eng#member")
		if diff := cmp.Diff(expected, gotTk, cmpOpts...); diff != "" {
			require.FailNowf(t, "mismatch (-want +got):\n%s", diff)
		}

		_, err = iter.Next(ctx)
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

		gotOne, err := iter.Next(ctx)
		require.NoError(t, err)
		gotTwo, err := iter.Next(ctx)
		require.NoError(t, err)

		expected := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			tuple.NewTupleKey("document:1", "viewer", "grouping:eng#member"),
		}
		if diff := cmp.Diff(expected, []*openfgav1.TupleKey{gotOne, gotTwo}, cmpOpts...); diff != "" {
			require.FailNowf(t, "mismatch (-want +got):\n%s", diff)
		}

		_, err = iter.Next(ctx)
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

		got, err := iter.Next(ctx)
		require.NoError(t, err)

		expected := tuple.NewTupleKey("document:1", "viewer", "user:*")
		if diff := cmp.Diff(expected, got, cmpOpts...); diff != "" {
			require.FailNowf(t, "mismatch (-want +got):\n%s", diff)
		}

		_, err = iter.Next(ctx)
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

		gotOne, err := iter.Next(ctx)
		require.NoError(t, err)
		gotTwo, err := iter.Next(ctx)
		require.NoError(t, err)

		expected := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			tuple.NewTupleKey("document:1", "viewer", "user:*"),
		}
		if diff := cmp.Diff(expected, []*openfgav1.TupleKey{gotOne, gotTwo}, cmpOpts...); diff != "" {
			require.FailNowf(t, "mismatch (-want +got):\n%s", diff)
		}

		_, err = iter.Next(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("tuples_with_nil_condition", func(t *testing.T) {
		// This test ensures we don't normalize nil conditions to an empty value.
		storeID := ulid.Make().String()

		tupleKey1 := tuple.NewTupleKey("document:1", "viewer", "user:jon")
		tupleKey2 := tuple.NewTupleKey("group:1", "member", "group:2#member")

		tks := []*openfgav1.TupleKey{
			{
				Object:    tupleKey1.GetObject(),
				Relation:  tupleKey1.GetRelation(),
				User:      tupleKey1.GetUser(),
				Condition: nil,
			},
			{
				Object:    tupleKey2.GetObject(),
				Relation:  tupleKey2.GetRelation(),
				User:      tupleKey2.GetUser(),
				Condition: nil,
			},
		}

		err := datastore.Write(ctx, storeID, nil, tks)
		require.NoError(t, err)

		iter, err := datastore.Read(ctx, storeID, tupleKey1)
		require.NoError(t, err)
		defer iter.Stop()

		tp, err := iter.Next(ctx)
		require.NoError(t, err)
		require.Nil(t, tp.GetKey().GetCondition())

		tuples, _, err := datastore.ReadPage(ctx, storeID, &openfgav1.TupleKey{}, storage.PaginationOptions{
			PageSize: 2,
		})
		require.NoError(t, err)
		require.Len(t, tuples, 2)
		require.Nil(t, tuples[0].GetKey().GetCondition())
		require.Nil(t, tuples[1].GetKey().GetCondition())

		tp, err = datastore.ReadUserTuple(ctx, storeID, tupleKey1)
		require.NoError(t, err)
		require.Nil(t, tp.GetKey().GetCondition())

		iter, err = datastore.ReadUsersetTuples(ctx, storeID, storage.ReadUsersetTuplesFilter{
			Object:   tupleKey2.GetObject(),
			Relation: tupleKey2.GetRelation(),
		})
		require.NoError(t, err)
		defer iter.Stop()

		tp, err = iter.Next(ctx)
		require.NoError(t, err)
		require.Nil(t, tp.GetKey().GetCondition())

		iter, err = datastore.ReadStartingWithUser(ctx, storeID, storage.ReadStartingWithUserFilter{
			ObjectType: tuple.GetType(tupleKey1.GetObject()),
			Relation:   tupleKey1.GetRelation(),
			UserFilter: []*openfgav1.ObjectRelation{
				{Object: tupleKey1.GetUser()},
			},
		})
		require.NoError(t, err)
		defer iter.Stop()

		tp, err = iter.Next(ctx)
		require.NoError(t, err)
		require.Nil(t, tp.GetKey().GetCondition())

		changes, _, err := datastore.ReadChanges(ctx, storeID, "", storage.PaginationOptions{}, 0)
		require.NoError(t, err)
		require.Len(t, changes, 2)
		require.Nil(t, changes[0].GetTupleKey().GetCondition())
		require.Nil(t, changes[1].GetTupleKey().GetCondition())
	})

	t.Run("normalize_empty_context", func(t *testing.T) {
		// This test ensures we normalize nil or empty context as empty context in all reads.
		storeID := ulid.Make().String()

		tupleKey1 := tuple.NewTupleKey("document:1", "viewer", "user:jon")
		tupleKey2 := tuple.NewTupleKey("group:1", "member", "group:2#member")

		tks := []*openfgav1.TupleKey{
			{
				Object:   tupleKey1.GetObject(),
				Relation: tupleKey1.GetRelation(),
				User:     tupleKey1.GetUser(),
				Condition: &openfgav1.RelationshipCondition{
					Name:    "somecondition",
					Context: nil,
				},
			},
			{
				Object:   tupleKey2.GetObject(),
				Relation: tupleKey2.GetRelation(),
				User:     tupleKey2.GetUser(),
				Condition: &openfgav1.RelationshipCondition{
					Name:    "othercondition",
					Context: nil,
				},
			},
		}

		err := datastore.Write(ctx, storeID, nil, tks)
		require.NoError(t, err)

		iter, err := datastore.Read(ctx, storeID, tupleKey1)
		require.NoError(t, err)
		defer iter.Stop()

		tp, err := iter.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, "somecondition", tp.GetKey().GetCondition().GetName())
		require.NotNil(t, tp.GetKey().GetCondition().GetContext())
		require.Empty(t, tp.GetKey().GetCondition().GetContext())

		tuples, _, err := datastore.ReadPage(ctx, storeID, &openfgav1.TupleKey{}, storage.PaginationOptions{
			PageSize: 2,
		})
		require.NoError(t, err)
		require.Len(t, tuples, 2)
		require.NotNil(t, tuples[0].GetKey().GetCondition().GetContext())
		require.NotNil(t, tuples[1].GetKey().GetCondition().GetContext())

		tp, err = datastore.ReadUserTuple(ctx, storeID, tupleKey1)
		require.NoError(t, err)
		require.NotNil(t, tp.GetKey().GetCondition().GetContext())

		iter, err = datastore.ReadUsersetTuples(ctx, storeID, storage.ReadUsersetTuplesFilter{
			Object:   tupleKey2.GetObject(),
			Relation: tupleKey2.GetRelation(),
		})
		require.NoError(t, err)
		defer iter.Stop()

		tp, err = iter.Next(ctx)
		require.NoError(t, err)
		require.NotNil(t, tp.GetKey().GetCondition().GetContext())

		iter, err = datastore.ReadStartingWithUser(ctx, storeID, storage.ReadStartingWithUserFilter{
			ObjectType: tuple.GetType(tupleKey1.GetObject()),
			Relation:   tupleKey1.GetRelation(),
			UserFilter: []*openfgav1.ObjectRelation{
				{Object: tupleKey1.GetUser()},
			},
		})
		require.NoError(t, err)
		defer iter.Stop()

		tp, err = iter.Next(ctx)
		require.NoError(t, err)
		require.NotNil(t, tp.GetKey().GetCondition().GetContext())

		changes, _, err := datastore.ReadChanges(ctx, storeID, "", storage.PaginationOptions{}, 0)
		require.NoError(t, err)
		require.Len(t, changes, 2)
		require.NotNil(t, changes[0].GetTupleKey().GetCondition().GetContext())
		require.NotNil(t, changes[1].GetTupleKey().GetCondition().GetContext())
	})
}

func ReadPageTestCorrectnessOfContinuationTokens(t *testing.T, datastore storage.OpenFGADatastore) {
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
		{
			Object:   "document:doc4",
			Relation: "viewer",
			User:     "user:jon",
			Condition: &openfgav1.RelationshipCondition{
				Name: "condition",
			},
		},
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

		require.ElementsMatch([]string{"document:doc1", "document:doc2", "document:doc4"}, objects)
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

func ReadTestCorrectnessOfTuples(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	tuples := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "reader", "user:anne"),
		tuple.NewTupleKey("document:1", "reader", "user:bob"),
		tuple.NewTupleKey("document:1", "writer", "user:bob"),
		{
			Object:   "document:2",
			Relation: "viewer",
			User:     "user:anne",
			Condition: &openfgav1.RelationshipCondition{
				Name: "condition",
			},
		},
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
			{
				Object:   "document:2",
				Relation: "viewer",
				User:     "user:anne",
				Condition: &openfgav1.RelationshipCondition{
					Name:    "condition",
					Context: &structpb.Struct{},
				},
			},
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

	t.Run("filter_by_user_and_objectType", func(t *testing.T) {
		tupleIterator, err := datastore.Read(
			ctx,
			storeID,
			tuple.NewTupleKey("document:", "", "user:bob"),
		)
		require.NoError(t, err)
		defer tupleIterator.Stop()

		expectedTupleKeys := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "reader", "user:bob"),
			tuple.NewTupleKey("document:1", "writer", "user:bob"),
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

func ReadPageTestCorrectnessOfContinuationTokensV2(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	storeID := ulid.Make().String()

	tuplesWritten := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "reader", "user:anne"),
		// read should skip over these
		tuple.NewTupleKey("document:2", "a", "user:anne"),
		tuple.NewTupleKey("document:2", "b", "user:anne"),
		tuple.NewTupleKey("document:2", "c", "user:anne"),
		tuple.NewTupleKey("document:2", "d", "user:anne"),
		tuple.NewTupleKey("document:2", "e", "user:anne"),
		tuple.NewTupleKey("document:2", "f", "user:anne"),
		tuple.NewTupleKey("document:2", "g", "user:anne"),
		tuple.NewTupleKey("document:2", "h", "user:anne"),
		tuple.NewTupleKey("document:2", "j", "user:anne"),
		// end of skip
		tuple.NewTupleKey("document:1", "admin", "user:anne"),
	}

	err := datastore.Write(ctx, storeID, nil, tuplesWritten)
	require.NoError(t, err)

	t.Run("returns_2_results_and_no_continuation_token_when_page_size_2", func(t *testing.T) {
		tuplesRead, contToken, err := datastore.ReadPage(
			ctx,
			storeID,
			tuple.NewTupleKey("document:1", "", "user:anne"),
			storage.PaginationOptions{
				PageSize: 2,
			},
		)
		require.NoError(t, err)

		expectedTuples := []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "admin", "user:anne")},
			{Key: tuple.NewTupleKey("document:1", "reader", "user:anne")},
		}

		requireEqualTuples(t, expectedTuples, tuplesRead)
		require.Empty(t, contToken)
	})

	t.Run("returns_1_results_and_continuation_token_when_page_size_1", func(t *testing.T) {
		firstRead, contToken, err := datastore.ReadPage(
			ctx,
			storeID,
			tuple.NewTupleKey("document:1", "", "user:anne"),
			storage.PaginationOptions{
				PageSize: 1,
			},
		)
		require.NoError(t, err)

		require.Len(t, firstRead, 1)
		require.Equal(t, "document:1", firstRead[0].Key.Object)
		require.Equal(t, "user:anne", firstRead[0].Key.User)
		require.NotEmpty(t, contToken)

		// use the token

		secondRead, contToken, err := datastore.ReadPage(
			ctx,
			storeID,
			tuple.NewTupleKey("document:1", "", "user:anne"),
			storage.PaginationOptions{
				PageSize: 50, // fetch the remainder
				From:     string(contToken),
			},
		)
		require.NoError(t, err)

		require.Len(t, secondRead, 1)
		require.Equal(t, "document:1", secondRead[0].Key.Object)
		require.Equal(t, "user:anne", secondRead[0].Key.User)
		require.NotEqual(t, firstRead[0].Key.Relation, secondRead[0].Key.Relation)
		require.Empty(t, contToken)
	})
}

func ReadPageTestCorrectnessOfTuples(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	tuples := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "reader", "user:anne"),
		tuple.NewTupleKey("document:1", "reader", "user:bob"),
		tuple.NewTupleKey("document:1", "writer", "user:bob"),
		{
			Object:   "document:2",
			Relation: "viewer",
			User:     "user:anne",
			Condition: &openfgav1.RelationshipCondition{
				Name: "condition",
			},
		},
	}

	storeID := ulid.Make().String()

	err := datastore.Write(ctx, storeID, nil, tuples)
	require.NoError(t, err)

	t.Run("empty_filter_returns_all_tuples", func(t *testing.T) {
		gotTuples, contToken, err := datastore.ReadPage(
			ctx,
			storeID,
			tuple.NewTupleKey("", "", ""),
			storage.PaginationOptions{
				PageSize: 50,
			},
		)
		require.NoError(t, err)

		expectedTuples := []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "reader", "user:anne")},
			{Key: tuple.NewTupleKey("document:1", "reader", "user:bob")},
			{Key: tuple.NewTupleKey("document:1", "writer", "user:bob")},
			{Key: tuple.NewTupleKeyWithCondition("document:2", "viewer", "user:anne", "condition", nil)},
		}

		requireEqualTuples(t, expectedTuples, gotTuples)
		require.Empty(t, contToken)
	})

	t.Run("filter_by_user_and_relation_and_objectID", func(t *testing.T) {
		gotTuples, contToken, err := datastore.ReadPage(
			ctx,
			storeID,
			tuple.NewTupleKey("document:1", "reader", "user:bob"),
			storage.PaginationOptions{
				PageSize: 50,
			},
		)
		require.NoError(t, err)

		expectedTuples := []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "reader", "user:bob")},
		}

		requireEqualTuples(t, expectedTuples, gotTuples)
		require.Empty(t, contToken)
	})

	t.Run("filter_by_user_and_relation_and_objectType", func(t *testing.T) {
		gotTuples, contToken, err := datastore.ReadPage(
			ctx,
			storeID,
			tuple.NewTupleKey("document:", "reader", "user:bob"),
			storage.PaginationOptions{
				PageSize: 50,
			},
		)
		require.NoError(t, err)

		expectedTuples := []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "reader", "user:bob")},
		}

		requireEqualTuples(t, expectedTuples, gotTuples)
		require.Empty(t, contToken)
	})

	t.Run("filter_by_user_and_objectType", func(t *testing.T) {
		gotTuples, contToken, err := datastore.ReadPage(
			ctx,
			storeID,
			tuple.NewTupleKey("document:", "", "user:bob"),
			storage.PaginationOptions{
				PageSize: 50,
			},
		)
		require.NoError(t, err)

		expectedTuples := []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "reader", "user:bob")},
			{Key: tuple.NewTupleKey("document:1", "writer", "user:bob")},
		}

		requireEqualTuples(t, expectedTuples, gotTuples)
		require.Empty(t, contToken)
	})

	t.Run("filter_by_relation_and_objectID", func(t *testing.T) {
		gotTuples, contToken, err := datastore.ReadPage(
			ctx,
			storeID,
			tuple.NewTupleKey("document:1", "reader", ""),
			storage.PaginationOptions{
				PageSize: 50,
			},
		)
		require.NoError(t, err)

		expectedTuples := []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "reader", "user:bob")},
			{Key: tuple.NewTupleKey("document:1", "reader", "user:anne")},
		}

		requireEqualTuples(t, expectedTuples, gotTuples)
		require.Empty(t, contToken)
	})

	t.Run("filter_by_objectID", func(t *testing.T) {
		gotTuples, contToken, err := datastore.ReadPage(
			ctx,
			storeID,
			tuple.NewTupleKey("document:1", "", ""),
			storage.PaginationOptions{
				PageSize: 50,
			},
		)
		require.NoError(t, err)

		expectedTuples := []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "reader", "user:anne")},
			{Key: tuple.NewTupleKey("document:1", "reader", "user:bob")},
			{Key: tuple.NewTupleKey("document:1", "writer", "user:bob")},
		}

		requireEqualTuples(t, expectedTuples, gotTuples)
		require.Empty(t, contToken)
	})

	t.Run("filter_by_objectID_and_user", func(t *testing.T) {
		gotTuples, contToken, err := datastore.ReadPage(
			ctx,
			storeID,
			tuple.NewTupleKey("document:1", "", "user:bob"),
			storage.PaginationOptions{
				PageSize: 50,
			},
		)
		require.NoError(t, err)

		expectedTuples := []*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("document:1", "reader", "user:bob")},
			{Key: tuple.NewTupleKey("document:1", "writer", "user:bob")},
		}

		requireEqualTuples(t, expectedTuples, gotTuples)
		require.Empty(t, contToken)
	})
}

func getObjects(tupleIterator storage.TupleIterator, require *require.Assertions) []string {
	var objects []string
	for {
		tp, err := tupleIterator.Next(context.Background())
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
		tp, err := tupleIterator.Next(context.Background())
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

func requireEqualTuples(t *testing.T, expectedTuples []*openfgav1.Tuple, actualTuples []*openfgav1.Tuple) {
	cmpOpts := []cmp.Option{
		protocmp.IgnoreFields(protoadapt.MessageV2Of(&openfgav1.Tuple{}), "timestamp"),
		testutils.TupleCmpTransformer,
		protocmp.Transform(),
	}
	diff := cmp.Diff(expectedTuples, actualTuples, cmpOpts...)
	require.Empty(t, diff)
}
