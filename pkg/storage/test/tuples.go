package test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

var (
	cmpSortTupleKeys = []cmp.Option{
		protocmp.IgnoreFields(protoadapt.MessageV2Of(&openfgav1.Tuple{}), "timestamp"),
		testutils.TupleKeyCmpTransformer,
		protocmp.Transform(),
	}
	cmpIgnoreTimestamp = []cmp.Option{
		protocmp.IgnoreFields(protoadapt.MessageV2Of(&openfgav1.TupleChange{}), "timestamp"),
		protocmp.Transform(),
	}
)

func ReadChangesTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	const numOfWrites = 300

	t.Run("lots_of_writes_returns_everything", func(t *testing.T) {
		storeID := ulid.Make().String()

		var writtenTuples []*openfgav1.TupleKey
		for i := 0; i < numOfWrites; i++ {
			newTuple := tuple.NewTupleKey(fmt.Sprintf("document:%d", i), "viewer", "user:jon")
			err := datastore.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{newTuple})
			require.NoError(t, err)
			writtenTuples = append(writtenTuples, newTuple)
		}

		// No assertions on the contents of the response, just on the length.

		t.Run("page_size_1", func(t *testing.T) {
			changes := readChangesWithPageSize(t, datastore, storeID, 1, "")
			assert.Len(t, changes, len(writtenTuples))
		})

		t.Run("page_size_2", func(t *testing.T) {
			changes := readChangesWithPageSize(t, datastore, storeID, 2, "")
			assert.Len(t, changes, len(writtenTuples))
		})

		t.Run("page_size_infinite", func(t *testing.T) {
			changes := readChangesWithPageSize(t, datastore, storeID, numOfWrites, "")
			assert.Len(t, changes, len(writtenTuples))
		})
	})

	t.Run("read_changes_with_start_time", func(t *testing.T) {
		storeID := ulid.Make().String()

		var writtenTuplesBefore, writtenTuplesAfter []*openfgav1.TupleKey
		for i := 0; i < numOfWrites/2; i++ {
			newTuple := tuple.NewTupleKey(fmt.Sprintf("document:%d", i), "viewer", "user:before")
			err := datastore.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{newTuple})
			require.NoError(t, err)
			writtenTuplesBefore = append(writtenTuplesBefore, newTuple)
		}

		time.Sleep(1 * time.Millisecond)

		startTime := time.Now()

		time.Sleep(1 * time.Millisecond)

		for i := numOfWrites / 2; i < numOfWrites; i++ {
			newTuple := tuple.NewTupleKey(fmt.Sprintf("document:%d", i), "viewer", "user:after")
			err := datastore.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{newTuple})
			require.NoError(t, err)
			writtenTuplesAfter = append(writtenTuplesAfter, newTuple)
		}

		// No assertions on the contents of the response, just on the length.

		t.Run("start_time_page_size_1", func(t *testing.T) {
			changes := readChangesWithStartTime(t, datastore, storeID, 1, startTime, false)
			assert.Len(t, changes, len(writtenTuplesAfter))
			for _, change := range changes {
				assert.Equal(t, "user:after", change.GetTupleKey().GetUser())
			}
		})

		t.Run("start_time_page_size_default", func(t *testing.T) {
			changes := readChangesWithStartTime(t, datastore, storeID, storage.DefaultPageSize, startTime, false)
			assert.Len(t, changes, len(writtenTuplesAfter))
			for _, change := range changes {
				assert.Equal(t, "user:after", change.GetTupleKey().GetUser())
			}
		})

		t.Run("start_time_zero", func(t *testing.T) {
			changes := readChangesWithStartTime(t, datastore, storeID, numOfWrites, time.Time{}, false)
			assert.Len(t, changes, len(writtenTuplesBefore)+len(writtenTuplesAfter))
		})

		t.Run("start_time_desc", func(t *testing.T) {
			changes := readChangesWithStartTime(t, datastore, storeID, 1, startTime, true)
			assert.Len(t, changes, len(writtenTuplesBefore))
			for _, change := range changes {
				assert.Equal(t, "user:before", change.GetTupleKey().GetUser())
			}
		})
	})

	t.Run("lots_of_writes_with_filter_returns_everything", func(t *testing.T) {
		storeID := ulid.Make().String()
		filter := "folder"

		var writtenTuples []*openfgav1.TupleKey
		for i := 0; i < numOfWrites; i++ {
			newTuple1 := tuple.NewTupleKey(fmt.Sprintf("document:%d", i), "viewer", "user:jon")
			newTuple2 := tuple.NewTupleKey(fmt.Sprintf("%s:%d", filter, i), "viewer", "user:jon")
			err := datastore.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{newTuple1, newTuple2})
			require.NoError(t, err)
			writtenTuples = append(writtenTuples, newTuple1, newTuple2)
		}

		// No assertions on the contents of the response, just on the length.

		t.Run("page_size_1", func(t *testing.T) {
			changes := readChangesWithPageSize(t, datastore, storeID, 1, filter)
			assert.Len(t, changes, len(writtenTuples)/2)
		})

		t.Run("page_size_2", func(t *testing.T) {
			changes := readChangesWithPageSize(t, datastore, storeID, 2, filter)
			assert.Len(t, changes, len(writtenTuples)/2)
		})

		t.Run("page_size_infinite", func(t *testing.T) {
			changes := readChangesWithPageSize(t, datastore, storeID, numOfWrites, filter)
			assert.Len(t, changes, len(writtenTuples)/2)
		})
	})

	t.Run("read_changes_returns_non_empty_timestamp", func(t *testing.T) {
		storeID := ulid.Make().String()

		tk1 := &openfgav1.TupleKey{
			Object:   "document:1",
			Relation: "viewer",
			User:     "user:anne",
			Condition: &openfgav1.RelationshipCondition{
				Name:    "condition",
				Context: testutils.MustNewStruct(t, map[string]interface{}{"param1": "ok"}),
			},
		}
		tk1WithoutCond := &openfgav1.TupleKeyWithoutCondition{
			Object:   "document:1",
			Relation: "viewer",
			User:     "user:anne",
		}

		err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk1})
		require.NoError(t, err)

		err = datastore.Write(ctx, storeID, []*openfgav1.TupleKeyWithoutCondition{tk1WithoutCond}, nil)
		require.NoError(t, err)

		changes := readChangesWithPageSize(t, datastore, storeID, storage.DefaultPageSize, "")
		require.Len(t, changes, 2)
		for _, change := range changes {
			require.True(t, change.GetTimestamp().IsValid())
			require.False(t, change.GetTimestamp().AsTime().IsZero())
		}
	})

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

		expectedChanges := []*openfgav1.TupleChange{
			{
				TupleKey:  tk1,
				Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
			},
			{
				TupleKey:  tk2,
				Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
			},
		}

		changes := readChangesWithPageSize(t, datastore, storeID, 1, "")
		if diff := cmp.Diff(expectedChanges, changes, cmpIgnoreTimestamp...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("read_changes_with_no_changes_should_return_not_found", func(t *testing.T) {
		storeID := ulid.Make().String()

		opts := storage.ReadChangesOptions{
			Pagination: storage.NewPaginationOptions(storage.DefaultPageSize, ""),
		}
		_, token, err := datastore.ReadChanges(ctx, storeID, storage.ReadChangesFilter{}, opts)
		require.ErrorIs(t, err, storage.ErrNotFound)
		require.Empty(t, token)

		opts = storage.ReadChangesOptions{
			Pagination: storage.NewPaginationOptions(storage.DefaultPageSize, ""),
		}
		_, token, err = datastore.ReadChanges(ctx, storeID, storage.ReadChangesFilter{ObjectType: "somefilter"}, opts)
		require.ErrorIs(t, err, storage.ErrNotFound)
		require.Empty(t, token)
	})

	t.Run("read_changes_with_horizon_offset_non_zero_should_return_not_found_(no_changes)", func(t *testing.T) {
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

		opts := storage.ReadChangesOptions{
			Pagination: storage.NewPaginationOptions(storage.DefaultPageSize, ""),
		}
		_, token, err := datastore.ReadChanges(ctx, storeID, storage.ReadChangesFilter{HorizonOffset: 1 * time.Minute}, opts)
		require.ErrorIs(t, err, storage.ErrNotFound)
		require.Empty(t, token)
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

		changes := readChangesWithPageSize(t, datastore, storeID, storage.DefaultPageSize, "folder")

		expectedChanges := []*openfgav1.TupleChange{
			{
				TupleKey:  tk1,
				Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
			},
		}
		if diff := cmp.Diff(expectedChanges, changes, cmpIgnoreTimestamp...); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}

		changes = readChangesWithPageSize(t, datastore, storeID, storage.DefaultPageSize, "document")

		expectedChanges = []*openfgav1.TupleChange{
			{
				TupleKey:  tk2,
				Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
			},
		}
		if diff := cmp.Diff(expectedChanges, changes, cmpIgnoreTimestamp...); diff != "" {
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
		var continuationToken string
		var err error
		for {
			opts := storage.ReadChangesOptions{
				Pagination: storage.NewPaginationOptions(10, continuationToken),
			}
			changes, continuationToken, err = datastore.ReadChanges(context.Background(), storeID, storage.ReadChangesFilter{HorizonOffset: 1 * time.Millisecond}, opts)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					break
				}
			}
			require.NoError(t, err)
			require.NotNil(t, continuationToken)

			for _, change := range changes {
				if _, ok := seenObjects[change.GetTupleKey().GetObject()]; ok {
					require.FailNowf(t, "duplicate changelog entry encountered", change.GetTupleKey().GetObject())
				}

				seenObjects[change.GetTupleKey().GetObject()] = struct{}{}
			}

			if continuationToken == "" {
				break
			}
		}
	})

	t.Run("read_changes_after_concurrent_writes_returns_no_duplicates", func(t *testing.T) {
		tk1 := tuple.NewTupleKey("repo:1", "admin", "alice")
		tk2 := tuple.NewTupleKey("repo:1", "admin", "bob")
		tk3 := tuple.NewTupleKey("repo:1", "admin", "charlie")
		storeID := ulid.Make().String()

		tuplesToWriteOne := []*openfgav1.TupleKey{tk1, tk2}
		tuplesToWriteTwo := []*openfgav1.TupleKey{tk3}
		totalTuplesToWrite := len(tuplesToWriteOne) + len(tuplesToWriteTwo)
		writeTuplesConcurrently(t, storeID, datastore, tuplesToWriteOne, tuplesToWriteTwo)

		// without type
		changes1 := readChangesWithPageSize(t, datastore, storeID, 1, "")
		require.Len(t, changes1, totalTuplesToWrite)

		// with type
		changes2 := readChangesWithPageSize(t, datastore, storeID, 1, "repo")
		require.Len(t, changes2, totalTuplesToWrite)
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

		changes := readChangesWithPageSize(t, datastore, storeID, storage.DefaultPageSize, "")
		if diff := cmp.Diff(expectedChanges, changes, cmpIgnoreTimestamp...); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
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

		changes := readChangesWithPageSize(t, datastore, storeID, storage.DefaultPageSize, "")

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

		if diff := cmp.Diff(expectedChanges, changes, cmpIgnoreTimestamp...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("read_changes_with_special_characters", func(t *testing.T) {
		storeID := ulid.Make().String()

		tk1 := &openfgav1.TupleKey{
			Object:   "document:1|special_document",
			Relation: "viewer",
			User:     "user:anne",
		}
		tk2 := &openfgav1.TupleKey{
			Object:   "document:1",
			Relation: "viewer",
			User:     "user:anne@github.com|special_user",
		}
		tk3 := &openfgav1.TupleKey{
			Object:   "document:1|special_document",
			Relation: "viewer",
			User:     "user:anne@github.com|special_user",
		}

		err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk1})
		require.NoError(t, err)

		err = datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk2})
		require.NoError(t, err)

		err = datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk3})
		require.NoError(t, err)

		changes := readChangesWithPageSize(t, datastore, storeID, storage.DefaultPageSize, "")

		expectedChanges := []*openfgav1.TupleChange{
			{
				TupleKey:  tk1,
				Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
			},
			{
				TupleKey:  tk2,
				Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
			},
			{
				TupleKey:  tk3,
				Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
			},
		}
		if diff := cmp.Diff(expectedChanges, changes, cmpIgnoreTimestamp...); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("sort_desc_returns_most_recent_changes", func(t *testing.T) {
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
		tk3 := &openfgav1.TupleKey{
			Object:   tuple.BuildObject("document", "1"),
			Relation: "viewer",
			User:     "josh",
		}

		var err error
		err = datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk1})
		require.NoError(t, err)
		time.Sleep(1 * time.Second)
		err = datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk2})
		require.NoError(t, err)
		time.Sleep(1 * time.Second)
		err = datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk3})
		require.NoError(t, err)

		opts := storage.ReadChangesOptions{
			Pagination: storage.NewPaginationOptions(1, ""),
			SortDesc:   true,
		}
		docs, token, _ := datastore.ReadChanges(context.Background(), storeID, storage.ReadChangesFilter{}, opts)
		require.Len(t, docs, 1)
		require.Equal(t, tk3.GetUser(), docs[0].GetTupleKey().GetUser())

		opts = storage.ReadChangesOptions{
			Pagination: storage.NewPaginationOptions(1, token),
			SortDesc:   true,
		}
		docs, token, err = datastore.ReadChanges(context.Background(), storeID, storage.ReadChangesFilter{}, opts)
		require.NoError(t, err)
		require.Len(t, docs, 1)
		require.Equal(t, tk2.GetUser(), docs[0].GetTupleKey().GetUser())

		opts.Pagination = storage.NewPaginationOptions(1, token)
		docs, _, err = datastore.ReadChanges(context.Background(), storeID, storage.ReadChangesFilter{}, opts)
		require.NoError(t, err)
		require.Len(t, docs, 1)
		require.Equal(t, tk1.GetUser(), docs[0].GetTupleKey().GetUser())
	})

	t.Run("sort_desc_returns_most_recent_changes_with_object_type_filter", func(t *testing.T) {
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
		tk3 := &openfgav1.TupleKey{
			Object:   tuple.BuildObject("folder", "1"),
			Relation: "viewer",
			User:     "josh",
		}

		var err error
		err = datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk1})
		require.NoError(t, err)
		time.Sleep(1 * time.Second)
		err = datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk2})
		require.NoError(t, err)
		time.Sleep(1 * time.Second)
		err = datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tk3})
		require.NoError(t, err)

		opts := storage.ReadChangesOptions{
			Pagination: storage.NewPaginationOptions(1, ""),
			SortDesc:   true,
		}
		docs, token, err := datastore.ReadChanges(context.Background(), storeID, storage.ReadChangesFilter{ObjectType: "folder"}, opts)
		require.NoError(t, err)
		require.Len(t, docs, 1)
		require.Equal(t, tk3.GetUser(), docs[0].GetTupleKey().GetUser())

		opts = storage.ReadChangesOptions{
			Pagination: storage.NewPaginationOptions(1, token),
			SortDesc:   true,
		}
		docs, token, err = datastore.ReadChanges(context.Background(), storeID, storage.ReadChangesFilter{ObjectType: "folder"}, opts)
		require.NoError(t, err)
		require.Len(t, docs, 1)
		require.Equal(t, tk1.GetUser(), docs[0].GetTupleKey().GetUser())

		opts.Pagination = storage.NewPaginationOptions(1, token)
		_, _, err = datastore.ReadChanges(context.Background(), storeID, storage.ReadChangesFilter{ObjectType: "folder"}, opts)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})
}

func TupleWritingAndReadingTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	t.Run("lots_of_writes_and_read_returns_everything", func(t *testing.T) {
		storeID := ulid.Make().String()

		var writtenTuples []*openfgav1.TupleKey
		for i := 0; i < storage.DefaultPageSize*50; i++ {
			newTuple := tuple.NewTupleKey(fmt.Sprintf("document:%d", i), "viewer", "user:jon")
			err := datastore.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{newTuple})
			require.NoError(t, err)
			writtenTuples = append(writtenTuples, newTuple)
		}

		t.Run("read_returns_everything", func(t *testing.T) {
			tupleIterator, err := datastore.Read(ctx, storeID, tuple.NewTupleKey("", "", ""), storage.ReadOptions{})
			require.NoError(t, err)
			defer tupleIterator.Stop()

			seenTuples := iterateThroughAllTuples(t, tupleIterator)
			if diff := cmp.Diff(writtenTuples, seenTuples, cmpSortTupleKeys...); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})

		t.Run("read_page_size_1_returns_everything", func(t *testing.T) {
			seenTuples := testutils.ConvertTuplesToTupleKeys(readWithPageSize(t, datastore, storeID, 1, nil))
			if diff := cmp.Diff(writtenTuples, seenTuples, cmpSortTupleKeys...); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})

		t.Run("read_page_size_2_returns_everything", func(t *testing.T) {
			seenTuples := testutils.ConvertTuplesToTupleKeys(readWithPageSize(t, datastore, storeID, 2, nil))
			if diff := cmp.Diff(writtenTuples, seenTuples, cmpSortTupleKeys...); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})

		t.Run("read_page_size_default_returns_everything", func(t *testing.T) {
			seenTuples := testutils.ConvertTuplesToTupleKeys(readWithPageSize(t, datastore, storeID, storage.DefaultPageSize, nil))
			if diff := cmp.Diff(writtenTuples, seenTuples, cmpSortTupleKeys...); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})

		t.Run("read_page_size_infinite_returns_everything", func(t *testing.T) {
			seenTuples := testutils.ConvertTuplesToTupleKeys(readWithPageSize(t, datastore, storeID, storage.DefaultPageSize*50000, nil))
			if diff := cmp.Diff(writtenTuples, seenTuples, cmpSortTupleKeys...); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})
	})

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
				User:     "org:openfga#viewer",
			},
		}
		expectedError := errors.New("transactional write failed due to conflict: one or more tuples to delete were deleted by another transaction")

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

		// Since the write didn't succeed, we expect all tuples back
		seenTuples := testutils.ConvertTuplesToTupleKeys(readWithPageSize(t, datastore, storeID, storage.DefaultPageSize, nil))
		if diff := cmp.Diff(tks, seenTuples, cmpSortTupleKeys...); diff != "" {
			t.Fatalf("mismatch (-want +got):\n%s", diff)
		}
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
		_, err = datastore.ReadUserTuple(ctx, storeID, tk, storage.ReadUserTupleOptions{})
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
				Object:   tk.GetObject(),
				Relation: tk.GetRelation(),
				User:     tk.GetUser(),
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
				Object:   tk.GetObject(),
				Relation: tk.GetRelation(),
				User:     tk.GetUser(),
				Condition: &openfgav1.RelationshipCondition{
					Name: "condition",
				},
			},
		}

		deletes := []*openfgav1.TupleKeyWithoutCondition{
			{
				Object:   tk.GetObject(),
				Relation: tk.GetRelation(),
				User:     tk.GetUser(),
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

		gotTuple, err := datastore.ReadUserTuple(ctx, storeID, tuple1, storage.ReadUserTupleOptions{})
		require.NoError(t, err)

		if diff := cmp.Diff(tuple1, gotTuple.GetKey(), cmpOpts...); diff != "" {
			require.FailNowf(t, "mismatch (-want +got):\n%s", diff)
		}

		gotTuple, err = datastore.ReadUserTuple(ctx, storeID, tuple2, storage.ReadUserTupleOptions{})
		require.NoError(t, err)

		if diff := cmp.Diff(tuple2, gotTuple.GetKey(), cmpOpts...); diff != "" {
			require.FailNowf(t, "mismatch (-want +got):\n%s", diff)
		}

		gotTuple, err = datastore.ReadUserTuple(ctx, storeID, tuple3, storage.ReadUserTupleOptions{})
		require.NoError(t, err)

		if diff := cmp.Diff(tuple3, gotTuple.GetKey(), cmpOpts...); diff != "" {
			require.FailNowf(t, "mismatch (-want +got):\n%s", diff)
		}

		gotTuple, err = datastore.ReadUserTuple(ctx, storeID, tuple4, storage.ReadUserTupleOptions{})
		require.NoError(t, err)

		if diff := cmp.Diff(tuple4, gotTuple.GetKey(), cmpOpts...); diff != "" {
			require.FailNowf(t, "mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("reading_a_tuple_that_does_not_exist_returns_not_found", func(t *testing.T) {
		storeID := ulid.Make().String()
		tk := &openfgav1.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}

		_, err := datastore.ReadUserTuple(ctx, storeID, tk, storage.ReadUserTupleOptions{})
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
		}, storage.ReadUsersetTuplesOptions{})
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

	t.Run("reading_userset_tuples_that_don't_exist_should_return_an_empty_iterator", func(t *testing.T) {
		storeID := ulid.Make().String()

		gotTuples, err := datastore.ReadUsersetTuples(ctx, storeID, storage.ReadUsersetTuplesFilter{Object: "doc:readme", Relation: "owner"}, storage.ReadUsersetTuplesOptions{})
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
			tuple.NewTupleKey("document:1", "viewer", "group:*"),
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
		}, storage.ReadUsersetTuplesOptions{})
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
		}, storage.ReadUsersetTuplesOptions{})
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
		}, storage.ReadUsersetTuplesOptions{})
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
		}, storage.ReadUsersetTuplesOptions{})
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

	t.Run("reading_userset_tuples_with_filter_made_of_mix_references_for_same_type", func(t *testing.T) {
		storeID := ulid.Make().String()
		tks := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:*"),
			tuple.NewTupleKey("document:1", "viewer", "users:*"),
			tuple.NewTupleKey("document:1", "viewer", "group:*"),
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
				typesystem.WildcardRelationReference("group"),
			},
		}, storage.ReadUsersetTuplesOptions{})
		require.NoError(t, err)

		iter := storage.NewTupleKeyIteratorFromTupleIterator(gotTuples)
		defer iter.Stop()

		gotOne, err := iter.Next(ctx)
		require.NoError(t, err)
		gotTwo, err := iter.Next(ctx)
		require.NoError(t, err)

		expected := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "group:*"),
			tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
		}
		if diff := cmp.Diff(expected, []*openfgav1.TupleKey{gotOne, gotTwo}, cmpOpts...); diff != "" {
			require.FailNowf(t, "mismatch (-want +got):\n%s", diff)
		}

		_, err = iter.Next(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("reading_userset_tuples_distinguish_different_relation_ref", func(t *testing.T) {
		storeID := ulid.Make().String()
		tks := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:*"),
			tuple.NewTupleKey("document:1", "viewer", "users:*"),
			tuple.NewTupleKey("document:1", "viewer", "group:*"),
			tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			tuple.NewTupleKey("document:1", "viewer", "group:eng#other"),
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
		}, storage.ReadUsersetTuplesOptions{})
		require.NoError(t, err)

		iter := storage.NewTupleKeyIteratorFromTupleIterator(gotTuples)
		defer iter.Stop()

		gotOne, err := iter.Next(ctx)
		require.NoError(t, err)
		_, err = iter.Next(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)

		expected := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
		}
		if diff := cmp.Diff(expected, []*openfgav1.TupleKey{gotOne}, cmpOpts...); diff != "" {
			require.FailNowf(t, "mismatch (-want +got):\n%s", diff)
		}
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

		iter, err := datastore.Read(ctx, storeID, tupleKey1, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter.Stop()

		tp, err := iter.Next(ctx)
		require.NoError(t, err)
		require.Nil(t, tp.GetKey().GetCondition())

		opts := storage.ReadPageOptions{
			Pagination: storage.NewPaginationOptions(2, ""),
		}
		tuples, _, err := datastore.ReadPage(ctx, storeID, &openfgav1.TupleKey{}, opts)
		require.NoError(t, err)
		require.Len(t, tuples, 2)
		require.Nil(t, tuples[0].GetKey().GetCondition())
		require.Nil(t, tuples[1].GetKey().GetCondition())

		tp, err = datastore.ReadUserTuple(ctx, storeID, tupleKey1, storage.ReadUserTupleOptions{})
		require.NoError(t, err)
		require.Nil(t, tp.GetKey().GetCondition())

		iter, err = datastore.ReadUsersetTuples(ctx, storeID, storage.ReadUsersetTuplesFilter{
			Object:   tupleKey2.GetObject(),
			Relation: tupleKey2.GetRelation(),
		}, storage.ReadUsersetTuplesOptions{})
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
		}, storage.ReadStartingWithUserOptions{})
		require.NoError(t, err)
		defer iter.Stop()

		tp, err = iter.Next(ctx)
		require.NoError(t, err)
		require.Nil(t, tp.GetKey().GetCondition())

		readChangesOpts := storage.ReadChangesOptions{
			Pagination: storage.NewPaginationOptions(storage.DefaultPageSize, ""),
		}
		changes, _, err := datastore.ReadChanges(ctx, storeID, storage.ReadChangesFilter{}, readChangesOpts)
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

		iter, err := datastore.Read(ctx, storeID, tupleKey1, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter.Stop()

		tp, err := iter.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, "somecondition", tp.GetKey().GetCondition().GetName())
		require.NotNil(t, tp.GetKey().GetCondition().GetContext())
		require.Empty(t, tp.GetKey().GetCondition().GetContext())

		opts := storage.ReadPageOptions{
			Pagination: storage.NewPaginationOptions(2, ""),
		}
		tuples, _, err := datastore.ReadPage(ctx, storeID, &openfgav1.TupleKey{}, opts)
		require.NoError(t, err)
		require.Len(t, tuples, 2)
		require.NotNil(t, tuples[0].GetKey().GetCondition().GetContext())
		require.NotNil(t, tuples[1].GetKey().GetCondition().GetContext())

		tp, err = datastore.ReadUserTuple(ctx, storeID, tupleKey1, storage.ReadUserTupleOptions{})
		require.NoError(t, err)
		require.NotNil(t, tp.GetKey().GetCondition().GetContext())

		iter, err = datastore.ReadUsersetTuples(ctx, storeID, storage.ReadUsersetTuplesFilter{
			Object:   tupleKey2.GetObject(),
			Relation: tupleKey2.GetRelation(),
		}, storage.ReadUsersetTuplesOptions{})
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
		}, storage.ReadStartingWithUserOptions{})
		require.NoError(t, err)
		defer iter.Stop()

		tp, err = iter.Next(ctx)
		require.NoError(t, err)
		require.NotNil(t, tp.GetKey().GetCondition().GetContext())

		readChangesOpts := storage.ReadChangesOptions{
			Pagination: storage.NewPaginationOptions(storage.DefaultPageSize, ""),
		}
		changes, _, err := datastore.ReadChanges(ctx, storeID, storage.ReadChangesFilter{}, readChangesOpts)
		require.NoError(t, err)
		require.Len(t, changes, 2)
		require.NotNil(t, changes[0].GetTupleKey().GetCondition().GetContext())
		require.NotNil(t, changes[1].GetTupleKey().GetCondition().GetContext())
	})
}

func ReadStartingWithUserTest(t *testing.T, datastore storage.OpenFGADatastore) {
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

	t.Run("accepts_nil_object_ids", func(t *testing.T) {
		storeID := ulid.Make().String()
		_, err := datastore.ReadStartingWithUser(
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
			storage.ReadStartingWithUserOptions{},
		)
		require.NoError(t, err)
	})

	t.Run("returns_results_with_two_user_filters", func(t *testing.T) {
		storeID := ulid.Make().String()

		err := datastore.Write(ctx, storeID, nil, tuples)
		require.NoError(t, err)

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
			}, storage.ReadStartingWithUserOptions{},
		)
		require.NoError(t, err)

		objects := getObjects(t, tupleIterator)

		require.ElementsMatch(t, []string{"document:doc1", "document:doc2", "document:doc4"}, objects)
	})

	t.Run("returns_no_results_if_the_input_users_do_not_match_the_tuples", func(t *testing.T) {
		storeID := ulid.Make().String()

		err := datastore.Write(ctx, storeID, nil, tuples)
		require.NoError(t, err)

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
			}, storage.ReadStartingWithUserOptions{},
		)
		require.NoError(t, err)

		objects := getObjects(t, tupleIterator)

		require.Empty(t, objects)
	})

	t.Run("returns_no_results_if_the_input_relation_does_not_match_any_tuples", func(t *testing.T) {
		storeID := ulid.Make().String()

		err := datastore.Write(ctx, storeID, nil, tuples)
		require.NoError(t, err)

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
			}, storage.ReadStartingWithUserOptions{},
		)
		require.NoError(t, err)

		objects := getObjects(t, tupleIterator)

		require.Empty(t, objects)
	})

	t.Run("returns_no_results_if_the_input_object_type_does_not_match_any_tuples", func(t *testing.T) {
		storeID := ulid.Make().String()

		err := datastore.Write(ctx, storeID, nil, tuples)
		require.NoError(t, err)

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
			}, storage.ReadStartingWithUserOptions{},
		)
		require.NoError(t, err)

		objects := getObjects(t, tupleIterator)

		require.Empty(t, objects)
	})

	t.Run("returns_results_that_match_objectids_provided", func(t *testing.T) {
		storeID := ulid.Make().String()

		err := datastore.Write(ctx, storeID, nil, tuples)
		require.NoError(t, err)
		objectIDs := storage.NewSortedSet()
		for _, v := range []string{"doc1", "doc2", "doc3"} {
			objectIDs.Add(v)
		}

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
				},
				ObjectIDs: objectIDs,
			},
			storage.ReadStartingWithUserOptions{},
		)
		require.NoError(t, err)

		tuples := iterateThroughAllTuples(t, tupleIterator)

		require.Len(t, tuples, 1)
		_, objectID := tuple.SplitObject(tuples[0].GetObject())
		require.Equal(t, "doc1", objectID)
	})
	t.Run("enforce_order_of_tuples", func(t *testing.T) {
		storeID := ulid.Make().String()

		var tupleInReverseOrder = []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:doc7", "viewer", "user:bob"),
			tuple.NewTupleKey("document:doc6", "viewer", "user:bob"),
			tuple.NewTupleKey("document:doc5", "viewer", "user:bob"),
			tuple.NewTupleKey("document:doc4", "viewer", "user:bob"),
			tuple.NewTupleKey("document:doc3", "viewer", "user:bob"),
			tuple.NewTupleKey("document:doc2", "viewer", "group:eng#member"),
			tuple.NewTupleKey("document:doc1", "viewer", "user:bob"),
			tuple.NewTupleKey("folder:folder1", "viewer", "user:bob"),
		}

		err := datastore.Write(ctx, storeID, nil, tupleInReverseOrder)
		require.NoError(t, err)

		tupleIterator, err := datastore.ReadStartingWithUser(
			ctx,
			storeID,
			storage.ReadStartingWithUserFilter{
				ObjectType: "document",
				Relation:   "viewer",
				UserFilter: []*openfgav1.ObjectRelation{
					{
						Object: "user:bob",
					},
				},
			},
			storage.ReadStartingWithUserOptions{
				WithResultsSortedAscending: true,
			},
		)
		require.NoError(t, err)

		tuples := iterateThroughAllTuples(t, tupleIterator)
		var actualObjectIDs []string
		for _, item := range tuples {
			_, objectID := tuple.SplitObject(item.GetObject())
			actualObjectIDs = append(actualObjectIDs, objectID)
		}
		require.Equal(t, []string{"doc1", "doc3", "doc4", "doc5", "doc6", "doc7"}, actualObjectIDs)
	})
	t.Run("enforce_order_of_tuples_with_public_wildcard", func(t *testing.T) {
		storeID := ulid.Make().String()

		var tupleInReverseOrder = []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:doc8", "viewer", "user:(A)"),
			tuple.NewTupleKey("document:doc7", "viewer", "user:bob"),
			tuple.NewTupleKey("document:doc6", "viewer", "user:*"),
			tuple.NewTupleKey("document:doc5", "viewer", "user:bob"),
			tuple.NewTupleKey("document:doc4", "viewer", "user:*"),
			tuple.NewTupleKey("document:doc3", "viewer", "user:(A)"),
			tuple.NewTupleKey("document:doc2", "viewer", "group:eng#member"),
			tuple.NewTupleKey("document:doc1", "viewer", "user:bob"),
			tuple.NewTupleKey("folder:folder1", "viewer", "user:bob"),
		}

		err := datastore.Write(ctx, storeID, nil, tupleInReverseOrder)
		require.NoError(t, err)

		tupleIterator, err := datastore.ReadStartingWithUser(
			ctx,
			storeID,
			storage.ReadStartingWithUserFilter{
				ObjectType: "document",
				Relation:   "viewer",
				UserFilter: []*openfgav1.ObjectRelation{
					{
						Object: "user:(A)",
					},
					{
						Object: "user:bob",
					},
					{
						Object: "user:*",
					},
				},
			},
			storage.ReadStartingWithUserOptions{
				WithResultsSortedAscending: true,
			},
		)
		require.NoError(t, err)

		tuples := iterateThroughAllTuples(t, tupleIterator)
		var actualObjectIDs []string
		for _, item := range tuples {
			_, objectID := tuple.SplitObject(item.GetObject())
			actualObjectIDs = append(actualObjectIDs, objectID)
		}
		require.Equal(t, []string{"doc1", "doc3", "doc4", "doc5", "doc6", "doc7", "doc8"}, actualObjectIDs)
	})
	t.Run("enforce_order_of_tuples_with_public_wildcard_with_object_filter", func(t *testing.T) {
		storeID := ulid.Make().String()

		var tupleInReverseOrder = []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:doc8", "viewer", "user:(A)"),
			tuple.NewTupleKey("document:doc7", "viewer", "user:bob"),
			tuple.NewTupleKey("document:doc6", "viewer", "user:*"),
			tuple.NewTupleKey("document:doc5", "viewer", "user:bob"),
			tuple.NewTupleKey("document:doc4", "viewer", "user:*"),
			tuple.NewTupleKey("document:doc3", "viewer", "user:(A)"),
			tuple.NewTupleKey("document:doc2", "viewer", "group:eng#member"),
			tuple.NewTupleKey("document:doc1", "viewer", "user:bob"),
			tuple.NewTupleKey("folder:folder1", "viewer", "user:bob"),
		}

		err := datastore.Write(ctx, storeID, nil, tupleInReverseOrder)
		require.NoError(t, err)

		objectIDs := storage.NewSortedSet()
		for _, v := range []string{"doc3", "doc4", "doc5", "doc6", "unknown"} {
			objectIDs.Add(v)
		}

		tupleIterator, err := datastore.ReadStartingWithUser(
			ctx,
			storeID,
			storage.ReadStartingWithUserFilter{
				ObjectType: "document",
				Relation:   "viewer",
				UserFilter: []*openfgav1.ObjectRelation{
					{
						Object: "user:(A)",
					},
					{
						Object: "user:bob",
					},
					{
						Object: "user:*",
					},
				},
				ObjectIDs: objectIDs,
			},
			storage.ReadStartingWithUserOptions{
				WithResultsSortedAscending: true,
			},
		)
		require.NoError(t, err)

		tuples := iterateThroughAllTuples(t, tupleIterator)
		var actualObjectIDs []string
		for _, item := range tuples {
			_, objectID := tuple.SplitObject(item.GetObject())
			actualObjectIDs = append(actualObjectIDs, objectID)
		}
		require.Equal(t, []string{"doc3", "doc4", "doc5", "doc6"}, actualObjectIDs)
	})
}

func ReadAndReadPageTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	// This list contains: users with special character |,
	// objects with special character |,
	// interleaved object IDs,
	// interleaved user IDs,
	// interleaved object types,
	// and tuples with conditions.
	tuples := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "reader", "user:github.com|anne@test.com"),
		tuple.NewTupleKey("document:2", "a", "user:github.com|anne@test.com"),
		tuple.NewTupleKey("document:1", "reader", "user:github.com|bob@test.com"),
		tuple.NewTupleKey("document:2", "b", "user:github.com|anne@test.com"),
		tuple.NewTupleKey("document:2", "c", "user:github.com|anne@test.com"),
		tuple.NewTupleKey("document:2", "d", "user:github.com|anne@test.com"),
		tuple.NewTupleKey("document:2", "e", "user:github.com|anne@test.com"),
		tuple.NewTupleKey("document:2", "f", "user:github.com|anne@test.com"),
		tuple.NewTupleKey("folder:x", "viewer", "user:github.com|anne@test.com"),
		tuple.NewTupleKey("folder:x", "viewer", "user:github.com|bob@test.com"),
		tuple.NewTupleKey("document:2", "g", "user:github.com|anne@test.com"),
		tuple.NewTupleKey("document:1", "writer", "user:github.com|bob@test.com"),
		tuple.NewTupleKey("document:2", "h", "user:github.com|anne@test.com"),
		tuple.NewTupleKey("document:2", "j", "user:github.com|anne@test.com"),
		tuple.NewTupleKey("document:1", "admin", "user:github.com|anne@test.com"),
		tuple.NewTupleKey("document:1|special", "reader", "user:github.com|anne@test.com"),
		tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:github.com|anne@test.com", "condition1", nil),
		tuple.NewTupleKey("document:2", "writer", "user:github.com|charlie@test.com"),
		tuple.NewTupleKey("document:1|special", "writer", "user:github.com|charlie@test.com"),
	}

	storeID := ulid.Make().String()

	err := datastore.Write(ctx, storeID, nil, tuples)
	require.NoError(t, err)

	t.Run("returns_non_empty_timestamps", func(t *testing.T) {
		testCases := map[string]struct {
			filter *openfgav1.TupleKey
		}{
			`no_filter`: {
				filter: tuple.NewTupleKey("", "", ""),
			},
			`filter_by_objectID`: {
				filter: tuple.NewTupleKey("document:1", "", ""),
			},
		}

		for testName, test := range testCases {
			t.Run(testName, func(t *testing.T) {
				t.Run("Read_Page", func(t *testing.T) {
					seenTuples := readWithPageSize(t, datastore, storeID, 1, test.filter)
					for _, tuple := range seenTuples {
						require.True(t, tuple.GetTimestamp().IsValid())
						require.False(t, tuple.GetTimestamp().AsTime().IsZero())
					}
				})

				t.Run("Read", func(t *testing.T) {
					tupleIterator, err := datastore.Read(ctx, storeID, test.filter, storage.ReadOptions{})
					require.NoError(t, err)
					defer tupleIterator.Stop()

					for {
						tp, err := tupleIterator.Next(context.Background())
						if err != nil {
							require.ErrorIs(t, err, storage.ErrIteratorDone)
							break
						}
						require.True(t, tp.GetTimestamp().IsValid())
						require.False(t, tp.GetTimestamp().AsTime().IsZero())
					}
				})
			})
		}
	})

	testCases := map[string]struct {
		filter         *openfgav1.TupleKey
		expectedTuples []*openfgav1.TupleKey
	}{
		`no_filter`: {
			filter:         tuple.NewTupleKey("", "", ""),
			expectedTuples: tuples,
		},
		`filter_by_user_and_relation_and_objectID`: {
			filter: tuple.NewTupleKey("document:1", "reader", "user:github.com|anne@test.com"),
			expectedTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "reader", "user:github.com|anne@test.com"),
			},
		},
		`filter_by_user_and_relation_and_objectType`: {
			filter: tuple.NewTupleKey("document:", "reader", "user:github.com|bob@test.com"),
			expectedTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "reader", "user:github.com|bob@test.com"),
			},
		},
		`filter_by_user_and_objectType`: {
			filter: tuple.NewTupleKey("document:", "", "user:github.com|bob@test.com"),
			expectedTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "reader", "user:github.com|bob@test.com"),
				tuple.NewTupleKey("document:1", "writer", "user:github.com|bob@test.com"),
			},
		},
		`filter_by_relation_and_objectID`: {
			filter: tuple.NewTupleKey("document:1", "reader", ""),
			expectedTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "reader", "user:github.com|anne@test.com"),
				tuple.NewTupleKey("document:1", "reader", "user:github.com|bob@test.com"),
			},
		},
		`filter_by_objectID`: {
			filter: tuple.NewTupleKey("document:1", "", ""),
			expectedTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "reader", "user:github.com|anne@test.com"),
				tuple.NewTupleKey("document:1", "reader", "user:github.com|bob@test.com"),
				tuple.NewTupleKey("document:1", "writer", "user:github.com|bob@test.com"),
				tuple.NewTupleKey("document:1", "admin", "user:github.com|anne@test.com"),
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:github.com|anne@test.com", "condition1", nil),
			},
		},
		`filter_by_objectID_and_user`: {
			filter: tuple.NewTupleKey("document:1", "", "user:github.com|bob@test.com"),
			expectedTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "reader", "user:github.com|bob@test.com"),
				tuple.NewTupleKey("document:1", "writer", "user:github.com|bob@test.com"),
			},
		},
		`filter_by_objectID_with_special_character`: {
			filter: tuple.NewTupleKey("document:1|special", "", ""),
			expectedTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1|special", "reader", "user:github.com|anne@test.com"),
				tuple.NewTupleKey("document:1|special", "writer", "user:github.com|charlie@test.com"),
			},
		},
	}

	for testName, test := range testCases {
		t.Run(testName, func(t *testing.T) {
			t.Run("Read_Page", func(t *testing.T) {
				seenTuples := testutils.ConvertTuplesToTupleKeys(readWithPageSize(t, datastore, storeID, 1, test.filter))
				if diff := cmp.Diff(test.expectedTuples, seenTuples, cmpSortTupleKeys...); diff != "" {
					t.Fatalf("mismatch (-want +got):\n%s", diff)
				}
				seenTuples = testutils.ConvertTuplesToTupleKeys(readWithPageSize(t, datastore, storeID, 2, test.filter))
				if diff := cmp.Diff(test.expectedTuples, seenTuples, cmpSortTupleKeys...); diff != "" {
					t.Fatalf("mismatch (-want +got):\n%s", diff)
				}
				seenTuples = testutils.ConvertTuplesToTupleKeys(readWithPageSize(t, datastore, storeID, storage.DefaultPageSize, test.filter))
				if diff := cmp.Diff(test.expectedTuples, seenTuples, cmpSortTupleKeys...); diff != "" {
					t.Fatalf("mismatch (-want +got):\n%s", diff)
				}
			})

			t.Run("Read", func(t *testing.T) {
				tupleIterator, err := datastore.Read(ctx, storeID, test.filter, storage.ReadOptions{})
				require.NoError(t, err)
				defer tupleIterator.Stop()

				seenTuples := iterateThroughAllTuples(t, tupleIterator)
				if diff := cmp.Diff(test.expectedTuples, seenTuples, cmpSortTupleKeys...); diff != "" {
					t.Fatalf("mismatch (-want +got):\n%s", diff)
				}
			})
		})
	}
}

// getObjects returns all the objects from an iterator.
// If the iterator throws an error, it fails the test.
func getObjects(t *testing.T, tupleIterator storage.TupleIterator) []string {
	var objects []string
	for {
		tp, err := tupleIterator.Next(context.Background())
		if err != nil {
			require.ErrorIs(t, err, storage.ErrIteratorDone)
			break
		}

		objects = append(objects, tp.GetKey().GetObject())
	}
	return objects
}

// iterateThroughAllTuples returns all the tuples in the iterator.
// If the iterator throws an error, it fails the test.
func iterateThroughAllTuples(t *testing.T, tupleIterator storage.TupleIterator) []*openfgav1.TupleKey {
	t.Helper()
	var tupleKeys []*openfgav1.Tuple
	for {
		tp, err := tupleIterator.Next(context.Background())
		if err != nil {
			require.ErrorIs(t, err, storage.ErrIteratorDone)
			break
		}

		tupleKeys = append(tupleKeys, tp)
	}
	return testutils.ConvertTuplesToTupleKeys(tupleKeys)
}

// readChanges calls ReadChanges. It reads everything from the store, pageSize changes at a time.
// Along the way, it makes assertions on the changes seen. It returns all changes seen.
func readChangesWithPageSize(t *testing.T, ds storage.OpenFGADatastore, storeID string, pageSize int, objectTypeFilter string) []*openfgav1.TupleChange {
	t.Helper()
	var (
		tupleChanges      []*openfgav1.TupleChange
		seenChanges       []*openfgav1.TupleChange
		continuationToken string
		err               error
	)
	for {
		opts := storage.ReadChangesOptions{
			Pagination: storage.PaginationOptions{
				PageSize: pageSize,
				From:     continuationToken,
			},
		}
		tupleChanges, continuationToken, err = ds.ReadChanges(context.Background(), storeID, storage.ReadChangesFilter{ObjectType: objectTypeFilter}, opts)
		if err != nil {
			require.ErrorIs(t, err, storage.ErrNotFound)
			break
		}
		// Not strict equal because there may be less changes than the page size
		require.LessOrEqual(t, len(tupleChanges), pageSize, "len(tupleChanges)=%d, pageSize=%d, tuples=%v", len(tupleChanges), pageSize, tupleChanges)
		seenChanges = append(seenChanges, tupleChanges...)
		if len(tupleChanges) == 0 {
			require.Empty(t, continuationToken)
		} else {
			require.NotEmpty(t, continuationToken)
		}
	}

	return seenChanges
}

// readChanges calls ReadChanges. It reads everything from the store, pageSize changes at a time.
// Along the way, it makes assertions on the changes seen. It returns all changes seen.
// It reads changes starting from a specific time, by converting the timestamp into a continuation token.
func readChangesWithStartTime(t *testing.T, ds storage.OpenFGADatastore, storeID string, pageSize int, startTime time.Time, desc bool) []*openfgav1.TupleChange {
	t.Helper()
	var (
		tupleChanges      []*openfgav1.TupleChange
		seenChanges       []*openfgav1.TupleChange
		continuationToken string
		err               error
	)
	if !startTime.IsZero() {
		ulidST := ulid.MustNew(ulid.Timestamp(startTime), ulid.DefaultEntropy())
		continuationToken = ulidST.String()
	}
	for {
		opts := storage.ReadChangesOptions{
			Pagination: storage.NewPaginationOptions(
				int32(pageSize),
				continuationToken,
			),
			SortDesc: desc,
		}
		tupleChanges, continuationToken, err = ds.ReadChanges(context.Background(), storeID, storage.ReadChangesFilter{}, opts)
		if err != nil {
			require.ErrorIs(t, err, storage.ErrNotFound)
			break
		}
		// Not strict equal because there may be less changes than the page size
		require.LessOrEqual(t, len(tupleChanges), pageSize, "len(tupleChanges)=%d, pageSize=%d, tuples=%v", len(tupleChanges), pageSize, tupleChanges)
		seenChanges = append(seenChanges, tupleChanges...)
		if len(tupleChanges) == 0 {
			require.Empty(t, continuationToken)
		} else {
			require.NotEmpty(t, continuationToken)
		}
	}

	return seenChanges
}

// readWithPageSize calls ReadPage. It reads everything from the store, pageSize tuples at a time.
// Along the way, it makes assertions on the tuples seen. It returns all tuples seen, in no particular oder.
func readWithPageSize(t *testing.T, ds storage.OpenFGADatastore, storeID string, pageSize int, filter *openfgav1.TupleKey) []*openfgav1.Tuple {
	t.Helper()
	var (
		tuples            []*openfgav1.Tuple
		seenTuples        []*openfgav1.Tuple
		continuationToken string
		err               error
	)
	for {
		opts := storage.ReadPageOptions{
			Pagination: storage.PaginationOptions{
				PageSize: pageSize,
				From:     continuationToken,
			},
		}
		tuples, continuationToken, err = ds.ReadPage(context.Background(), storeID, filter, opts)
		if err != nil {
			require.ErrorIs(t, err, storage.ErrNotFound)
			break
		}

		seenTuples = append(seenTuples, tuples...)
		if len(continuationToken) != 0 {
			require.Equal(t, len(tuples), pageSize)
		} else {
			require.LessOrEqual(t, len(tuples), pageSize)
			break
		}
	}

	return seenTuples
}

// writeTuplesConcurrently writes two groups of tuples concurrently to expose potential race issues when reading changes.
func writeTuplesConcurrently(t *testing.T, store string, datastore storage.OpenFGADatastore, tupleGroupOne, tupleGroupTwo []*openfgav1.TupleKey) {
	t.Helper()
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		err := datastore.Write(
			ctx,
			store,
			[]*openfgav1.TupleKeyWithoutCondition{},
			tupleGroupOne,
		)
		if err != nil {
			t.Logf("failed to write tuples: %s", err)
		}
		wg.Done()
	}()

	go func() {
		err := datastore.Write(
			ctx,
			store,
			[]*openfgav1.TupleKeyWithoutCondition{},
			tupleGroupTwo,
		)
		if err != nil {
			t.Logf("failed to write tuples: %s", err)
		}
		wg.Done()
	}()

	wg.Wait()
}
