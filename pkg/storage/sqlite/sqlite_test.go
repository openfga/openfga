//go:build integration

package sqlite

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/openfga/openfga/pkg/storage/test"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

func TestSQLiteDatastore(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "sqlite")

	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()
	test.RunAllTests(t, ds)

	// Run tests with a custom large max_tuples_per_write value.
	dsCustom, err := New(uri, sqlcommon.NewConfig(
		sqlcommon.WithMaxTuplesPerWrite(5000),
	))
	require.NoError(t, err)
	defer dsCustom.Close()

	t.Run("WriteTuplesWithMaxTuplesPerWrite", test.WriteTuplesWithMaxTuplesPerWrite(dsCustom, context.Background()))
}

func TestSQLiteDatastoreAfterCloseIsNotReady(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "sqlite")

	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
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
			cfg := sqlcommon.NewConfig()
			ds, err := New(uri, cfg)
			require.NoError(t, err)
			defer ds.Close()

			ctx := context.Background()

			store := "store"
			firstTuple := tupleUtils.NewTupleKey("doc:object_id_1", "relation", "user:user_1")
			secondTuple := tupleUtils.NewTupleKey("doc:object_id_2", "relation", "user:user_2")
			thirdTuple := tupleUtils.NewTupleKey("doc:object_id_3", "relation", "user:user_3")

			err = ds.write(ctx,
				store,
				[]*openfgav1.TupleKeyWithoutCondition{},
				[]*openfgav1.TupleKey{firstTuple},
				storage.TupleWriteOptions{},
				time.Now())
			require.NoError(t, err)

			// Tweak time so that ULID is smaller.
			err = ds.write(ctx,
				store,
				[]*openfgav1.TupleKeyWithoutCondition{},
				[]*openfgav1.TupleKey{secondTuple},
				storage.TupleWriteOptions{},
				time.Now().Add(time.Minute*-1))
			require.NoError(t, err)

			err = ds.write(ctx,
				store,
				[]*openfgav1.TupleKeyWithoutCondition{},
				[]*openfgav1.TupleKey{thirdTuple},
				storage.TupleWriteOptions{},
				time.Now().Add(time.Minute*-2))
			require.NoError(t, err)

			iter, err := ds.Read(
				ctx,
				store,
				storage.ReadFilter{Object: "doc:", Relation: "relation", User: ""},
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
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()

	ctx := context.Background()

	store := "store"
	firstTuple := tupleUtils.NewTupleKey("doc:object_id_1", "relation", "user:user_1")
	secondTuple := tupleUtils.NewTupleKey("doc:object_id_2", "relation", "user:user_2")

	err = ds.write(ctx,
		store,
		[]*openfgav1.TupleKeyWithoutCondition{},
		[]*openfgav1.TupleKey{firstTuple},
		storage.TupleWriteOptions{},
		time.Now())
	require.NoError(t, err)

	// Tweak time so that ULID is smaller.
	err = ds.write(ctx,
		store,
		[]*openfgav1.TupleKeyWithoutCondition{},
		[]*openfgav1.TupleKey{secondTuple},
		storage.TupleWriteOptions{},
		time.Now().Add(time.Minute*-1))
	require.NoError(t, err)

	opts := storage.ReadPageOptions{
		Pagination: storage.NewPaginationOptions(storage.DefaultPageSize, ""),
	}
	tuples, _, err := ds.ReadPage(ctx,
		store,
		storage.ReadFilter{Object: "doc:", Relation: "relation", User: ""},
		opts)
	require.NoError(t, err)

	require.Len(t, tuples, 2)
	// We expect that objectID2 will return first because it has a smaller ulid.
	require.Equal(t, secondTuple, tuples[0].GetKey())
	require.Equal(t, firstTuple, tuples[1].GetKey())
}

func TestSQLiteDatastore_ReadWithUserParts(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "sqlite")
	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()
	ctx := context.Background()

	store := ulid.Make().String()

	// Setup test tuples with different user types
	tuples := []*openfgav1.TupleKey{
		// Direct user
		{Object: "document:doc1", Relation: "viewer", User: "user:alice"},
		// Userset
		{Object: "document:doc2", Relation: "viewer", User: "group:eng#member"},
		// Wildcard
		{Object: "document:doc3", Relation: "viewer", User: "user:*"},
		// Different combinations
		{Object: "document:doc4", Relation: "editor", User: "user:bob"},
		{Object: "document:doc5", Relation: "editor", User: "team:backend#member"},
	}

	// Write test tuples
	err = ds.Write(ctx, store, nil, tuples)
	require.NoError(t, err)

	tests := []struct {
		name         string
		filter       storage.ReadFilter
		expectedObjs []string
	}{
		{
			name:         "filter_by_direct_user",
			filter:       storage.ReadFilter{User: "user:alice"},
			expectedObjs: []string{"document:doc1"},
		},
		{
			name:         "filter_by_userset_user",
			filter:       storage.ReadFilter{User: "group:eng#member"},
			expectedObjs: []string{"document:doc2"},
		},
		{
			name:         "filter_by_wildcard_user",
			filter:       storage.ReadFilter{User: "user:*"},
			expectedObjs: []string{"document:doc3"},
		},
		{
			name:         "filter_by_user_object_type_only",
			filter:       storage.ReadFilter{User: "user:"},
			expectedObjs: []string{"document:doc1", "document:doc3", "document:doc4"},
		},
		{
			name:         "filter_by_user_object_type_team",
			filter:       storage.ReadFilter{User: "team:"},
			expectedObjs: []string{"document:doc5"},
		},
		{
			name: "filter_by_relation_and_user_type",
			filter: storage.ReadFilter{
				Relation: "editor",
				User:     "user:",
			},
			expectedObjs: []string{"document:doc4"},
		},
		{
			name: "no_matches_for_nonexistent_user",
			filter: storage.ReadFilter{
				User: "user:charlie",
			},
			expectedObjs: []string{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			iter, err := ds.Read(ctx, store, test.filter, storage.ReadOptions{})
			require.NoError(t, err)
			defer iter.Stop()

			var actualObjs []string
			for {
				tuple, err := iter.Next(ctx)
				if err != nil {
					if errors.Is(err, storage.ErrIteratorDone) {
						break
					}
					require.NoError(t, err)
				}
				actualObjs = append(actualObjs, tuple.GetKey().GetObject())
			}

			require.ElementsMatch(t, test.expectedObjs, actualObjs)
		})
	}
}

func TestSQLiteDatastore_ReadWithPartialUserFiltering(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "sqlite")
	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()
	ctx := context.Background()

	store := ulid.Make().String()

	// Setup tuples with various user formats for testing partial filtering
	tuples := []*openfgav1.TupleKey{
		{Object: "doc:1", Relation: "viewer", User: "user:alice"},
		{Object: "doc:2", Relation: "viewer", User: "user:bob"},
		{Object: "doc:3", Relation: "viewer", User: "group:admins#member"},
		{Object: "doc:4", Relation: "viewer", User: "group:users#member"},
		{Object: "doc:5", Relation: "viewer", User: "team:eng#lead"},
		{Object: "doc:6", Relation: "viewer", User: "org:acme#employee"},
		{Object: "doc:7", Relation: "viewer", User: "user:*"},
		{Object: "doc:8", Relation: "viewer", User: "group:*"},
	}

	err = ds.Write(ctx, store, nil, tuples)
	require.NoError(t, err)

	tests := []struct {
		name         string
		userFilter   string
		expectedDocs []string
		description  string
	}{
		{
			name:         "filter_by_user_object_type_user",
			userFilter:   "user:",
			expectedDocs: []string{"doc:1", "doc:2", "doc:7"},
			description:  "Should match all users regardless of ID or relation",
		},
		{
			name:         "filter_by_user_object_type_group",
			userFilter:   "group:",
			expectedDocs: []string{"doc:3", "doc:4", "doc:8"},
			description:  "Should match all groups regardless of ID or relation",
		},
		{
			name:         "filter_by_user_object_type_team",
			userFilter:   "team:",
			expectedDocs: []string{"doc:5"},
			description:  "Should match team type",
		},
		{
			name:         "filter_by_specific_user_id",
			userFilter:   "user:alice",
			expectedDocs: []string{"doc:1"},
			description:  "Should match specific user ID",
		},
		{
			name:         "filter_by_specific_group_with_relation",
			userFilter:   "group:admins#member",
			expectedDocs: []string{"doc:3"},
			description:  "Should match specific group with relation",
		},
		{
			name:         "filter_by_wildcard_user",
			userFilter:   "user:*",
			expectedDocs: []string{"doc:7"},
			description:  "Should match wildcard user",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filter := storage.ReadFilter{
				User: test.userFilter,
			}

			iter, err := ds.Read(ctx, store, filter, storage.ReadOptions{})
			require.NoError(t, err)
			defer iter.Stop()

			var actualDocs []string
			for {
				tuple, err := iter.Next(ctx)
				if err != nil {
					if errors.Is(err, storage.ErrIteratorDone) {
						break
					}
					require.NoError(t, err)
				}
				actualDocs = append(actualDocs, tuple.GetKey().GetObject())
			}

			require.ElementsMatch(t, test.expectedDocs, actualDocs, test.description)
		})
	}
}

func TestSQLiteDatastore_ReadWithEmptyUserParts(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "sqlite")
	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()
	ctx := context.Background()

	store := ulid.Make().String()

	// Setup test data
	tuples := []*openfgav1.TupleKey{
		{Object: "doc:1", Relation: "viewer", User: "user:alice"},
		{Object: "doc:2", Relation: "viewer", User: "group:eng#member"},
	}

	err = ds.Write(ctx, store, nil, tuples)
	require.NoError(t, err)

	// Test with empty user - should not add any user filters
	filter := storage.ReadFilter{
		User: "",
	}

	iter, err := ds.Read(ctx, store, filter, storage.ReadOptions{})
	require.NoError(t, err)
	defer iter.Stop()

	var actualObjs []string
	for {
		tuple, err := iter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}
			require.NoError(t, err)
		}
		actualObjs = append(actualObjs, tuple.GetKey().GetObject())
	}

	// Should return all tuples since no user filter is applied
	require.Len(t, actualObjs, 2)
}

func TestSQLiteDatastore_ReadPageWithUserFiltering(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "sqlite")
	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()
	ctx := context.Background()

	store := ulid.Make().String()

	// Create multiple tuples with same user type for pagination testing
	var tuples []*openfgav1.TupleKey
	for i := 0; i < 10; i++ {
		tuples = append(tuples, &openfgav1.TupleKey{
			Object:   "doc:group1",
			Relation: "viewer",
			User:     fmt.Sprintf("user:user%d", i),
		})
		tuples = append(tuples, &openfgav1.TupleKey{
			Object:   "doc:group1",
			Relation: "viewer",
			User:     fmt.Sprintf("group:admin%d", i),
		})
	}

	err = ds.Write(ctx, store, nil, tuples)
	require.NoError(t, err)

	// Test pagination with user type filtering
	filter := storage.ReadFilter{
		Object:   "doc:group1",
		Relation: "viewer",
		User:     "user:",
	}

	readPageOptions := storage.ReadPageOptions{
		Pagination: storage.PaginationOptions{
			PageSize: 5,
		},
	}

	// First page
	tuples1, token, err := ds.ReadPage(ctx, store, filter, readPageOptions)
	require.NoError(t, err)
	require.Len(t, tuples1, 5)
	require.NotEmpty(t, token)

	// All returned tuples should have user type "user"
	for _, tuple := range tuples1 {
		userType, _, _ := tupleUtils.ToUserParts(tuple.GetKey().GetUser())
		require.Equal(t, "user", userType)
	}

	// Second page
	readPageOptions.Pagination.From = token
	tuples2, token2, err := ds.ReadPage(ctx, store, filter, readPageOptions)
	require.NoError(t, err)
	require.Len(t, tuples2, 5)
	require.Empty(t, token2)
}

func TestSQLiteDatastore_ReadCombinedFilters(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "sqlite")
	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()
	ctx := context.Background()

	store := ulid.Make().String()

	// Setup test data with various combinations
	tuples := []*openfgav1.TupleKey{
		{Object: "doc:1", Relation: "viewer", User: "user:alice"},
		{Object: "doc:2", Relation: "editor", User: "user:alice"},
		{Object: "doc:3", Relation: "viewer", User: "user:bob"},
		{Object: "folder:1", Relation: "viewer", User: "user:alice"},
		{Object: "doc:4", Relation: "viewer", User: "group:eng#member"},
	}

	err = ds.Write(ctx, store, nil, tuples)
	require.NoError(t, err)

	tests := []struct {
		name         string
		filter       storage.ReadFilter
		expectedObjs []string
		description  string
	}{
		{
			name: "object_and_user_filter",
			filter: storage.ReadFilter{
				Object: "doc:",
				User:   "user:alice",
			},
			expectedObjs: []string{"doc:1", "doc:2"},
			description:  "Should match docs with specific user",
		},
		{
			name: "relation_and_user_type_filter",
			filter: storage.ReadFilter{
				Relation: "viewer",
				User:     "user:",
			},
			expectedObjs: []string{"doc:1", "doc:3", "folder:1"},
			description:  "Should match viewer relation with user type",
		},
		{
			name: "full_combination_filter",
			filter: storage.ReadFilter{
				Object:   "doc:",
				Relation: "viewer",
				User:     "user:alice",
			},
			expectedObjs: []string{"doc:1"},
			description:  "Should match all filters combined",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			iter, err := ds.Read(ctx, store, test.filter, storage.ReadOptions{})
			require.NoError(t, err)
			defer iter.Stop()

			var actualObjs []string
			for {
				tuple, err := iter.Next(ctx)
				if err != nil {
					if errors.Is(err, storage.ErrIteratorDone) {
						break
					}
					require.NoError(t, err)
				}
				actualObjs = append(actualObjs, tuple.GetKey().GetObject())
			}

			require.ElementsMatch(t, test.expectedObjs, actualObjs, test.description)
		})
	}
}

func TestReadFilterWithConditions(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "sqlite")
	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()
	ctx := context.Background()
	store := ulid.Make().String()

	// Setup test data with various combinations
	tuples := []*openfgav1.TupleKey{
		{Object: "folder:2021-budget", Relation: "owner", User: "user:anne", Condition: &openfgav1.RelationshipCondition{Name: "cond1"}},
		{Object: "folder:2022-budget", Relation: "owner", User: "user:anne"},
	}
	err = ds.Write(ctx, store, nil, tuples)
	require.NoError(t, err)

	// Read: if the tuple has condition and the filter has the same condition the tuple should be returned
	tk := tupleUtils.NewTupleKeyWithCondition("folder:2021-budget", "owner", "user:anne", "cond1", nil)
	filter := storage.ReadFilter{Object: "folder:2021-budget", Relation: "owner", User: "user:anne", Conditions: []string{"cond1"}}
	iter, err := ds.Read(ctx, store, filter, storage.ReadOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	curTuple, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, tk, curTuple.GetKey())
	_, err = iter.Next(ctx)
	require.Error(t, err)

	// Read: if filter has no condition the tuple cannot be returned
	filter = storage.ReadFilter{Object: "folder:2021-budget", Relation: "owner", User: "user:anne", Conditions: []string{""}}
	iter, err = ds.Read(ctx, store, filter, storage.ReadOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	_, err = iter.Next(ctx)
	require.Error(t, err)

	// Read: if filter has a condition but the tuple stored does not have any condition, then the tuple cannot be returned
	filter = storage.ReadFilter{Object: "folder:2022-budget", Relation: "owner", User: "user:anne", Conditions: []string{"cond1"}}
	iter, err = ds.Read(ctx, store, filter, storage.ReadOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	_, err = iter.Next(ctx)
	require.Error(t, err)

	// Read: if filter does not have condition and the tuple stored does not have any condition, then the tuple cannot be returned
	tk = tupleUtils.NewTupleKeyWithCondition("folder:2022-budget", "owner", "user:anne", "", nil)
	filter = storage.ReadFilter{Object: "folder:2022-budget", Relation: "owner", User: "user:anne", Conditions: []string{""}}
	iter, err = ds.Read(ctx, store, filter, storage.ReadOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, tk, curTuple.GetKey())
	_, err = iter.Next(ctx)
	require.Error(t, err)

	// Read: without condition specification in the filter, backward compatibility should be maintained
	tk = tupleUtils.NewTupleKeyWithCondition("folder:2021-budget", "owner", "user:anne", "cond1", nil)
	filter = storage.ReadFilter{Object: "folder:2021-budget", Relation: "owner", User: "user:anne"}
	iter, err = ds.Read(ctx, store, filter, storage.ReadOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, tk, curTuple.GetKey())
	_, err = iter.Next(ctx)
	require.Error(t, err)
}

func TestReadUserTupleFilterWithConditions(t *testing.T) {
	ctx := context.Background()
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "sqlite")

	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()

	stmt := `
  INSERT INTO tuple (
   store, object_type, object_id, relation, 
   user_object_type, user_object_id, user_relation, user_type,
   condition_name, condition_context, ulid, inserted_at
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('subsec'));
 `

	// Insert tuple with condition
	_, err = ds.db.Exec(
		stmt, "store", "folder", "2021-budget", "owner",
		"user", "anne", "", "user",
		"cond1", nil, ulid.Make().String(),
	)
	require.NoError(t, err)

	// Insert tuple without condition
	_, err = ds.db.Exec(
		stmt, "store", "folder", "2022-budget", "owner",
		"user", "anne", "", "user",
		nil, nil, ulid.Make().String(),
	)
	require.NoError(t, err)

	// ReadUserTuple: if the tuple has condition and the filter has the same condition, the tuple should be returned
	tk := tupleUtils.NewTupleKeyWithCondition("folder:2021-budget", "owner", "user:anne", "cond1", nil)
	filter := storage.ReadUserTupleFilter{
		Object:     "folder:2021-budget",
		Relation:   "owner",
		User:       "user:anne",
		Conditions: []string{"cond1"},
	}
	tuple, err := ds.ReadUserTuple(ctx, "store", filter, storage.ReadUserTupleOptions{})
	require.NoError(t, err)
	require.Equal(t, tk, tuple.GetKey())

	// ReadUserTuple: if filter has no condition, the tuple with condition should not be returned
	filter = storage.ReadUserTupleFilter{
		Object:     "folder:2021-budget",
		Relation:   "owner",
		User:       "user:anne",
		Conditions: []string{""},
	}
	_, err = ds.ReadUserTuple(ctx, "store", filter, storage.ReadUserTupleOptions{})
	require.Error(t, err)
	require.ErrorIs(t, err, storage.ErrNotFound)

	// ReadUserTuple: if filter has a condition but the tuple stored does not have any condition, the tuple should not be returned
	filter = storage.ReadUserTupleFilter{
		Object:     "folder:2022-budget",
		Relation:   "owner",
		User:       "user:anne",
		Conditions: []string{"cond1"},
	}
	_, err = ds.ReadUserTuple(ctx, "store", filter, storage.ReadUserTupleOptions{})
	require.Error(t, err)
	require.ErrorIs(t, err, storage.ErrNotFound)

	// ReadUserTuple: if filter does not have condition and the tuple stored does not have any condition, the tuple should be returned
	tk = tupleUtils.NewTupleKeyWithCondition("folder:2022-budget", "owner", "user:anne", "", nil)
	filter = storage.ReadUserTupleFilter{
		Object:     "folder:2022-budget",
		Relation:   "owner",
		User:       "user:anne",
		Conditions: []string{""},
	}
	tuple, err = ds.ReadUserTuple(ctx, "store", filter, storage.ReadUserTupleOptions{})
	require.NoError(t, err)
	require.Equal(t, tk, tuple.GetKey())

	// ReadUserTuple: without condition specification in the filter, backward compatibility should be maintained
	tk = tupleUtils.NewTupleKeyWithCondition("folder:2021-budget", "owner", "user:anne", "cond1", nil)
	filter = storage.ReadUserTupleFilter{
		Object:   "folder:2021-budget",
		Relation: "owner",
		User:     "user:anne",
	}
	tuple, err = ds.ReadUserTuple(ctx, "store", filter, storage.ReadUserTupleOptions{})
	require.NoError(t, err)
	require.Equal(t, tk, tuple.GetKey())
}

func TestReadStartingWithUserFilterWithConditions(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "sqlite")
	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()
	ctx := context.Background()
	store := ulid.Make().String()

	// Setup test data with various combinations
	tuples := []*openfgav1.TupleKey{
		{Object: "folder:2021-budget", Relation: "owner", User: "user:anne", Condition: &openfgav1.RelationshipCondition{Name: "cond1"}},
		{Object: "folder:2023-budget", Relation: "owner", User: "user:anne", Condition: &openfgav1.RelationshipCondition{Name: "cond1"}},
		{Object: "folder:2022-budget", Relation: "owner", User: "user:anne"},
		{Object: "folder:2024-budget", Relation: "owner", User: "user:anne"},
		{Object: "folder:2022-budget", Relation: "owner", User: "user:jack"},
		{Object: "folder:2024-budget", Relation: "owner", User: "user:jack"},
	}
	err = ds.Write(ctx, store, nil, tuples)
	require.NoError(t, err)

	// ReadStartingWithUser: if the tuple has condition and the filter has the same condition the tuple should be returned
	filter := storage.ReadStartingWithUserFilter{
		ObjectType: "folder",
		Relation:   "owner",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "user:anne"},
		},
		Conditions: []string{"cond1"},
	}
	iter, err := ds.ReadStartingWithUser(ctx, store, filter, storage.ReadStartingWithUserOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	curTuple, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"folder:2021-budget", "folder:2023-budget"}, curTuple.GetKey().GetObject())
	require.Equal(t, "cond1", curTuple.GetKey().GetCondition().GetName())
	require.Equal(t, "user:anne", curTuple.GetKey().GetUser())
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"folder:2021-budget", "folder:2023-budget"}, curTuple.GetKey().GetObject())
	require.Equal(t, "cond1", curTuple.GetKey().GetCondition().GetName())
	require.Equal(t, "user:anne", curTuple.GetKey().GetUser())
	_, err = iter.Next(ctx)
	require.Error(t, err)

	// ReadStartingWithUser: if filter has a condition but the tuple stored does not have any condition, then the tuple cannot be returned
	filter = storage.ReadStartingWithUserFilter{
		ObjectType: "folder",
		Relation:   "owner",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "user:jack"},
		},
		Conditions: []string{"cond1"},
	}
	iter, err = ds.ReadStartingWithUser(ctx, store, filter, storage.ReadStartingWithUserOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	_, err = iter.Next(ctx)
	require.Error(t, err)

	// ReadStartingWithUser: if filter does not have condition and the tuple stored does not have any condition, then the tuple cannot be returned
	filter = storage.ReadStartingWithUserFilter{
		ObjectType: "folder",
		Relation:   "owner",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "user:anne"},
		},
		Conditions: []string{""},
	}
	iter, err = ds.ReadStartingWithUser(ctx, store, filter, storage.ReadStartingWithUserOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"folder:2022-budget", "folder:2024-budget"}, curTuple.GetKey().GetObject())
	require.Empty(t, curTuple.GetKey().GetCondition().GetName())
	require.Equal(t, "user:anne", curTuple.GetKey().GetUser())
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"folder:2022-budget", "folder:2024-budget"}, curTuple.GetKey().GetObject())
	require.Empty(t, curTuple.GetKey().GetCondition().GetName())
	require.Equal(t, "user:anne", curTuple.GetKey().GetUser())
	_, err = iter.Next(ctx)
	require.Error(t, err)

	// ReadStartingWithUser: without condition specification in the filter, backward compatibility should be maintained
	filter = storage.ReadStartingWithUserFilter{
		ObjectType: "folder",
		Relation:   "owner",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "user:anne"},
		},
	}
	iter, err = ds.ReadStartingWithUser(ctx, store, filter, storage.ReadStartingWithUserOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"folder:2022-budget", "folder:2024-budget", "folder:2021-budget", "folder:2023-budget"}, curTuple.GetKey().GetObject())
	require.Equal(t, "user:anne", curTuple.GetKey().GetUser())
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"folder:2022-budget", "folder:2024-budget", "folder:2021-budget", "folder:2023-budget"}, curTuple.GetKey().GetObject())
	require.Equal(t, "user:anne", curTuple.GetKey().GetUser())
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"folder:2022-budget", "folder:2024-budget", "folder:2021-budget", "folder:2023-budget"}, curTuple.GetKey().GetObject())
	require.Equal(t, "user:anne", curTuple.GetKey().GetUser())
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"folder:2022-budget", "folder:2024-budget", "folder:2021-budget", "folder:2023-budget"}, curTuple.GetKey().GetObject())
	require.Equal(t, "user:anne", curTuple.GetKey().GetUser())
	_, err = iter.Next(ctx)
	require.Error(t, err)
}

func TestReadUsersetTuplesFilterWithConditions(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "sqlite")
	uri := testDatastore.GetConnectionURI(true)
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()
	ctx := context.Background()
	store := ulid.Make().String()

	// Setup test data with various combinations
	tuples := []*openfgav1.TupleKey{
		{Object: "document:2021-budget", Relation: "member", User: "group:g1#member", Condition: &openfgav1.RelationshipCondition{Name: "cond1"}},
		{Object: "document:2021-budget", Relation: "member", User: "group:g2#member", Condition: &openfgav1.RelationshipCondition{Name: "cond1"}},
		{Object: "document:2021-budget", Relation: "member", User: "group:g3#member"},
		{Object: "document:2021-budget", Relation: "member", User: "group:g4#member"},
		{Object: "document:2022-budget", Relation: "member", User: "group:g2#member"},
		{Object: "document:2022-budget", Relation: "member", User: "group:g1#member"},
	}
	err = ds.Write(ctx, store, nil, tuples)
	require.NoError(t, err)

	// ReadUsersetTuples: if the tuple has condition and the filter has the same condition the tuple should be returned
	filter := storage.ReadUsersetTuplesFilter{
		Object:   "document:2021-budget",
		Relation: "member",
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			{
				Type:               "group",
				RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"},
			},
		},
		Conditions: []string{"cond1"},
	}
	iter, err := ds.ReadUsersetTuples(ctx, store, filter, storage.ReadUsersetTuplesOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	curTuple, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"group:g1#member", "group:g2#member"}, curTuple.GetKey().GetUser())
	require.Equal(t, "cond1", curTuple.GetKey().GetCondition().GetName())
	require.Equal(t, "document:2021-budget", curTuple.GetKey().GetObject())
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"group:g1#member", "group:g2#member"}, curTuple.GetKey().GetUser())
	require.Equal(t, "cond1", curTuple.GetKey().GetCondition().GetName())
	require.Equal(t, "document:2021-budget", curTuple.GetKey().GetObject())
	_, err = iter.Next(ctx)
	require.Error(t, err)

	// ReadUsersetTuples: if filter has a condition but the tuple stored does not have any condition, then the tuple cannot be returned
	filter = storage.ReadUsersetTuplesFilter{
		Object:   "document:2022-budget",
		Relation: "member",
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			{
				Type:               "group",
				RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"},
			},
		},
		Conditions: []string{"cond1"},
	}
	iter, err = ds.ReadUsersetTuples(ctx, store, filter, storage.ReadUsersetTuplesOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	_, err = iter.Next(ctx)
	require.Error(t, err)

	// ReadUsersetTuples: if filter does not have condition and the tuple stored does not have any condition, then the tuple cannot be returned
	filter = storage.ReadUsersetTuplesFilter{
		Object:   "document:2021-budget",
		Relation: "member",
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			{
				Type:               "group",
				RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"},
			},
		},
		Conditions: []string{""},
	}
	iter, err = ds.ReadUsersetTuples(ctx, store, filter, storage.ReadUsersetTuplesOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"group:g3#member", "group:g4#member"}, curTuple.GetKey().GetUser())
	require.Empty(t, curTuple.GetKey().GetCondition().GetName())
	require.Equal(t, "document:2021-budget", curTuple.GetKey().GetObject())
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"group:g3#member", "group:g4#member"}, curTuple.GetKey().GetUser())
	require.Empty(t, curTuple.GetKey().GetCondition().GetName())
	require.Equal(t, "document:2021-budget", curTuple.GetKey().GetObject())
	_, err = iter.Next(ctx)
	require.Error(t, err)

	// ReadUsersetTuples: without condition specification in the filter, backward compatibility should be maintained
	filter = storage.ReadUsersetTuplesFilter{
		Object:   "document:2021-budget",
		Relation: "member",
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			{
				Type:               "group",
				RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"},
			},
		},
	}
	iter, err = ds.ReadUsersetTuples(ctx, store, filter, storage.ReadUsersetTuplesOptions{})
	require.NoError(t, err)
	defer iter.Stop()
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"group:g3#member", "group:g4#member", "group:g1#member", "group:g2#member"}, curTuple.GetKey().GetUser())
	require.Equal(t, "document:2021-budget", curTuple.GetKey().GetObject())
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"group:g3#member", "group:g4#member", "group:g1#member", "group:g2#member"}, curTuple.GetKey().GetUser())
	require.Equal(t, "document:2021-budget", curTuple.GetKey().GetObject())
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"group:g3#member", "group:g4#member", "group:g1#member", "group:g2#member"}, curTuple.GetKey().GetUser())
	require.Equal(t, "document:2021-budget", curTuple.GetKey().GetObject())
	curTuple, err = iter.Next(ctx)
	require.NoError(t, err)
	require.Contains(t, []string{"group:g3#member", "group:g4#member", "group:g1#member", "group:g2#member"}, curTuple.GetKey().GetUser())
	require.Equal(t, "document:2021-budget", curTuple.GetKey().GetObject())
	_, err = iter.Next(ctx)
	require.Error(t, err)
}
