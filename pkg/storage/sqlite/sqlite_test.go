package sqlite

import (
	"context"
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
	"github.com/openfga/openfga/pkg/tuple"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)
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
			firstTuple := tuple.NewTupleKey("doc:object_id_1", "relation", "user:user_1")
			secondTuple := tuple.NewTupleKey("doc:object_id_2", "relation", "user:user_2")
			thirdTuple := tuple.NewTupleKey("doc:object_id_3", "relation", "user:user_3")

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
	cfg := sqlcommon.NewConfig()
	ds, err := New(uri, cfg)
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
		tuple.NewTupleKey("doc:", "relation", ""),
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
		tupleKey     *openfgav1.TupleKey
		expectedObjs []string
	}{
		{
			name: "filter_by_direct_user",
			tupleKey: &openfgav1.TupleKey{
				User: "user:alice",
			},
			expectedObjs: []string{"document:doc1"},
		},
		{
			name: "filter_by_userset_user",
			tupleKey: &openfgav1.TupleKey{
				User: "group:eng#member",
			},
			expectedObjs: []string{"document:doc2"},
		},
		{
			name: "filter_by_wildcard_user",
			tupleKey: &openfgav1.TupleKey{
				User: "user:*",
			},
			expectedObjs: []string{"document:doc3"},
		},
		{
			name: "filter_by_user_object_type_only",
			tupleKey: &openfgav1.TupleKey{
				User: "user:",
			},
			expectedObjs: []string{"document:doc1", "document:doc3", "document:doc4"},
		},
		{
			name: "filter_by_user_object_type_team",
			tupleKey: &openfgav1.TupleKey{
				User: "team:",
			},
			expectedObjs: []string{"document:doc5"},
		},
		{
			name: "filter_by_relation_and_user_type",
			tupleKey: &openfgav1.TupleKey{
				Relation: "editor",
				User:     "user:",
			},
			expectedObjs: []string{"document:doc4"},
		},
		{
			name: "no_matches_for_nonexistent_user",
			tupleKey: &openfgav1.TupleKey{
				User: "user:charlie",
			},
			expectedObjs: []string{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			iter, err := ds.Read(ctx, store, test.tupleKey, storage.ReadOptions{})
			require.NoError(t, err)
			defer iter.Stop()

			var actualObjs []string
			for {
				tuple, err := iter.Next(ctx)
				if err != nil {
					if err == storage.ErrIteratorDone {
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
			tupleKey := &openfgav1.TupleKey{
				User: test.userFilter,
			}

			iter, err := ds.Read(ctx, store, tupleKey, storage.ReadOptions{})
			require.NoError(t, err)
			defer iter.Stop()

			var actualDocs []string
			for {
				tuple, err := iter.Next(ctx)
				if err != nil {
					if err == storage.ErrIteratorDone {
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
	tupleKey := &openfgav1.TupleKey{
		User: "",
	}

	iter, err := ds.Read(ctx, store, tupleKey, storage.ReadOptions{})
	require.NoError(t, err)
	defer iter.Stop()

	var actualObjs []string
	for {
		tuple, err := iter.Next(ctx)
		if err != nil {
			if err == storage.ErrIteratorDone {
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
	tupleKey := &openfgav1.TupleKey{
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
	tuples1, token, err := ds.ReadPage(ctx, store, tupleKey, readPageOptions)
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
	tuples2, token2, err := ds.ReadPage(ctx, store, tupleKey, readPageOptions)
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
		tupleKey     *openfgav1.TupleKey
		expectedObjs []string
		description  string
	}{
		{
			name: "object_and_user_filter",
			tupleKey: &openfgav1.TupleKey{
				Object: "doc:",
				User:   "user:alice",
			},
			expectedObjs: []string{"doc:1", "doc:2"},
			description:  "Should match docs with specific user",
		},
		{
			name: "relation_and_user_type_filter",
			tupleKey: &openfgav1.TupleKey{
				Relation: "viewer",
				User:     "user:",
			},
			expectedObjs: []string{"doc:1", "doc:3", "folder:1"},
			description:  "Should match viewer relation with user type",
		},
		{
			name: "full_combination_filter",
			tupleKey: &openfgav1.TupleKey{
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
			iter, err := ds.Read(ctx, store, test.tupleKey, storage.ReadOptions{})
			require.NoError(t, err)
			defer iter.Stop()

			var actualObjs []string
			for {
				tuple, err := iter.Next(ctx)
				if err != nil {
					if err == storage.ErrIteratorDone {
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
