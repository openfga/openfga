package test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func ReadChangesTest(t *testing.T, dbTester DatastoreTester[storage.OpenFGADatastore]) {

	require := require.New(t)
	ctx := context.Background()

	datastore, err := dbTester.New()
	require.NoError(err)

	t.Run("read changes with continuation token", func(t *testing.T) {
		store := testutils.CreateRandomString(10)

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

		err := datastore.Write(ctx, store, nil, []*openfgapb.TupleKey{tk1, tk2})
		if err != nil {
			t.Fatal(err)
		}

		changes, continuationToken, err := datastore.ReadChanges(ctx, store, "", storage.PaginationOptions{PageSize: 1}, 0)
		if err != nil {
			t.Fatalf("expected no error but got '%v'", err)
		}

		if string(continuationToken) == "" {
			t.Error("expected non-empty token")
		}

		expectedChanges := []*openfgapb.TupleChange{
			{
				TupleKey:  tk1,
				Operation: openfgapb.TupleOperation_TUPLE_OPERATION_WRITE,
			},
		}

		if diff := cmp.Diff(expectedChanges, changes, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-got +want):\n%s", diff)
		}

		changes, continuationToken, err = datastore.ReadChanges(ctx, store, "", storage.PaginationOptions{
			PageSize: 2,
			From:     string(continuationToken),
		},
			0,
		)
		if err != nil {
			t.Errorf("expected no error but got '%v'", err)
		}

		if string(continuationToken) == "" {
			t.Error("expected non-empty token")
		}

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
		store := testutils.CreateRandomString(10)

		_, _, err := datastore.ReadChanges(ctx, store, "", storage.PaginationOptions{PageSize: storage.DefaultPageSize}, 0)
		if !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("expected '%v', got '%v'", storage.ErrNotFound, err)
		}
	})

	t.Run("read changes with horizon offset should return not found (no changes)", func(t *testing.T) {
		store := testutils.CreateRandomString(10)

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

		err := datastore.Write(ctx, store, nil, []*openfgapb.TupleKey{tk1, tk2})
		if err != nil {
			t.Fatal(err)
		}

		_, _, err = datastore.ReadChanges(ctx, store, "", storage.PaginationOptions{PageSize: storage.DefaultPageSize}, 1*time.Minute)
		if !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("expected '%v', got '%v'", storage.ErrNotFound, err)
		}
	})

	t.Run("read changes with non-empty object type should only read that object type", func(t *testing.T) {
		store := testutils.CreateRandomString(10)

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

		err := datastore.Write(ctx, store, nil, []*openfgapb.TupleKey{tk1, tk2})
		if err != nil {
			t.Fatal(err)
		}

		changes, continuationToken, err := datastore.ReadChanges(ctx, store, "folder", storage.PaginationOptions{PageSize: storage.DefaultPageSize}, 0)
		if err != nil {
			t.Errorf("expected no error but got '%v'", err)
		}

		if len(continuationToken) == 0 {
			t.Errorf("expected empty token but got '%s'", continuationToken)
		}

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

func TupleWritingAndReadingTest(t *testing.T, dbTester DatastoreTester[storage.OpenFGADatastore]) {

	require := require.New(t)
	ctx := context.Background()

	datastore, err := dbTester.New()
	require.NoError(err)

	t.Run("deletes would succeed and write would fail, fails and introduces no changes", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
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
		if err := datastore.Write(ctx, store, nil, tks); err != nil {
			t.Fatal(err)
		}
		// Try to delete tks[0,1], and at the same time write tks[2]. It should fail with expectedError.
		if err := datastore.Write(ctx, store, []*openfgapb.TupleKey{tks[0], tks[1]}, []*openfgapb.TupleKey{tks[2]}); err.Error() != expectedError.Error() {
			t.Fatalf("got '%v', want '%v'", err, expectedError)
		}
		tuples, _, err := datastore.ReadByStore(ctx, store, storage.PaginationOptions{PageSize: 50})
		if err != nil {
			t.Fatal(err)
		}
		if len(tuples) != len(tks) {
			t.Fatalf("got '%d', want '%d'", len(tuples), len(tks))
		}
	})

	t.Run("delete fails if the tuple does not exist", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		tk := &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}
		expectedError := storage.InvalidWriteInputError(tk, openfgapb.TupleOperation_TUPLE_OPERATION_DELETE)

		if err := datastore.Write(ctx, store, []*openfgapb.TupleKey{tk}, nil); err.Error() != expectedError.Error() {
			t.Fatalf("got '%v', want '%v'", err, expectedError)
		}
	})

	t.Run("deleting a tuple which exists succeeds", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		tk := &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}

		// Write
		if err := datastore.Write(ctx, store, nil, []*openfgapb.TupleKey{tk}); err != nil {
			t.Fatal(err)
		}
		// Then delete
		if err := datastore.Write(ctx, store, []*openfgapb.TupleKey{tk}, nil); err != nil {
			t.Fatal(err)
		}
		// Ensure it is not there
		if _, err := datastore.ReadUserTuple(ctx, store, tk); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("got '%v', want '%v'", err, storage.ErrNotFound)
		}
	})

	t.Run("inserting a tuple twice fails", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		tk := &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}
		expectedError := storage.InvalidWriteInputError(tk, openfgapb.TupleOperation_TUPLE_OPERATION_WRITE)

		// First write should succeed.
		if err := datastore.Write(ctx, store, nil, []*openfgapb.TupleKey{tk}); err != nil {
			t.Fatal(err)
		}
		// Second write of the same tuple should fail.
		if err := datastore.Write(ctx, store, nil, []*openfgapb.TupleKey{tk}); err.Error() != expectedError.Error() {
			t.Fatalf("got '%v', want '%v'", err, expectedError)
		}
	})

	t.Run("reading a tuple that exists succeeds", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		tuple := &openfgapb.Tuple{Key: &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}}

		if err := datastore.Write(ctx, store, nil, []*openfgapb.TupleKey{tuple.Key}); err != nil {
			t.Fatal(err)
		}
		gotTuple, err := datastore.ReadUserTuple(ctx, store, tuple.Key)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(gotTuple, tuple, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-got +want):\n%s", diff)
		}
	})

	t.Run("reading a tuple that does not exist returns not found", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
		tk := &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}

		if _, err := datastore.ReadUserTuple(ctx, store, tk); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("got '%v', want '%v'", err, storage.ErrNotFound)
		}
	})

	t.Run("reading userset tuples that exists succeeds", func(t *testing.T) {
		store := testutils.CreateRandomString(10)
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

		if err := datastore.Write(ctx, store, nil, tks); err != nil {
			t.Fatal(err)
		}
		gotTuples, err := datastore.ReadUsersetTuples(ctx, store, &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner"})
		if err != nil {
			t.Fatal(err)
		}
		defer gotTuples.Stop()

		gotTupleKeys := []*openfgapb.TupleKey{}

		// We should find the first two tupleKeys
		for i := 0; i < 2; i++ {
			gotTuple, err := gotTuples.Next()
			if err != nil {
				t.Fatal(err)
			}

			gotTupleKeys = append(gotTupleKeys, gotTuple.Key)
		}

		// Then the iterator should run out
		if _, err := gotTuples.Next(); !errors.Is(err, storage.TupleIteratorDone) {
			t.Fatalf("got '%v', want '%v'", err, storage.TupleIteratorDone)
		}

		if diff := cmp.Diff(gotTupleKeys, tks[:2], cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-got +want):\n%s", diff)
		}
	})

	t.Run("reading userset tuples that don't exist should an empty iterator", func(t *testing.T) {
		store := testutils.CreateRandomString(10)

		gotTuples, err := datastore.ReadUsersetTuples(ctx, store, &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner"})
		if err != nil {
			t.Fatal(err)
		}
		defer gotTuples.Stop()

		if _, err := gotTuples.Next(); !errors.Is(err, storage.TupleIteratorDone) {
			t.Fatalf("got '%v', want '%v'", err, storage.TupleIteratorDone)
		}
	})
}

func TuplePaginationOptionsTest(t *testing.T, dbTester DatastoreTester[storage.OpenFGADatastore]) {

	require := require.New(t)
	ctx := context.Background()

	datastore, err := dbTester.New()
	require.NoError(err)

	store := testutils.CreateRandomString(10)
	tk0 := &openfgapb.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}
	tk1 := &openfgapb.TupleKey{Object: "doc:readme", Relation: "viewer", User: "11"}

	if err := datastore.Write(ctx, store, nil, []*openfgapb.TupleKey{tk0, tk1}); err != nil {
		t.Fatal(err)
	}

	t.Run("readPage pagination works properly", func(t *testing.T) {
		tuples0, contToken0, err := datastore.ReadPage(ctx, store, &openfgapb.TupleKey{Object: "doc:readme"}, storage.PaginationOptions{PageSize: 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(tuples0) != 1 {
			t.Fatalf("got '%d', want '1'", len(tuples0))
		}
		if diff := cmp.Diff(tuples0[0].Key, tk0, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-got +want):\n%s", diff)
		}
		require.NotEmpty(string(contToken0))

		tuples1, contToken1, err := datastore.ReadPage(ctx, store, &openfgapb.TupleKey{Object: "doc:readme"}, storage.PaginationOptions{PageSize: 1, From: string(contToken0)})
		if err != nil {
			t.Fatal(err)
		}
		if len(tuples1) != 1 {
			t.Fatalf("got '%d', want '1'", len(tuples0))
		}
		if diff := cmp.Diff(tuples1[0].Key, tk1, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-got +want):\n%s", diff)
		}
		if len(contToken1) != 0 {
			t.Fatalf("got '%s', want empty", string(contToken1))
		}
	})

	t.Run("reading a page completely does not return a continuation token", func(t *testing.T) {
		tuples, contToken, err := datastore.ReadPage(ctx, store, &openfgapb.TupleKey{Object: "doc:readme"}, storage.PaginationOptions{PageSize: 2})
		if err != nil {
			t.Fatal(err)
		}
		if len(tuples) != 2 {
			t.Fatalf("got '%d', want '2'", len(tuples))
		}
		if len(contToken) != 0 {
			t.Fatalf("got '%s', want empty", string(contToken))
		}
	})

	t.Run("reading a page partially returns a continuation token", func(t *testing.T) {
		tuples, contToken, err := datastore.ReadPage(ctx, store, &openfgapb.TupleKey{Object: "doc:readme"}, storage.PaginationOptions{PageSize: 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(tuples) != 1 {
			t.Fatalf("got '%d', want '1'", len(tuples))
		}
		if len(contToken) == 0 {
			t.Fatalf("got '%s', want empty", string(contToken))
		}
	})

	t.Run("readByStore pagination works properly", func(t *testing.T) {
		tuple0, contToken0, err := datastore.ReadByStore(ctx, store, storage.PaginationOptions{PageSize: 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(tuple0) != 1 {
			t.Fatalf("expected one tuple, got %d", len(tuple0))
		}
		if diff := cmp.Diff(tuple0[0].Key, tk0, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-got +want):\n%s", diff)
		}
		if len(contToken0) == 0 {
			t.Fatal("got empty, want non-empty")
		}

		tuple1, contToken1, err := datastore.ReadByStore(ctx, store, storage.PaginationOptions{PageSize: 1, From: string(contToken0)})
		if err != nil {
			t.Fatal(err)
		}
		if len(tuple1) != 1 {
			t.Fatalf("expected one tuple, got %d", len(tuple1))
		}
		if diff := cmp.Diff(tuple1[0].Key, tk1, cmpOpts...); diff != "" {
			t.Fatalf("mismatch (-got +want):\n%s", diff)
		}
		if len(contToken1) != 0 {
			t.Fatalf("got '%s', want empty", string(contToken1))
		}
	})

	t.Run("reading by store completely does not return a continuation token", func(t *testing.T) {
		tuples, contToken, err := datastore.ReadByStore(ctx, store, storage.PaginationOptions{PageSize: 2})
		if err != nil {
			t.Fatal(err)
		}
		if len(tuples) != 2 {
			t.Fatalf("got '%d', want '2'", len(tuples))
		}
		if len(contToken) != 0 {
			t.Fatalf("got '%s', want empty", string(contToken))
		}
	})

	t.Run("reading by store partially returns a continuation token", func(t *testing.T) {
		tuples, contToken, err := datastore.ReadByStore(ctx, store, storage.PaginationOptions{PageSize: 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(tuples) != 1 {
			t.Fatalf("got '%d', want '2'", len(tuples))
		}
		if len(contToken) == 0 {
			t.Fatalf("got empty, want non-empty")
		}
	})
}
