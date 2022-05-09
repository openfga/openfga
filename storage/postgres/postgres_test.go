package postgres

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	pkgTestutils "github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/storage"
	"github.com/openfga/openfga/storage/postgres/testutils"
	"github.com/go-errors/errors"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/kelseyhightower/envconfig"
	"go.buf.build/openfga/go/openfga/api/openfga"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/api/iterator"
)

var (
	ctx      context.Context
	postgres *Postgres
)

var (
	cmpOpts = []cmp.Option{
		cmpopts.IgnoreUnexported(openfga.TupleKey{}, openfga.Tuple{}, openfga.TupleChange{}, openfga.Assertion{}),
		cmpopts.IgnoreFields(openfga.Tuple{}, "Timestamp"),
		cmpopts.IgnoreFields(openfga.TupleChange{}, "Timestamp"),
	}
)

type testConfig struct {
	PostgresURI string `envconfig:"POSTGRES_URI" default:"postgres://postgres:password@127.0.0.1:5432/postgres"`
}

func TestMain(m *testing.M) {
	var config testConfig
	if err := envconfig.Process("TEST_CONFIG", &config); err != nil {
		fmt.Printf("error reading config: %v\n", err)
		os.Exit(1)
	}

	ctx = context.Background()
	pool, err := pgxpool.Connect(ctx, config.PostgresURI)
	if err != nil {
		fmt.Printf("failed to create Postgres pool: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := testutils.CreatePostgresTestTables(ctx, pool); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	postgres = New(pool, telemetry.NewNoopTracer(), logger.NewNoopLogger())

	// Because we want different store names
	rand.Seed(time.Now().UnixNano())

	os.Exit(m.Run())
}

func TestTupleWritingAndReading(t *testing.T) {
	t.Run("inserting the same tuple twice fails and introduces no changes", func(t *testing.T) {
		store := pkgTestutils.CreateRandomString(10)
		tk := &openfga.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}
		expectedError := storage.InvalidWriteInputError(tk, openfga.TupleOperation_WRITE)
		if err := postgres.Write(ctx, store, nil, []*openfga.TupleKey{tk, tk}); err.Error() != expectedError.Error() {
			t.Errorf("got '%v', want '%v'", err, expectedError)
		}
		// Ensure that nothing got written
		if _, err := postgres.ReadUserTuple(ctx, store, tk); !errors.Is(err, storage.NotFound) {
			t.Errorf("got '%v', want '%v'", err, storage.NotFound)
		}
	})

	t.Run("deletes would succeed and write would fail, fails and introduces no changes", func(t *testing.T) {
		store := pkgTestutils.CreateRandomString(10)
		tks := []*openfga.TupleKey{
			{
				Object:   "doc:readme",
				Relation: "owner",
				User:     "org:auth0#member",
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
		expectedError := storage.InvalidWriteInputError(tks[2], openfga.TupleOperation_WRITE)

		// Write tks
		if err := postgres.Write(ctx, store, nil, tks); err != nil {
			t.Error(err)
		}
		// Try to delete tks[0,1], and at the same time write tks[2]. It should fail with expectedError.
		if err := postgres.Write(ctx, store, []*openfga.TupleKey{tks[0], tks[1]}, []*openfga.TupleKey{tks[2]}); err.Error() != expectedError.Error() {
			t.Errorf("got '%v', want '%v'", err, expectedError)
		}
		tuples, _, err := postgres.ReadByStore(ctx, store, storage.PaginationOptions{PageSize: 50})
		if err != nil {
			t.Error(err)
		}
		if len(tuples) != len(tks) {
			t.Errorf("got '%d', want '%d'", len(tuples), len(tks))
		}
	})

	t.Run("delete fails if the tuple does not exist", func(t *testing.T) {
		store := pkgTestutils.CreateRandomString(10)
		tk := &openfga.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}
		expectedError := storage.InvalidWriteInputError(tk, openfga.TupleOperation_DELETE)

		if err := postgres.Write(ctx, store, []*openfga.TupleKey{tk}, nil); err.Error() != expectedError.Error() {
			t.Errorf("got '%v', want '%v'", err, expectedError)
		}
	})

	t.Run("deleting a tuple which exists succeeds", func(t *testing.T) {
		store := pkgTestutils.CreateRandomString(10)
		tk := &openfga.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}

		// Write
		if err := postgres.Write(ctx, store, nil, []*openfga.TupleKey{tk}); err != nil {
			t.Error(err)
		}
		// Then delete
		if err := postgres.Write(ctx, store, []*openfga.TupleKey{tk}, nil); err != nil {
			t.Error(err)
		}
		// Ensure it is not there
		if _, err := postgres.ReadUserTuple(ctx, store, tk); !errors.Is(err, storage.NotFound) {
			t.Errorf("got '%v', want '%v'", err, storage.NotFound)
		}
	})

	t.Run("inserting a tuple twice fails", func(t *testing.T) {
		store := pkgTestutils.CreateRandomString(10)
		tk := &openfga.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}
		expectedError := storage.InvalidWriteInputError(tk, openfga.TupleOperation_WRITE)

		// First write should succeed.
		if err := postgres.Write(ctx, store, nil, []*openfga.TupleKey{tk}); err != nil {
			t.Error(err)
		}
		// Second write of the same tuple should fail.
		if err := postgres.Write(ctx, store, nil, []*openfga.TupleKey{tk}); err.Error() != expectedError.Error() {
			t.Errorf("got '%v', want '%v'", err, expectedError)
		}
	})

	t.Run("reading a tuple that exists succeeds", func(t *testing.T) {
		store := pkgTestutils.CreateRandomString(10)
		tuple := &openfga.Tuple{Key: &openfga.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}}

		if err := postgres.Write(ctx, store, nil, []*openfga.TupleKey{tuple.Key}); err != nil {
			t.Error(err)
		}
		gotTuple, err := postgres.ReadUserTuple(ctx, store, tuple.Key)
		if err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(gotTuple, tuple, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-got +want):\n%s", diff)
		}
	})

	t.Run("reading a tuple that does not exist returns not found", func(t *testing.T) {
		store := pkgTestutils.CreateRandomString(10)
		tk := &openfga.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}

		if _, err := postgres.ReadUserTuple(ctx, store, tk); !errors.Is(err, storage.NotFound) {
			t.Errorf("got '%v', want '%v'", err, storage.NotFound)
		}
	})

	t.Run("reading userset tuples that exists succeeds", func(t *testing.T) {
		store := pkgTestutils.CreateRandomString(10)
		tks := []*openfga.TupleKey{
			{
				Object:   "doc:readme",
				Relation: "owner",
				User:     "org:auth0#member",
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

		if err := postgres.Write(ctx, store, nil, tks); err != nil {
			t.Error(err)
		}
		gotTuples, err := postgres.ReadUsersetTuples(ctx, store, &openfga.TupleKey{Object: "doc:readme", Relation: "owner"})
		if err != nil {
			t.Error(err)
		}

		// We should find the first two tupleKeys
		for i := 0; i < 2; i++ {
			gotTuple, err := gotTuples.Next()
			if err != nil {
				t.Error(err)
			}
			if diff := cmp.Diff(gotTuple.Key, tks[i], cmpOpts...); diff != "" {
				t.Errorf("mismatch (-got +want):\n%s", diff)
			}
		}
		// Then the iterator should run out
		if _, err := gotTuples.Next(); !errors.Is(err, iterator.Done) {
			t.Errorf("got '%v', want '%v'", err, iterator.Done)
		}
	})

	t.Run("reading userset tuples that don't exist should an empty iterator", func(t *testing.T) {
		store := pkgTestutils.CreateRandomString(10)

		gotTuples, err := postgres.ReadUsersetTuples(ctx, store, &openfga.TupleKey{Object: "doc:readme", Relation: "owner"})
		if err != nil {
			t.Error(err)
		}
		if _, err := gotTuples.Next(); !errors.Is(err, iterator.Done) {
			t.Errorf("got '%v', want '%v'", err, iterator.Done)
		}
	})
}

func TestTuplePaginationOptions(t *testing.T) {
	store := pkgTestutils.CreateRandomString(10)
	tk0 := &openfga.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"}
	tk1 := &openfga.TupleKey{Object: "doc:readme", Relation: "viewer", User: "11"}

	if err := postgres.Write(ctx, store, nil, []*openfga.TupleKey{tk0, tk1}); err != nil {
		t.Fatal(err)
	}

	t.Run("readPage pagination works properly", func(t *testing.T) {
		tuples0, contToken0, err := postgres.ReadPage(ctx, store, &openfga.TupleKey{Object: "doc:readme"}, storage.PaginationOptions{PageSize: 1})
		if err != nil {
			t.Error(err)
		}
		if len(tuples0) != 1 {
			t.Errorf("got '%d', want '1'", len(tuples0))
		}
		if diff := cmp.Diff(tuples0[0].Key, tk0, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-got +want):\n%s", diff)
		}
		if len(contToken0) == 0 {
			t.Errorf("got '%s', want empty", string(contToken0))
		}

		tuples1, contToken1, err := postgres.ReadPage(ctx, store, &openfga.TupleKey{Object: "doc:readme"}, storage.PaginationOptions{PageSize: 1, From: string(contToken0)})
		if err != nil {
			t.Fatal(err)
		}
		if len(tuples1) != 1 {
			t.Errorf("got '%d', want '1'", len(tuples0))
		}
		if diff := cmp.Diff(tuples1[0].Key, tk1, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-got +want):\n%s", diff)
		}
		if len(contToken1) != 0 {
			t.Errorf("got '%s', want empty", string(contToken1))
		}
	})

	t.Run("reading a page completely does not return a continuation token", func(t *testing.T) {
		tuples, contToken, err := postgres.ReadPage(ctx, store, &openfga.TupleKey{Object: "doc:readme"}, storage.PaginationOptions{PageSize: 2})
		if err != nil {
			t.Error(err)
		}
		if len(tuples) != 2 {
			t.Errorf("got '%d', want '2'", len(tuples))
		}
		if len(contToken) != 0 {
			t.Errorf("got '%s', want empty", string(contToken))
		}
	})

	t.Run("reading a page partially returns a continuation token", func(t *testing.T) {
		tuples, contToken, err := postgres.ReadPage(ctx, store, &openfga.TupleKey{Object: "doc:readme"}, storage.PaginationOptions{PageSize: 1})
		if err != nil {
			t.Error(err)
		}
		if len(tuples) != 1 {
			t.Errorf("got '%d', want '1'", len(tuples))
		}
		if len(contToken) == 0 {
			t.Errorf("got '%s', want empty", string(contToken))
		}
	})

	t.Run("readByStore pagination works properly", func(t *testing.T) {
		tuple0, contToken0, err := postgres.ReadByStore(ctx, store, storage.PaginationOptions{PageSize: 1})
		if err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(tuple0[0].Key, tk0, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-got +want):\n%s", diff)
		}
		if len(contToken0) == 0 {
			t.Error("got empty, want non-empty")
		}

		tuple1, contToken1, err := postgres.ReadByStore(ctx, store, storage.PaginationOptions{PageSize: 1, From: string(contToken0)})
		if err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(tuple1[0].Key, tk1, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-got +want):\n%s", diff)
		}
		if len(contToken1) != 0 {
			t.Errorf("got '%s', want empty", string(contToken1))
		}
	})

	t.Run("reading by store completely does not return a continuation token", func(t *testing.T) {
		tuples, contToken, err := postgres.ReadByStore(ctx, store, storage.PaginationOptions{PageSize: 2})
		if err != nil {
			t.Error(err)
		}
		if len(tuples) != 2 {
			t.Errorf("got '%d', want '2'", len(tuples))
		}
		if len(contToken) != 0 {
			t.Errorf("got '%s', want empty", string(contToken))
		}
	})

	t.Run("reading by store partially returns a continuation token", func(t *testing.T) {
		tuples, contToken, err := postgres.ReadByStore(ctx, store, storage.PaginationOptions{PageSize: 1})
		if err != nil {
			t.Error(err)
		}
		if len(tuples) != 1 {
			t.Errorf("got '%d', want '2'", len(tuples))
		}
		if len(contToken) == 0 {
			t.Errorf("got empty, want non-empty")
		}
	})
}

func TestFindLatestAuthorizationModelID(t *testing.T) {

	store := pkgTestutils.CreateRandomString(10)
	now := time.Now()
	oldModelID, err := id.NewStringFromTime(now)
	if err != nil {
		t.Fatal(err)
	}
	err = postgres.WriteAuthorizationModel(context.Background(), store, oldModelID, &openfgav1pb.TypeDefinitions{
		TypeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "folder",
				Relations: map[string]*openfgav1pb.Userset{
					"viewer": {
						Userset: &openfgav1pb.Userset_This{},
					},
				},
			},
		},
	})
	if err != nil {
		t.Errorf("failed to write authorization model: %v", err)
	}

	newModelID, err := id.NewStringFromTime(now)
	if err != nil {
		t.Fatal(err)
	}
	err = postgres.WriteAuthorizationModel(context.Background(), store, newModelID, &openfgav1pb.TypeDefinitions{
		TypeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "folder",
				Relations: map[string]*openfgav1pb.Userset{
					"reader": {
						Userset: &openfgav1pb.Userset_This{},
					},
				},
			},
		},
	})
	if err != nil {
		t.Errorf("failed to write authorization model: %v", err)
	}

	latestID, err := postgres.FindLatestAuthorizationModelID(context.Background(), store)
	if err != nil {
		t.Errorf("failed to read latest authorization model: %v", err)
	}

	if latestID != newModelID {
		t.Errorf("got '%s', want '%s'", latestID, newModelID)
	}
}

func TestWriteAndReadAuthorizationModel(t *testing.T) {

	store := pkgTestutils.CreateRandomString(10)
	modelID, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}
	expectedModel := &openfgav1pb.TypeDefinitions{
		TypeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "folder",
				Relations: map[string]*openfgav1pb.Userset{
					"viewer": {
						Userset: &openfgav1pb.Userset_This{
							This: &openfgav1pb.DirectUserset{},
						},
					},
				},
			},
		},
	}

	if err := postgres.WriteAuthorizationModel(context.Background(), store, modelID, expectedModel); err != nil {
		t.Errorf("failed to write authorization model: %v", err)
	}

	model, err := postgres.ReadAuthorizationModel(context.Background(), store, modelID)
	if err != nil {
		t.Errorf("failed to read authorization model: %v", err)
	}

	cmpOpts := []cmp.Option{
		cmpopts.IgnoreUnexported(
			openfgav1pb.TypeDefinition{},
			openfgav1pb.Userset{},
			openfgav1pb.Userset_This{},
			openfgav1pb.DirectUserset{},
		),
	}

	if diff := cmp.Diff(expectedModel.TypeDefinitions, model.TypeDefinitions, cmpOpts...); diff != "" {
		t.Errorf("mismatch (-got +want):\n%s", diff)
	}

	_, err = postgres.ReadAuthorizationModel(context.Background(), "undefined", modelID)
	if err != storage.NotFound {
		t.Errorf("got error '%v', want '%v'", err, storage.NotFound)
	}
}

func TestReadTypeDefinition(t *testing.T) {

	store := pkgTestutils.CreateRandomString(10)
	modelID, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}
	expectedTypeDef := &openfgav1pb.TypeDefinition{
		Type: "folder",
		Relations: map[string]*openfgav1pb.Userset{
			"viewer": {
				Userset: &openfgav1pb.Userset_This{
					This: &openfgav1pb.DirectUserset{},
				},
			},
		},
	}

	err = postgres.WriteAuthorizationModel(context.Background(), store, modelID, &openfgav1pb.TypeDefinitions{
		TypeDefinitions: []*openfgav1pb.TypeDefinition{
			expectedTypeDef,
		},
	})
	if err != nil {
		t.Errorf("failed to write authorization model: %v", err)
	}

	typeDef, err := postgres.ReadTypeDefinition(context.Background(), store, modelID, "folder")
	if err != nil {
		t.Errorf("expected no error but got '%v'", err)
	}

	cmpOpts := []cmp.Option{
		cmpopts.IgnoreUnexported(
			openfgav1pb.TypeDefinition{},
			openfgav1pb.Userset{},
			openfgav1pb.Userset_This{},
			openfgav1pb.DirectUserset{},
		),
	}

	if diff := cmp.Diff(expectedTypeDef, typeDef, cmpOpts...); diff != "" {
		t.Errorf("mismatch (-got +want):\n%s", diff)
	}
}

func TestReadAuthorizationModels(t *testing.T) {
	store := pkgTestutils.CreateRandomString(10)
	modelID1, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}
	err = postgres.WriteAuthorizationModel(context.Background(), store, modelID1, &openfgav1pb.TypeDefinitions{
		TypeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "folder",
				Relations: map[string]*openfgav1pb.Userset{
					"viewer": {
						Userset: &openfgav1pb.Userset_This{
							This: &openfgav1pb.DirectUserset{},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Errorf("failed to write authorization model: %v", err)
	}

	modelID2, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}
	err = postgres.WriteAuthorizationModel(context.Background(), store, modelID2, &openfgav1pb.TypeDefinitions{
		TypeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "folder",
				Relations: map[string]*openfgav1pb.Userset{
					"reader": {
						Userset: &openfgav1pb.Userset_This{
							This: &openfgav1pb.DirectUserset{},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Errorf("failed to write authorization model: %v", err)
	}

	modelIDs, continuationToken, err := postgres.ReadAuthorizationModels(context.Background(), store, storage.PaginationOptions{
		PageSize: 1,
	})
	if err != nil {
		t.Errorf("expected no error but got '%v'", err)
	}

	if !reflect.DeepEqual(modelIDs, []string{modelID1}) {
		t.Errorf("expected '%v' but got '%v", []string{modelID1}, modelIDs)
	}

	if len(continuationToken) == 0 {
		t.Error("expected non-empty continuation token")
	}

	modelIDs, continuationToken, err = postgres.ReadAuthorizationModels(context.Background(), store, storage.PaginationOptions{
		PageSize: 2,
		From:     string(continuationToken),
	})
	if err != nil {
		t.Errorf("expected no error but got '%v'", err)
	}

	if !reflect.DeepEqual(modelIDs, []string{modelID2}) {
		t.Errorf("expected '%v' but got '%v", []string{modelID2}, modelIDs)
	}

	if len(continuationToken) != 0 {
		t.Errorf("expected empty continuation token but got '%v'", string(continuationToken))
	}
}

func TestAssertion(t *testing.T) {
	t.Run("writing and reading assertions succeeds", func(t *testing.T) {
		store := pkgTestutils.CreateRandomString(10)
		modelID := pkgTestutils.CreateRandomString(10)
		assertions := []*openfga.Assertion{
			{
				TupleKey:    &openfga.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"},
				Expectation: false,
			},
			{
				TupleKey:    &openfga.TupleKey{Object: "doc:readme", Relation: "viewer", User: "11"},
				Expectation: true,
			},
		}

		if err := postgres.WriteAssertions(ctx, store, modelID, assertions); err != nil {
			t.Error(err)
		}
		gotAssertions, err := postgres.ReadAssertions(ctx, store, modelID)
		if err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(assertions, gotAssertions, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-got +want):\n%s", diff)
		}
	})

	t.Run("writing to one modelID and reading from other returns nothing", func(t *testing.T) {
		store := pkgTestutils.CreateRandomString(10)
		oldModelID := pkgTestutils.CreateRandomString(10)
		newModelID := pkgTestutils.CreateRandomString(10)
		assertions := []*openfga.Assertion{
			{
				TupleKey:    &openfga.TupleKey{Object: "doc:readme", Relation: "owner", User: "10"},
				Expectation: false,
			},
		}

		if err := postgres.WriteAssertions(ctx, store, oldModelID, assertions); err != nil {
			t.Error(err)
		}
		gotAssertions, err := postgres.ReadAssertions(ctx, store, newModelID)
		if err != nil {
			t.Error(err)
		}
		if len(gotAssertions) != 0 {
			t.Errorf("got assertions, but expected none: %v", gotAssertions)
		}
	})
}

func TestReadChanges(t *testing.T) {

	t.Run("read changes with continuation token", func(t *testing.T) {
		store := pkgTestutils.CreateRandomString(10)

		tk1 := &openfga.TupleKey{
			Object:   tuple.BuildObject("folder", "folder1"),
			Relation: "viewer",
			User:     "bob",
		}
		tk2 := &openfga.TupleKey{
			Object:   tuple.BuildObject("folder", "folder2"),
			Relation: "viewer",
			User:     "bill",
		}

		err := postgres.Write(ctx, store, nil, []*openfga.TupleKey{tk1, tk2})
		if err != nil {
			t.Errorf("failed to write tuples: %v", err)
		}

		changes, continuationToken, err := postgres.ReadChanges(
			context.Background(),
			store,
			"folder",
			storage.PaginationOptions{
				PageSize: 1,
			},
			0,
		)
		if err != nil {
			t.Errorf("expected no error but got '%v'", err)
		}

		if string(continuationToken) == "" {
			t.Error("expected non-empty token")
		}

		expectedChanges := []*openfga.TupleChange{
			{
				TupleKey:  tk1,
				Operation: openfga.TupleOperation_WRITE,
			},
		}

		if diff := cmp.Diff(expectedChanges, changes, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-got +want):\n%s", diff)
		}

		changes, continuationToken, err = postgres.ReadChanges(
			context.Background(),
			store,
			"folder",
			storage.PaginationOptions{
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

		expectedChanges = []*openfga.TupleChange{
			{
				TupleKey:  tk2,
				Operation: openfga.TupleOperation_WRITE,
			},
		}
		if diff := cmp.Diff(expectedChanges, changes, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-got +want):\n%s", diff)
		}
	})

	t.Run("read changes with no changes should return not found", func(t *testing.T) {
		store := pkgTestutils.CreateRandomString(10)

		_, _, err := postgres.ReadChanges(ctx, store, "", storage.PaginationOptions{PageSize: storage.DefaultPageSize}, 0)
		if !errors.Is(err, storage.NotFound) {
			t.Errorf("expected '%v', got '%v'", storage.NotFound, err)
		}
	})

	t.Run("read changes with horizon offset should return not found (no changes)", func(t *testing.T) {
		store := pkgTestutils.CreateRandomString(10)

		tk1 := &openfga.TupleKey{
			Object:   tuple.BuildObject("folder", "folder1"),
			Relation: "viewer",
			User:     "bob",
		}
		tk2 := &openfga.TupleKey{
			Object:   tuple.BuildObject("folder", "folder2"),
			Relation: "viewer",
			User:     "bill",
		}

		err := postgres.Write(ctx, store, nil, []*openfga.TupleKey{tk1, tk2})
		if err != nil {
			t.Fatal(err)
		}

		_, _, err = postgres.ReadChanges(ctx, store, "", storage.PaginationOptions{PageSize: storage.DefaultPageSize}, 1*time.Minute)
		if !errors.Is(err, storage.NotFound) {
			t.Errorf("expected '%v', got '%v'", storage.NotFound, err)
		}
	})

	t.Run("read changes with non-empty object type should only read that object type", func(t *testing.T) {
		store := pkgTestutils.CreateRandomString(10)

		tk1 := &openfga.TupleKey{
			Object:   tuple.BuildObject("folder", "1"),
			Relation: "viewer",
			User:     "bob",
		}
		tk2 := &openfga.TupleKey{
			Object:   tuple.BuildObject("document", "1"),
			Relation: "viewer",
			User:     "bill",
		}

		err := postgres.Write(ctx, store, nil, []*openfga.TupleKey{tk1, tk2})
		if err != nil {
			t.Fatal(err)
		}

		changes, continuationToken, err := postgres.ReadChanges(ctx, store, "folder", storage.PaginationOptions{PageSize: storage.DefaultPageSize}, 0)
		if err != nil {
			t.Errorf("expected no error but got '%v'", err)
		}

		if len(continuationToken) == 0 {
			t.Errorf("expected empty token but got '%s'", continuationToken)
		}

		expectedChanges := []*openfga.TupleChange{
			{
				TupleKey:  tk1,
				Operation: openfga.TupleOperation_WRITE,
			},
		}
		if diff := cmp.Diff(expectedChanges, changes, cmpOpts...); diff != "" {
			t.Errorf("mismatch (-got +want):\n%s", diff)
		}
	})
}
