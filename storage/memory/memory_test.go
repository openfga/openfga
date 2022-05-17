package memory_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/telemetry"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/storage"
	"github.com/openfga/openfga/storage/memory"
	"github.com/openfga/openfga/storage/test"
	"go.buf.build/openfga/go/openfga/api/openfga"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

var (
	memoryStorage *memory.MemoryBackend
)

func init() {
	memoryStorage = memory.New(telemetry.NewNoopTracer(), 10000, 10000)
}

func TestMemdbStorage(t *testing.T) {
	t.Skip() // todo(jon-whit): fix in-memory implementation so they pass the storage tests

	testEngine := storagefixtures.RunDatastoreEngine(t, "memory")

	test.TestAll(t, test.DatastoreTesterFunc(func() (storage.OpenFGADatastore, error) {
		ds := testEngine.NewDatastore(t, func(engine, uri string) storage.OpenFGADatastore {
			return memory.New(telemetry.NewNoopTracer(), 10, 24)
		})

		return ds, nil
	}))
}

func TestReadAuthorizationModels(t *testing.T) {
	ctx := context.Background()

	store := testutils.CreateRandomString(10)
	modelID1, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}
	tds1 := []*openfgav1pb.TypeDefinition{
		{
			Type: "github-repo",
			Relations: map[string]*openfgav1pb.Userset{
				"repo_admin": {},
			},
		},
	}
	err = memoryStorage.WriteAuthorizationModel(ctx, store, modelID1, &openfgav1pb.TypeDefinitions{TypeDefinitions: tds1})

	if err != nil {
		t.Fatalf("Error writing type definition: %v", err)
	}

	modelID2, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}
	tds2 := []*openfgav1pb.TypeDefinition{
		{
			Type: "github-org",
			Relations: map[string]*openfgav1pb.Userset{
				"org_owner": {},
			},
		},
	}
	err = memoryStorage.WriteAuthorizationModel(ctx, store, modelID2, &openfgav1pb.TypeDefinitions{TypeDefinitions: tds2})
	if err != nil {
		t.Fatalf("Error writing type definition: %v", err)
	}

	cmpOpts := []cmp.Option{
		cmpopts.IgnoreUnexported(
			openfgav1pb.TypeDefinition{},
			openfgav1pb.Userset{},
			openfgav1pb.Userset_This{},
			openfgav1pb.DirectUserset{},
		),
	}

	// first page
	models, continuationToken, err := memoryStorage.ReadAuthorizationModels(ctx, store, storage.PaginationOptions{PageSize: 1})
	if err != nil {
		t.Fatalf("error reading models: %v", err)
	}
	if len(models) != 1 {
		t.Fatalf("expected 1, got %d", len(models))
	}
	if modelID2 != models[0].Id {
		t.Fatalf("expected '%s', got '%s", modelID2, models[0].Id)
	}
	if diff := cmp.Diff(tds2, models[0].TypeDefinitions, cmpOpts...); diff != "" {
		t.Fatalf("mismatch (-got +want):\n%s", diff)
	}
	if len(continuationToken) == 0 {
		t.Fatal("expected continuation token")
	}

	// second page
	models, continuationToken, err = memoryStorage.ReadAuthorizationModels(ctx, store, storage.PaginationOptions{PageSize: storage.DefaultPageSize, From: string(continuationToken)})
	if err != nil {
		t.Fatalf("error reading models: %v", err)
	}
	if len(models) != 1 {
		t.Fatalf("expected 1, got %d", len(models))
	}
	if modelID1 != models[0].Id {
		t.Fatalf("expected '%s', got '%s", modelID1, models[0].Id)
	}
	if diff := cmp.Diff(tds1, models[0].TypeDefinitions, cmpOpts...); diff != "" {
		t.Fatalf("mismatch (-got +want):\n%s", diff)
	}
	if len(continuationToken) != 0 {
		t.Fatal("expected empty continuation token")
	}
}

func TestGetStores(t *testing.T) {
	ctx := context.Background()
	// no stores
	_, _, err := memoryStorage.ListStores(ctx, storage.PaginationOptions{})
	if err != nil {
		t.Fatalf("Expected no error but got %v", err)
	}
	// write two stores
	_, err = memoryStorage.CreateStore(ctx, &openfga.Store{
		Id:   "001",
		Name: "store1",
	})
	if err != nil {
		t.Fatalf("Error writing first store: %v", err)
	}
	_, err = memoryStorage.CreateStore(ctx, &openfga.Store{
		Id:   "002",
		Name: "store2",
	})
	if err != nil {
		t.Fatalf("Error writing second store: %v", err)
	}
	// read stores - first page
	firstPageStores, firstContinuationToken, err := memoryStorage.ListStores(ctx, storage.PaginationOptions{
		PageSize: 1,
	})
	if err != nil {
		t.Fatalf("Error reading all stores: %v", err)
	}
	if len(firstPageStores) != 1 {
		t.Fatalf("Unexpected number of stores: got %v, expected %v", len(firstPageStores), 1)
	}
	if len(firstContinuationToken) == 0 {
		t.Fatal("Expected continuation token")
	}

	// second page
	secondPageStores, secondContinuationToken, err := memoryStorage.ListStores(ctx, storage.PaginationOptions{
		PageSize: 1,
		From:     string(firstContinuationToken),
	})
	if err != nil {
		t.Fatalf("Error reading all stores: %v", err)
	}
	if len(secondPageStores) != 1 {
		t.Fatalf("Unexpected number of stores: got %v, expected %v", len(firstPageStores), 1)
	}
	if len(secondContinuationToken) == 0 {
		t.Fatal("Expected continuation token")
	}

	// third page - no more stores
	thirdPageStores, lastToken, err := memoryStorage.ListStores(ctx, storage.PaginationOptions{
		From: string(secondContinuationToken),
	})
	if err != nil {
		t.Fatalf("Error reading stores: %v", err)
	}
	if len(thirdPageStores) != 0 {
		t.Fatalf("Unexpected number of stores: got %v, expected %v", len(firstPageStores), 0)
	}
	if len(lastToken) != 0 {
		t.Fatal("Expected no continuation token")
	}
}
