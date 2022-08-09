package test

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/server/commands"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestListStores(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	// clean up all stores from other tests
	getStoresQuery := commands.NewListStoresQuery(datastore, logger, encoder.NewBase64Encoder())
	deleteCmd := commands.NewDeleteStoreCommand(datastore, logger)
	deleteContinuationToken := ""
	for ok := true; ok; ok = deleteContinuationToken != "" {
		listStoresResponse, _ := getStoresQuery.Execute(ctx, &openfgapb.ListStoresRequest{
			ContinuationToken: deleteContinuationToken,
		})
		for _, store := range listStoresResponse.Stores {
			if _, err := deleteCmd.Execute(ctx, &openfgapb.DeleteStoreRequest{
				StoreId: store.Id,
			}); err != nil {
				t.Fatalf("failed cleaning stores with %v", err)
			}
		}
		deleteContinuationToken = listStoresResponse.ContinuationToken
	}

	// ensure there are actually no stores
	listStoresResponse, actualError := getStoresQuery.Execute(ctx, &openfgapb.ListStoresRequest{})
	if actualError != nil {
		t.Fatalf("Expected no error, but got %v", actualError)
	}
	if len(listStoresResponse.Stores) != 0 {
		t.Fatalf("Expected 0 stores, got: %v", len(listStoresResponse.Stores))
	}

	// create two stores
	createStoreQuery := commands.NewCreateStoreCommand(datastore, logger)
	firstStoreName := testutils.CreateRandomString(10)
	_, err := createStoreQuery.Execute(ctx, &openfgapb.CreateStoreRequest{Name: firstStoreName})
	if err != nil {
		t.Fatalf("Error creating store 1: %v", err)
	}

	secondStoreName := testutils.CreateRandomString(10)
	_, err = createStoreQuery.Execute(ctx, &openfgapb.CreateStoreRequest{Name: secondStoreName})
	if err != nil {
		t.Fatalf("Error creating store 2: %v", err)
	}
	// first page: 1st store
	listStoresResponse, actualError = getStoresQuery.Execute(ctx, &openfgapb.ListStoresRequest{
		PageSize:          wrapperspb.Int32(1),
		ContinuationToken: "",
	})
	if actualError != nil {
		t.Errorf("Expected no error, but got %v", actualError)
	}

	if len(listStoresResponse.Stores) != 1 {
		t.Fatalf("Expected 1 store, got: %v", len(listStoresResponse.Stores))
	}
	if listStoresResponse.Stores[0].Name != firstStoreName {
		t.Fatalf("Expected store name to be %v but got %v", firstStoreName, listStoresResponse.Stores[0].Name)
	}
	if listStoresResponse.ContinuationToken == "" {
		t.Fatal("Expected continuation token, got nothing")
	}

	// first page: 2nd store
	secondListStoresResponse, actualError := getStoresQuery.Execute(ctx, &openfgapb.ListStoresRequest{
		PageSize:          wrapperspb.Int32(1),
		ContinuationToken: listStoresResponse.ContinuationToken,
	})
	if actualError != nil {
		t.Errorf("Expected no error, but got %v", actualError)
	}

	if len(secondListStoresResponse.Stores) != 1 {
		t.Fatalf("Expected 1 store, got: %v", len(secondListStoresResponse.Stores))
	}
	if secondListStoresResponse.Stores[0].Name != secondStoreName {
		t.Fatalf("Expected store name to be %v but got %v", secondStoreName, secondListStoresResponse.Stores[0].Name)
	}
	// no token <=> no more results
	if secondListStoresResponse.ContinuationToken != "" {
		t.Fatalf("Expected no continuation token, got %v", secondListStoresResponse.ContinuationToken)
	}

}
