package test

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/server/commands"
	"github.com/openfga/openfga/server/queries"
	teststorage "github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestListStores(t *testing.T, dbTester teststorage.DatastoreTester) {
	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	datastore, err := dbTester.New()
	require.NoError(err)

	fakeEncoder, err := encoder.NewTokenEncrypter("key")
	if err != nil {
		t.Fatal(err)
	}

	getStoresQuery := queries.NewListStoresQuery(datastore, fakeEncoder, logger)
	_, actualError := getStoresQuery.Execute(ctx, &openfgav1pb.ListStoresRequest{})
	if actualError != nil {
		t.Fatalf("Expected no error, but got %v", actualError)
	}

	createStoreQuery := commands.NewCreateStoreCommand(datastore, logger)
	_, err = createStoreQuery.Execute(ctx, &openfgav1pb.CreateStoreRequest{Name: testutils.CreateRandomString(10)})
	if err != nil {
		t.Fatalf("Error creating store 1: %v", err)
	}

	_, err = createStoreQuery.Execute(ctx, &openfgav1pb.CreateStoreRequest{Name: testutils.CreateRandomString(10)})
	if err != nil {
		t.Fatalf("Error creating store 2: %v", err)
	}

	listStoresResponse, actualError := getStoresQuery.Execute(ctx, &openfgav1pb.ListStoresRequest{
		PageSize:          wrapperspb.Int32(1),
		ContinuationToken: "",
	})
	if actualError != nil {
		t.Errorf("Expected no error, but got %v", actualError)
	}

	if len(listStoresResponse.Stores) != 1 {
		t.Fatalf("Expected 1 store, got: %v", len(listStoresResponse.Stores))
	}
	if listStoresResponse.ContinuationToken == "" {
		t.Fatal("Expected continuation token, got nothing")
	}

	secondListStoresResponse, actualError := getStoresQuery.Execute(ctx, &openfgav1pb.ListStoresRequest{
		PageSize:          wrapperspb.Int32(1),
		ContinuationToken: listStoresResponse.ContinuationToken,
	})
	if actualError != nil {
		t.Errorf("Expected no error, but got %v", actualError)
	}

	if len(secondListStoresResponse.Stores) != 1 {
		t.Fatalf("Expected 1 store, got: %v", len(secondListStoresResponse.Stores))
	}
	if secondListStoresResponse.ContinuationToken == "" {
		t.Fatal("Expected continuation token, got nothing")
	}
}
