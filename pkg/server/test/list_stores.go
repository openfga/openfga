package test

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, actualError)
	require.Equal(t, 0, len(listStoresResponse.Stores))

	// create two stores
	createStoreQuery := commands.NewCreateStoreCommand(datastore, logger)
	firstStoreName := testutils.CreateRandomString(10)
	_, err := createStoreQuery.Execute(ctx, &openfgapb.CreateStoreRequest{Name: firstStoreName})
	require.NoError(t, err, "error creating store 1")

	secondStoreName := testutils.CreateRandomString(10)
	_, err = createStoreQuery.Execute(ctx, &openfgapb.CreateStoreRequest{Name: secondStoreName})
	require.NoError(t, err, "error creating store 2")
	// first page: 1st store
	listStoresResponse, actualError = getStoresQuery.Execute(ctx, &openfgapb.ListStoresRequest{
		PageSize:          wrapperspb.Int32(1),
		ContinuationToken: "",
	})
	require.NoError(t, actualError)
	require.Equal(t, 1, len(listStoresResponse.Stores))
	require.Equal(t, firstStoreName, listStoresResponse.Stores[0].Name)
	require.NotEmpty(t, listStoresResponse.ContinuationToken)

	// first page: 2nd store
	secondListStoresResponse, actualError := getStoresQuery.Execute(ctx, &openfgapb.ListStoresRequest{
		PageSize:          wrapperspb.Int32(1),
		ContinuationToken: listStoresResponse.ContinuationToken,
	})
	require.NoError(t, actualError)
	require.Equal(t, 1, len(secondListStoresResponse.Stores))
	require.Equal(t, secondStoreName, secondListStoresResponse.Stores[0].Name)
	// no token <=> no more results
	require.Empty(t, secondListStoresResponse.ContinuationToken)
}
