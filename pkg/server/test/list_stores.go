package test

import (
	"context"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
)

func TestListStores(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	// clean up all stores from other tests
	getStoresQuery := commands.NewListStoresQuery(datastore)
	deleteCmd := commands.NewDeleteStoreCommand(datastore)
	deleteContinuationToken := ""
	for ok := true; ok; ok = deleteContinuationToken != "" {
		listStoresResponse, err := getStoresQuery.Execute(ctx, &openfgav1.ListStoresRequest{
			ContinuationToken: deleteContinuationToken,
		})
		require.NoError(t, err)
		for _, store := range listStoresResponse.GetStores() {
			_, err := deleteCmd.Execute(ctx, &openfgav1.DeleteStoreRequest{
				StoreId: store.GetId(),
			})
			require.NoError(t, err)
		}
		deleteContinuationToken = listStoresResponse.GetContinuationToken()
	}

	// ensure there are actually no stores
	listStoresResponse, actualError := getStoresQuery.Execute(ctx, &openfgav1.ListStoresRequest{})
	require.NoError(t, actualError)
	require.Empty(t, listStoresResponse.GetStores())

	// create two stores
	createStoreQuery := commands.NewCreateStoreCommand(datastore)
	firstStoreName := testutils.CreateRandomString(10)
	_, err := createStoreQuery.Execute(ctx, &openfgav1.CreateStoreRequest{Name: firstStoreName})
	require.NoError(t, err, "error creating store 1")

	secondStoreName := testutils.CreateRandomString(10)
	_, err = createStoreQuery.Execute(ctx, &openfgav1.CreateStoreRequest{Name: secondStoreName})
	require.NoError(t, err, "error creating store 2")
	// first page: 1st store
	listStoresResponse, actualError = getStoresQuery.Execute(ctx, &openfgav1.ListStoresRequest{
		PageSize:          wrapperspb.Int32(1),
		ContinuationToken: "",
	})
	require.NoError(t, actualError)
	require.Len(t, listStoresResponse.GetStores(), 1)
	require.Equal(t, firstStoreName, listStoresResponse.GetStores()[0].GetName())
	require.NotEmpty(t, listStoresResponse.GetContinuationToken())

	// first page: 2nd store
	secondListStoresResponse, actualError := getStoresQuery.Execute(ctx, &openfgav1.ListStoresRequest{
		PageSize:          wrapperspb.Int32(1),
		ContinuationToken: listStoresResponse.GetContinuationToken(),
	})
	require.NoError(t, actualError)
	require.Len(t, secondListStoresResponse.GetStores(), 1)
	require.Equal(t, secondStoreName, secondListStoresResponse.GetStores()[0].GetName())
	// no token <=> no more results
	require.Empty(t, secondListStoresResponse.GetContinuationToken())
}
