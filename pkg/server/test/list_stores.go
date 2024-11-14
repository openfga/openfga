package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

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
		}, nil)
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
	listStoresResponse, actualError := getStoresQuery.Execute(ctx, &openfgav1.ListStoresRequest{}, nil)
	require.NoError(t, actualError)
	require.Empty(t, listStoresResponse.GetStores())

	// create three stores
	createStoreQuery := commands.NewCreateStoreCommand(datastore)
	firstStoreName := testutils.CreateRandomString(10)
	firstStoreResponse, err := createStoreQuery.Execute(ctx, &openfgav1.CreateStoreRequest{Name: firstStoreName})
	require.NoError(t, err, "error creating store 1")

	secondStoreName := testutils.CreateRandomString(10)
	_, err = createStoreQuery.Execute(ctx, &openfgav1.CreateStoreRequest{Name: secondStoreName})
	require.NoError(t, err, "error creating store 2")

	thirdStoreName := testutils.CreateRandomString(10)
	thirdStoreResponse, err := createStoreQuery.Execute(ctx, &openfgav1.CreateStoreRequest{Name: thirdStoreName})
	require.NoError(t, err, "error creating store 3")

	t.Run("list with page size 1", func(t *testing.T) {
		// first page: 1st store
		listStoresResponse, actualError = getStoresQuery.Execute(ctx, &openfgav1.ListStoresRequest{
			PageSize:          wrapperspb.Int32(1),
			ContinuationToken: "",
		}, nil)
		require.NoError(t, actualError)
		require.Len(t, listStoresResponse.GetStores(), 1)
		require.Equal(t, firstStoreName, listStoresResponse.GetStores()[0].GetName())
		require.NotEmpty(t, listStoresResponse.GetContinuationToken())

		secondListStoresResponse, actualError := getStoresQuery.Execute(ctx, &openfgav1.ListStoresRequest{
			PageSize:          wrapperspb.Int32(1),
			ContinuationToken: listStoresResponse.GetContinuationToken(),
		}, nil)
		require.NoError(t, actualError)
		require.Len(t, secondListStoresResponse.GetStores(), 1)
		require.Equal(t, secondStoreName, secondListStoresResponse.GetStores()[0].GetName())
		require.NotEmpty(t, secondListStoresResponse.GetContinuationToken())

		// first page: 3rd store
		thirdListStoresResponse, actualError := getStoresQuery.Execute(ctx, &openfgav1.ListStoresRequest{
			PageSize:          wrapperspb.Int32(1),
			ContinuationToken: secondListStoresResponse.GetContinuationToken(),
		}, nil)
		require.NoError(t, actualError)
		require.Len(t, thirdListStoresResponse.GetStores(), 1)
		require.Equal(t, thirdStoreName, thirdListStoresResponse.GetStores()[0].GetName())

		// first page: 2nd store
		// no token <=> no more results
		require.Empty(t, thirdListStoresResponse.GetContinuationToken())
	})

	t.Run("list with store ids filter", func(t *testing.T) {
		// test filter by store IDs
		filterListStoresResponse, actualError := getStoresQuery.Execute(ctx, &openfgav1.ListStoresRequest{
			PageSize:          wrapperspb.Int32(2),
			ContinuationToken: "",
		}, []string{firstStoreResponse.GetId(), thirdStoreResponse.GetId()})
		require.NoError(t, actualError)
		require.Len(t, filterListStoresResponse.GetStores(), 2)
		require.Equal(t, firstStoreName, filterListStoresResponse.GetStores()[0].GetName())
		require.Equal(t, thirdStoreName, filterListStoresResponse.GetStores()[1].GetName())
		require.Empty(t, filterListStoresResponse.GetContinuationToken())
	})

	t.Run("list with name filter", func(t *testing.T) {
		// test filter by name
		filterListStoresResponse, actualError := getStoresQuery.Execute(ctx, &openfgav1.ListStoresRequest{
			PageSize: wrapperspb.Int32(3),
			Name:     firstStoreName,
		}, nil)
		require.NoError(t, actualError)
		require.Len(t, filterListStoresResponse.GetStores(), 1)
		require.Equal(t, firstStoreName, filterListStoresResponse.GetStores()[0].GetName())
		require.Empty(t, filterListStoresResponse.GetContinuationToken())
	})
}
