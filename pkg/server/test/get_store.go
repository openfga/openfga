package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/testing/protocmp"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
)

func TestGetStoreQuery(t *testing.T, datastore storage.OpenFGADatastore) {
	type getStoreQueryTest struct {
		_name            string
		request          *openfgav1.GetStoreRequest
		expectedResponse *openfgav1.GetStoreResponse
		err              error
	}

	var tests = []getStoreQueryTest{
		{
			_name:   "ReturnsNotFound",
			request: &openfgav1.GetStoreRequest{StoreId: "non-existent store"},
			err:     serverErrors.StoreIDNotFound,
		},
	}

	ignoreStoreFields := protocmp.IgnoreFields(protoadapt.MessageV2Of(&openfgav1.GetStoreResponse{}), "created_at", "updated_at", "id")

	ctx := context.Background()

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			query := commands.NewGetStoreQuery(datastore)
			resp, err := query.Execute(ctx, test.request)

			if test.err != nil {
				require.ErrorIs(t, err, test.err)
				require.Nil(t, resp)
			} else {
				require.NoError(t, err)
				if diff := cmp.Diff(test.expectedResponse, resp, ignoreStoreFields, protocmp.Transform()); diff != "" {
					t.Errorf("store mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestGetStoreSucceeds(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()

	store := testutils.CreateRandomString(10)
	createStoreQuery := commands.NewCreateStoreCommand(datastore)

	createStoreResponse, err := createStoreQuery.Execute(ctx, &openfgav1.CreateStoreRequest{Name: store})
	require.NoError(t, err)

	query := commands.NewGetStoreQuery(datastore)
	actualResponse, actualError := query.Execute(ctx, &openfgav1.GetStoreRequest{StoreId: createStoreResponse.GetId()})
	require.NoError(t, actualError)

	expectedResponse := &openfgav1.GetStoreResponse{
		Id:   createStoreResponse.GetId(),
		Name: store,
	}

	ignoreStoreFields := protocmp.IgnoreFields(protoadapt.MessageV2Of(&openfgav1.GetStoreResponse{}), "created_at", "updated_at", "id")
	if diff := cmp.Diff(expectedResponse, actualResponse, ignoreStoreFields, protocmp.Transform()); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}
