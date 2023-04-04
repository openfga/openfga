package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestGetStoreQuery(t *testing.T, datastore storage.OpenFGADatastore) {
	type getStoreQueryTest struct {
		_name            string
		request          *openfgapb.GetStoreRequest
		expectedResponse *openfgapb.GetStoreResponse
		err              error
	}

	var tests = []getStoreQueryTest{
		{
			_name:   "ReturnsNotFound",
			request: &openfgapb.GetStoreRequest{StoreId: "non-existent store"},
			err:     serverErrors.StoreIDNotFound,
		},
	}

	ignoreStateOpts := cmpopts.IgnoreUnexported(openfgapb.GetStoreResponse{})
	ignoreStoreFields := cmpopts.IgnoreFields(openfgapb.GetStoreResponse{}, "CreatedAt", "UpdatedAt", "Id")

	ctx := context.Background()
	logger := logger.NewNoopLogger()

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {

			query := commands.NewGetStoreQuery(datastore, logger)
			resp, err := query.Execute(ctx, test.request)

			if test.err != nil {
				require.ErrorIs(t, err, test.err)
				require.Nil(t, resp)
			} else {
				require.NoError(t, err)
				if diff := cmp.Diff(test.expectedResponse, resp, ignoreStateOpts, ignoreStoreFields, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("store mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestGetStoreSucceeds(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	store := testutils.CreateRandomString(10)
	createStoreQuery := commands.NewCreateStoreCommand(datastore, logger)

	createStoreResponse, err := createStoreQuery.Execute(ctx, &openfgapb.CreateStoreRequest{Name: store})
	require.NoError(t, err)

	query := commands.NewGetStoreQuery(datastore, logger)
	actualResponse, actualError := query.Execute(ctx, &openfgapb.GetStoreRequest{StoreId: createStoreResponse.Id})
	require.NoError(t, actualError)

	expectedResponse := &openfgapb.GetStoreResponse{
		Id:   createStoreResponse.Id,
		Name: store,
	}

	ignoreStateOpts := cmpopts.IgnoreUnexported(openfgapb.GetStoreResponse{})
	ignoreStoreFields := cmpopts.IgnoreFields(openfgapb.GetStoreResponse{}, "CreatedAt", "UpdatedAt", "Id")

	if diff := cmp.Diff(expectedResponse, actualResponse, ignoreStateOpts, ignoreStoreFields, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}
