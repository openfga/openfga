package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	teststorage "github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestGetStoreQuery(t *testing.T, dc teststorage.DatastoreConstructor[storage.OpenFGADatastore]) {
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

	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	datastore, err := dc.New()
	require.NoError(err)

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {

			query := commands.NewGetStoreQuery(datastore, logger)
			actualResponse, actualError := query.Execute(ctx, test.request)

			if test.err != nil {
				if actualError == nil {
					t.Errorf("[%s] Expected error '%s', but got none", test._name, test.err)
				}
				if test.err.Error() != actualError.Error() {
					t.Errorf("[%s] Expected error '%s', actual '%s'", test._name, test.err, actualError)
				}
			}
			if test.err == nil && actualError != nil {
				t.Errorf("[%s] Did not expect an error but got one: %v", test._name, actualError)
			}

			if actualResponse == nil && test.err == nil {
				t.Errorf("[%s] Expected non nil response, got nil", test._name)
			} else {
				if diff := cmp.Diff(actualResponse, test.expectedResponse, ignoreStateOpts, ignoreStoreFields, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("[%s] store mismatch (-got +want):\n%s", test._name, diff)
				}
			}
		})
	}
}

func TestGetStoreSucceeds(t *testing.T, dc teststorage.DatastoreConstructor[storage.OpenFGADatastore]) {
	ignoreStateOpts := cmpopts.IgnoreUnexported(openfgapb.GetStoreResponse{})
	ignoreStoreFields := cmpopts.IgnoreFields(openfgapb.GetStoreResponse{}, "CreatedAt", "UpdatedAt", "Id")

	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	datastore, err := dc.New()
	require.NoError(err)

	store := testutils.CreateRandomString(10)
	createStoreQuery := commands.NewCreateStoreCommand(datastore, logger)

	createStoreResponse, err := createStoreQuery.Execute(ctx, &openfgapb.CreateStoreRequest{Name: store})
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	query := commands.NewGetStoreQuery(datastore, logger)
	actualResponse, actualError := query.Execute(ctx, &openfgapb.GetStoreRequest{StoreId: createStoreResponse.Id})

	if actualError != nil {
		t.Errorf("Expected no error, but got %v", actualError)
	}

	expectedResponse := &openfgapb.GetStoreResponse{
		Id:   createStoreResponse.Id,
		Name: store,
	}

	if diff := cmp.Diff(actualResponse, expectedResponse, ignoreStateOpts, ignoreStoreFields, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("unexpected result (-got +want):\n%s", diff)
	}
}
