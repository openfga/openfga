package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/server/commands"
	"github.com/openfga/openfga/storage"
	teststorage "github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestCreateStore(t *testing.T, dbTester teststorage.DatastoreTester[storage.OpenFGADatastore]) {

	type createStoreTestSettings struct {
		_name    string
		request  *openfgapb.CreateStoreRequest
		response *openfgapb.CreateStoreResponse
		err      error
	}

	name := testutils.CreateRandomString(10)

	var tests = []createStoreTestSettings{
		{
			_name: "CreateStoreSucceeds",
			request: &openfgapb.CreateStoreRequest{
				Name: name,
			},
			response: &openfgapb.CreateStoreResponse{
				Name: name,
			},
		},
	}

	ignoreStateOpts := cmpopts.IgnoreUnexported(openfgapb.CreateStoreResponse{})
	ignoreStoreFields := cmpopts.IgnoreFields(openfgapb.CreateStoreResponse{}, "CreatedAt", "UpdatedAt", "Id")

	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	datastore, err := dbTester.New()
	require.NoError(err)

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {

			actualResponse, actualError := commands.NewCreateStoreCommand(datastore, logger).Execute(ctx, test.request)

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

			if actualResponse == nil {
				t.Error("Expected non nil response, got nil")
			} else {
				if diff := cmp.Diff(actualResponse, test.response, ignoreStateOpts, ignoreStoreFields, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("[%s] store mismatch (-got +want):\n%s", test._name, diff)
				}
			}
		})
	}
}
