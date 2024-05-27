package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/openfga/openfga/internal/server/commands"
	"github.com/openfga/openfga/pkg/server"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
)

func TestGetStoreQuery(t *testing.T, s *server.Server) {
	type getStoreQueryTest struct {
		_name            string
		expectedResponse *openfgav1.GetStoreResponse
		err              error
	}

	var tests = []getStoreQueryTest{
		{
			_name: "ReturnsNotFound",
			expectedResponse: &openfgav1.GetStoreResponse{
				Name: "ReturnsNotFound",
			},
			err: serverErrors.StoreIDNotFound,
		},
	}

	ignoreStoreFields := protocmp.IgnoreFields(protoadapt.MessageV2Of(&openfgav1.GetStoreResponse{}), "created_at", "updated_at", "id")

	ctx := context.Background()

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			store, err := s.CreateStore(ctx, &openfgav1.CreateStoreRequest{
				Name: test.expectedResponse.GetName(),
			})
			require.NoError(t, err)
			test.expectedResponse.Id = store.GetId()

			actualRespense, err := s.GetStore(ctx, &openfgav1.GetStoreRequest{
				StoreId: store.GetId(),
			})
			require.NoError(t, err)
			if diff := cmp.Diff(test.expectedResponse, actualRespense, ignoreStoreFields, protocmp.Transform()); diff != "" {
				t.Errorf("store mismatch (-want +got):\n%s", diff)
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
