package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/openfga/openfga/pkg/server"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestReadAuthorizationModelsWithoutPaging(t *testing.T, s *server.Server) {
	ctx := context.Background()
	store := ulid.Make().String()

	tests := []struct {
		name                      string
		model                     *openfgav1.AuthorizationModel
		expectedNumModelsReturned int
	}{
		{
			name:                      "empty",
			expectedNumModelsReturned: 0,
		},
		{
			name: "non-empty_type_definitions",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
					},
				},
			},
			expectedNumModelsReturned: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.model != nil {
				_, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
					StoreId:         store,
					TypeDefinitions: test.model.GetTypeDefinitions(),
					SchemaVersion:   test.model.GetSchemaVersion(),
					Conditions:      test.model.GetConditions(),
				})
				require.NoError(t, err)
			}

			resp, err := s.ReadAuthorizationModels(ctx, &openfgav1.ReadAuthorizationModelsRequest{
				StoreId: store,
			})
			require.NoError(t, err)

			require.Len(t, resp.GetAuthorizationModels(), test.expectedNumModelsReturned)
			require.Empty(t, resp.GetContinuationToken(), "expected an empty continuation token")
		})
	}
}

func TestReadAuthorizationModelsWithPaging(t *testing.T, s *server.Server) {
	ctx := context.Background()
	store := ulid.Make().String()

	resp1, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:       store,
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "repo",
			},
		},
		Conditions: map[string]*openfgav1.Condition{},
	})
	require.NoError(t, err)

	firstResponse, err := s.ReadAuthorizationModels(ctx, &openfgav1.ReadAuthorizationModelsRequest{
		StoreId:  store,
		PageSize: wrapperspb.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, firstResponse.GetAuthorizationModels(), 1)
	require.Equal(t, firstResponse.GetAuthorizationModels()[0].GetId(), resp1.GetAuthorizationModelId())
	require.Empty(t, firstResponse.GetContinuationToken(), "Expected continuation token")

	resp2, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:       store,
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "repo",
			},
		},
		Conditions: map[string]*openfgav1.Condition{},
	})
	require.NoError(t, err)
	secondResponse, err := s.ReadAuthorizationModels(ctx, &openfgav1.ReadAuthorizationModelsRequest{
		StoreId:           store,
		PageSize:          wrapperspb.Int32(1),
		ContinuationToken: "",
	})
	require.NoError(t, err)
	require.Len(t, secondResponse.GetAuthorizationModels(), 1)
	require.Equal(t, secondResponse.GetAuthorizationModels()[0].GetId(), resp2.GetAuthorizationModelId())
	require.NotEmpty(t, secondResponse.GetContinuationToken(), "Expected continuation token")

	_, err = s.ReadAuthorizationModels(ctx, &openfgav1.ReadAuthorizationModelsRequest{
		StoreId:           store,
		ContinuationToken: "bad",
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "Invalid continuation token")

	_, err = s.ReadAuthorizationModels(ctx, &openfgav1.ReadAuthorizationModelsRequest{
		StoreId:           "non-existent",
		ContinuationToken: "eyJwayI6IkxBVEVTVF9OU0NPTkZJR19hdXRoMHN0b3JlIiwic2siOiIxem1qbXF3MWZLZExTcUoyN01MdTdqTjh0cWgifQ==",
	})
	require.Error(t, err)
	require.ErrorIs(t, err, serverErrors.InvalidArgumentError(
		fmt.Errorf(`invalid ReadAuthorizationModelsRequest.StoreId: value does not match regex pattern "^[ABCDEFGHJKMNPQRSTVWXYZ0-9]{26}$"`)))
}

func TestReadAuthorizationModelsInvalidContinuationToken(t *testing.T, s *server.Server) {
	ctx := context.Background()
	store := ulid.Make().String()

	_, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         store,
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: []*openfgav1.TypeDefinition{{Type: "repo"}},
		Conditions:      map[string]*openfgav1.Condition{},
	})
	require.NoError(t, err)

	_, err = s.ReadAuthorizationModels(ctx, &openfgav1.ReadAuthorizationModelsRequest{
		StoreId:           store,
		ContinuationToken: "foobar",
	})
	require.ErrorIs(t, err, serverErrors.InvalidContinuationToken)
}
