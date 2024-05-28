package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/server"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestSuccessfulReadAuthorizationModelQuery(t *testing.T, datastore storage.OpenFGADatastore, s *server.Server) {
	var tests = []struct {
		name    string
		storeID string
		model   *openfgav1.AuthorizationModel
	}{
		/*
			{
				name:    "write_and_read_a_1.0_model",
				storeID: ulid.Make().String(),
				model: &openfgav1.AuthorizationModel{
					Id:            ulid.Make().String(),
					SchemaVersion: typesystem.SchemaVersion1_0,
					TypeDefinitions: []*openfgav1.TypeDefinition{
						{
							Type: "user",
						},
						{
							Type: "document",
							Relations: map[string]*openfgav1.Userset{
								"reader": {
									Userset: &openfgav1.Userset_This{},
								},
							},
						},
					},
				},
			},
		*/
		{
			name:    "write_and_read_an_1.1_model",
			storeID: ulid.Make().String(),
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
  schema 1.1
type user

type document
  relations
	define reader: [user]`).GetTypeDefinitions(),
			},
		},
	}

	ctx := context.Background()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authzModelResp, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
				StoreId:         test.storeID,
				TypeDefinitions: test.model.GetTypeDefinitions(),
				SchemaVersion:   test.model.GetSchemaVersion(),
				Conditions:      test.model.GetConditions(),
			})
			require.NoError(t, err)

			resp, err := s.ReadAuthorizationModel(ctx, &openfgav1.ReadAuthorizationModelRequest{
				StoreId: test.storeID,
				Id:      authzModelResp.GetAuthorizationModelId(),
			})

			require.NoError(t, err)
			require.Equal(t, authzModelResp.GetAuthorizationModelId(), resp.GetAuthorizationModel().GetId())
			require.Equal(t, test.model.GetSchemaVersion(), resp.GetAuthorizationModel().GetSchemaVersion())
		})
	}
}

func TestReadAuthorizationModelQueryErrors(t *testing.T, s *server.Server) {
	type readAuthorizationModelQueryTest struct {
		_name         string
		request       *openfgav1.ReadAuthorizationModelRequest
		expectedError error
	}

	var tests = []readAuthorizationModelQueryTest{
		{
			_name: "ReturnsAuthorizationModelNotFoundIfAuthorizationModelNotInDatabase",
			request: &openfgav1.ReadAuthorizationModelRequest{
				StoreId: ulid.Make().String(),
				Id:      "123",
			},
			expectedError: serverErrors.InvalidArgumentError(
				fmt.Errorf(`invalid ReadAuthorizationModelRequest.Id: value does not match regex pattern "^[ABCDEFGHJKMNPQRSTVWXYZ0-9]{26}$"`)),
		},
	}

	ctx := context.Background()

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			_, err := s.ReadAuthorizationModel(ctx, &openfgav1.ReadAuthorizationModelRequest{
				StoreId: test.request.GetStoreId(),
				Id:      test.request.GetId(),
			})
			require.ErrorIs(t, err, test.expectedError)
		})
	}
}

func ReadAuthorizationModelTest(t *testing.T, s *server.Server) {
	ctx := context.Background()
	storeID := ulid.Make().String()

	t.Run("writing_without_any_type_definitions_does_not_write_anything", func(t *testing.T) {
		model := &openfgav1.AuthorizationModel{
			Id:            ulid.Make().String(),
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"reader": {
							Userset: &openfgav1.Userset_This{},
						},
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"reader": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									typesystem.DirectRelationReference("user", ""),
								},
							},
						},
					},
				},
			},
		}

		_, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
			StoreId:         storeID,
			SchemaVersion:   model.GetSchemaVersion(),
			TypeDefinitions: model.GetTypeDefinitions(),
		})
		require.NoError(t, err)

		_, err = s.ReadAuthorizationModel(ctx, &openfgav1.ReadAuthorizationModelRequest{
			StoreId: storeID,
			Id:      model.GetId(),
		})
		require.ErrorContains(t, err, serverErrors.AuthorizationModelNotFound(model.GetId()).Error())
	})
}
