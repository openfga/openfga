package test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func WriteAuthorizationModelTest(t *testing.T, datastore storage.OpenFGADatastore) {
	storeID := ulid.Make().String()

	items := make([]*openfgapb.TypeDefinition, datastore.MaxTypesPerAuthorizationModel()+1)
	items[0] = &openfgapb.TypeDefinition{
		Type: "user",
	}
	for i := 1; i < datastore.MaxTypesPerAuthorizationModel(); i++ {
		items[i] = &openfgapb.TypeDefinition{
			Type: fmt.Sprintf("type%v", i),
			Relations: map[string]*openfgapb.Userset{
				"admin": {Userset: &openfgapb.Userset_This{}},
			},
			Metadata: &openfgapb.Metadata{
				Relations: map[string]*openfgapb.RelationMetadata{
					"admin": {
						DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
							typesystem.DirectRelationReference("user", ""),
						},
					},
				},
			},
		}
	}

	var tests = []struct {
		name          string
		request       *openfgapb.WriteAuthorizationModelRequest
		allowSchema10 bool
		err           error
	}{
		{
			name: "fails_if_too_many_types",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId:         storeID,
				TypeDefinitions: items,
				SchemaVersion:   typesystem.SchemaVersion1_1,
			},
			allowSchema10: false,
			err:           serverErrors.ExceededEntityLimit("type definitions in an authorization model", datastore.MaxTypesPerAuthorizationModel()),
		},
		{
			name: "fails_if_a_relation_is_not_defined",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"owner": {},
						},
					},
				},
				SchemaVersion: typesystem.SchemaVersion1_1,
			},
			allowSchema10: false,
			err:           serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{ObjectType: "repo", Relation: "owner"}),
		},
		{
			name: "Fails_if_type_info_metadata_is_omitted_in_1.1_model",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId:       storeID,
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": typesystem.This(),
						},
					},
				},
			},
			allowSchema10: false,
			err: serverErrors.InvalidAuthorizationModelInput(
				errors.New("the assignable relation 'reader' in object type 'document' must contain at least one relation type"),
			),
		},
		{
			name: "Fails_if_writing_1_0_model_because_it_will_be_interpreted_as_1_1",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": typesystem.This(),
						},
					},
				},
			},
			allowSchema10: true,
			err:           serverErrors.InvalidAuthorizationModelInput(typesystem.AssignableRelationError("document", "reader")),
		},
		{
			name: "Works_if_no_schema_version",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.WildcardRelationReference("user"),
									},
								},
							},
						},
					},
				},
			},
		},
	}

	ctx := context.Background()
	logger := logger.NewNoopLogger()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := commands.NewWriteAuthorizationModelCommand(datastore, logger)
			resp, err := cmd.Execute(ctx, test.request)
			require.ErrorIs(t, err, test.err)

			if err == nil {
				_, err = ulid.Parse(resp.AuthorizationModelId)
				require.NoError(t, err)
			}
		})
	}
}
