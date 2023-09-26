package test

import (
	"context"
	"fmt"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func WriteAuthorizationModelTest(t *testing.T, datastore storage.OpenFGADatastore) {
	storeID := ulid.Make().String()

	items := make([]*openfgav1.TypeDefinition, datastore.MaxTypesPerAuthorizationModel()+1)
	items[0] = &openfgav1.TypeDefinition{
		Type: "user",
	}
	for i := 1; i < datastore.MaxTypesPerAuthorizationModel(); i++ {
		items[i] = &openfgav1.TypeDefinition{
			Type: fmt.Sprintf("type%v", i),
			Relations: map[string]*openfgav1.Userset{
				"admin": {Userset: &openfgav1.Userset_This{}},
			},
			Metadata: &openfgav1.Metadata{
				Relations: map[string]*openfgav1.RelationMetadata{
					"admin": {
						DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
							typesystem.DirectRelationReference("user", ""),
						},
					},
				},
			},
		}
	}

	var tests = []struct {
		name          string
		request       *openfgav1.WriteAuthorizationModelRequest
		allowSchema10 bool
		errCode       codes.Code
	}{
		{
			name: "fails_if_too_many_types",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId:         storeID,
				TypeDefinitions: items,
				SchemaVersion:   typesystem.SchemaVersion1_1,
			},
			allowSchema10: false,
			errCode:       codes.Code(openfgav1.ErrorCode_exceeded_entity_limit),
		},
		{
			name: "fails_if_a_relation_is_not_defined",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgav1.Userset{
							"owner": {},
						},
					},
				},
				SchemaVersion: typesystem.SchemaVersion1_1,
			},
			allowSchema10: false,
			errCode:       codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			name: "Fails_if_type_info_metadata_is_omitted_in_1.1_model",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId:       storeID,
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": typesystem.This(),
						},
					},
				},
			},
			allowSchema10: false,
			errCode:       codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			name: "Fails_if_writing_1_0_model_because_it_will_be_interpreted_as_1_1",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": typesystem.This(),
						},
					},
				},
			},
			allowSchema10: true,
			errCode:       codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			name: "Works_if_no_schema_version",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										typesystem.WildcardRelationReference("user"),
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "self_referencing_type_restriction_with_entrypoint",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustParse(`
				type user

				type document
				  relations
				    define editor: [user] as self
				    define viewer: [document#viewer] as self or editor
				`),
				SchemaVersion: typesystem.SchemaVersion1_1,
			},
		},
		{
			name: "self_referencing_type_restriction_without_entrypoint_1",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustParse(`
				type user
				type document
				  relations
				    define viewer: [document#viewer] as self
				`),
				SchemaVersion: typesystem.SchemaVersion1_1,
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			name: "self_referencing_type_restriction_without_entrypoint_2",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustParse(`
				type user
				type document
				  relations
				    define editor: [user] as self
				    define viewer: [document#viewer] as self and editor
				`),
				SchemaVersion: typesystem.SchemaVersion1_1,
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			name: "self_referencing_type_restriction_without_entrypoint_3",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustParse(`
				type user
				type document
				  relations
				    define restricted: [user] as self
				    define viewer: [document#viewer] as self but not restricted
				`),
				SchemaVersion: typesystem.SchemaVersion1_1,
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			name: "rewritten_relation_in_intersection_unresolvable",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustParse(`
				type user

				type document
				  relations
				    define admin: [user] as self
				    define action1 as admin and action2 and action3
				    define action2 as admin and action1 and action3
				    define action3 as admin and action1 and action2
				`),
				SchemaVersion: typesystem.SchemaVersion1_1,
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			name: "direct_relationship_with_entrypoint",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustParse(`
				type user

				type document
				  relations
				    define viewer: [user] as self
				`),
			},
		},
		{
			name: "computed_relationship_with_entrypoint",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustParse(`
				type user

				type document
				  relations
				    define editor: [user] as self
				    define viewer as editor
				`),
			},
		},
		{
			name: "rewritten_relation_in_exclusion_unresolvable",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustParse(`
				type user

				type document
				  relations
				    define admin: [user] as self
				    define action1 as admin but not action2
				    define action2 as admin but not action3
				    define action3 as admin but not action1
				`),
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			name: "no_entrypoint_3a",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustParse(`
				type user

				type document
				  relations
				    define viewer: [document#viewer] as self and editor
				    define editor: [user] as self
				`),
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			name: "no_entrypoint_3b",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustParse(`
				type user

				type document
				  relations
				    define viewer: [document#viewer] as self but not editor
				    define editor: [user] as self
				`),
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			name: "no_entrypoint_4",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustParse(`
				type user

				type folder
				  relations
				    define parent: [document] as self
				    define viewer as editor from parent

				type document
				  relations
				    define parent: [folder] as self
				    define editor as viewer
				    define viewer as editor from parent
				`),
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			name: "self_referencing_type_restriction_with_entrypoint_1",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustParse(`
				type user

				type document
				  relations
				    define restricted: [user] as self
				    define editor: [user] as self
				    define viewer: [document#viewer] as self or editor
				    define can_view as viewer but not restricted
				    define can_view_actual as can_view
				`),
			},
		},
		{
			name: "self_referencing_type_restriction_with_entrypoint_2",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustParse(`
				type user

				type document
				  relations
				    define editor: [user] as self
				    define viewer: [document#viewer] as self or editor
				`),
			},
		},
		{
			name: "relation_with_union_of_ttu_rewrites",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustParse(`
				type user
				type org
				  relations
				    define admin: [user] as self
				    define member: [user] as self
				type group
				  relations
				    define member: [user] as self
				type feature
				  relations
				    define accessible as admin from subscriber_org or member from subscriber_group
				    define subscriber_group: [group] as self
				    define subscriber_org: [org] as self
				`),
			},
		},
		{
			name: "type_name_is_empty_string",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "",
					},
				},
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			name: "relation_name_is_empty_string",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
						Relations: map[string]*openfgav1.Userset{
							"": typesystem.This(),
						},
					},
					{
						Type: "other",
					},
				},
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			name: "many_circular_computed_relations",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustParse(`
				type user

				type canvas
				  relations
					define can_edit as editor or owner
					define editor: [user, account#member] as self
					define owner: [user] as self
					define viewer: [user, account#member] as self
	  
				type account
				  relations
					define admin: [user] as self or member or super_admin or owner
					define member: [user] as self or owner or admin or super_admin
					define owner: [user] as self
					define super_admin: [user] as self or admin or member
				`),
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			name: "circular_relations_involving_intersection",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustParse(`
				type user

				type other
				  relations
					define x: [user] as self and y
					define y: [user] as self and z
					define z: [user] as self or x
				`),
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			name: "circular_relations_involving_exclusion",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustParse(`
				type user

				type other
				  relations
					define x: [user] as self but not y
					define y: [user] as self but not z
					define z: [user] as self or x
				`),
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
	}

	ctx := context.Background()
	logger := logger.NewNoopLogger()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := commands.NewWriteAuthorizationModelCommand(datastore, logger)
			resp, err := cmd.Execute(ctx, test.request)
			status, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, test.errCode, status.Code())

			if err == nil {
				_, err = ulid.Parse(resp.AuthorizationModelId)
				require.NoError(t, err)
			}
		})
	}
}
