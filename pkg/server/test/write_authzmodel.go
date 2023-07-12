package test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
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
			err:           serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{ObjectType: "repo", Relation: "owner", Cause: typesystem.ErrInvalidUsersetRewrite}),
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
		{
			name: "self_referencing_type_restriction_with_entrypoint",
			request: &openfgapb.WriteAuthorizationModelRequest{
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
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustParse(`
				type user
				type document
				  relations
				    define viewer: [document#viewer] as self
				`),
				SchemaVersion: typesystem.SchemaVersion1_1,
			},
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{
				ObjectType: "document",
				Relation:   "viewer",
				Cause:      typesystem.ErrNoEntrypoints},
			),
		},
		{
			name: "self_referencing_type_restriction_without_entrypoint_2",
			request: &openfgapb.WriteAuthorizationModelRequest{
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{
				ObjectType: "document",
				Relation:   "viewer",
				Cause:      typesystem.ErrNoEntrypoints,
			}),
		},
		{
			name: "self_referencing_type_restriction_without_entrypoint_3",
			request: &openfgapb.WriteAuthorizationModelRequest{
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{
				ObjectType: "document",
				Relation:   "viewer",
				Cause:      typesystem.ErrNoEntrypoints,
			}),
		},
		{
			name: "rewritten_relation_in_intersection_unresolvable",
			request: &openfgapb.WriteAuthorizationModelRequest{
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{
				ObjectType: "document",
				Relation:   "action1",
				Cause:      typesystem.ErrNoEntryPointsLoop,
			}),
		},
		{
			name: "direct_relationship_with_entrypoint",
			request: &openfgapb.WriteAuthorizationModelRequest{
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
			request: &openfgapb.WriteAuthorizationModelRequest{
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
			request: &openfgapb.WriteAuthorizationModelRequest{
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{
				ObjectType: "document",
				Relation:   "action1",
				Cause:      typesystem.ErrNoEntryPointsLoop,
			}),
		},
		{
			name: "no_entrypoint_3a",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustParse(`
				type user

				type document
				  relations
				    define viewer: [document#viewer] as self and editor
				    define editor: [user] as self
				`),
			},
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{
				ObjectType: "document",
				Relation:   "viewer",
				Cause:      typesystem.ErrNoEntrypoints,
			}),
		},

		{
			name: "no_entrypoint_3b",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustParse(`
				type user

				type document
				  relations
				    define viewer: [document#viewer] as self but not editor
				    define editor: [user] as self
				`),
			},
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{
				ObjectType: "document",
				Relation:   "viewer",
				Cause:      typesystem.ErrNoEntrypoints,
			}),
		},
		{
			name: "no_entrypoint_4",
			request: &openfgapb.WriteAuthorizationModelRequest{
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{
				ObjectType: "document",
				Relation:   "editor",
				Cause:      typesystem.ErrNoEntrypoints,
			}),
		},
		{
			name: "self_referencing_type_restriction_with_entrypoint_1",
			request: &openfgapb.WriteAuthorizationModelRequest{
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
			request: &openfgapb.WriteAuthorizationModelRequest{
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
			request: &openfgapb.WriteAuthorizationModelRequest{
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
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "",
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				fmt.Errorf("the type name of a type definition cannot be an empty string"),
			),
		},
		{
			name: "relation_name_is_empty_string",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
						Relations: map[string]*openfgapb.Userset{
							"": typesystem.This(),
						},
					},
					{
						Type: "other",
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				fmt.Errorf("type 'user' defines a relation with an empty string for a name"),
			),
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
