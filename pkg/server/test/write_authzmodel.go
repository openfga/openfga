package test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
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
		err           error
	}{
		{
			name: "fails_if_too_many_types",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId:         storeID,
				TypeDefinitions: items,
				SchemaVersion:   typesystem.SchemaVersion1_1,
			},
			allowSchema10: false,
			err:           serverErrors.ExceededEntityLimit("type definitions in an authorization model", datastore.MaxTypesPerAuthorizationModel()),
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
			err:           serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{ObjectType: "repo", Relation: "owner", Cause: typesystem.ErrInvalidUsersetRewrite}),
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
			err: serverErrors.InvalidAuthorizationModelInput(
				errors.New("the assignable relation 'reader' in object type 'document' must contain at least one relation type"),
			),
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
			err:           serverErrors.InvalidAuthorizationModelInput(typesystem.AssignableRelationError("document", "reader")),
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{
				ObjectType: "document",
				Relation:   "viewer",
				Cause:      typesystem.ErrNoEntrypoints},
			),
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{
				ObjectType: "document",
				Relation:   "viewer",
				Cause:      typesystem.ErrNoEntrypoints,
			}),
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{
				ObjectType: "document",
				Relation:   "viewer",
				Cause:      typesystem.ErrNoEntrypoints,
			}),
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{
				ObjectType: "document",
				Relation:   "action1",
				Cause:      typesystem.ErrNoEntryPointsLoop,
			}),
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{
				ObjectType: "document",
				Relation:   "action1",
				Cause:      typesystem.ErrNoEntryPointsLoop,
			}),
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{
				ObjectType: "document",
				Relation:   "viewer",
				Cause:      typesystem.ErrNoEntrypoints,
			}),
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{
				ObjectType: "document",
				Relation:   "viewer",
				Cause:      typesystem.ErrNoEntrypoints,
			}),
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{
				ObjectType: "document",
				Relation:   "editor",
				Cause:      typesystem.ErrNoEntrypoints,
			}),
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
			err: serverErrors.InvalidAuthorizationModelInput(
				fmt.Errorf("the type name of a type definition cannot be an empty string"),
			),
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
			err: serverErrors.InvalidAuthorizationModelInput(
				fmt.Errorf("type 'user' defines a relation with an empty string for a name"),
			),
		},

		// conditions
		{
			name: "condition_valid",
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
										typesystem.ConditionedRelationReference(typesystem.WildcardRelationReference("user"), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "param1 == 'ok'",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"param1": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
							},
						},
					},
				},
			},
		},
		{
			name: "condition_fails_undefined",
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
										typesystem.ConditionedRelationReference(typesystem.WildcardRelationReference("user"), "invalid_condition_name"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "param1 == 'ok'",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"param1": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
							},
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				fmt.Errorf("condition invalid_condition_name is undefined for relation viewer"),
			),
		},
		{
			name: "condition_fails_syntax_error",
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
										typesystem.ConditionedRelationReference(typesystem.WildcardRelationReference("user"), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"param1": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
							},
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				fmt.Errorf("failed to compile condition expression: ERROR: <input>:1:1: Syntax error: mismatched input '<EOF>' expecting {'[', '{', '(', '.', '-', '!', 'true', 'false', 'null', NUM_FLOAT, NUM_INT, NUM_UINT, STRING, BYTES, IDENTIFIER}"),
			),
		},
		{
			name: "condition_fails_invalid_parameter_type",
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
										typesystem.ConditionedRelationReference(typesystem.WildcardRelationReference("user"), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "param1 == 'ok'",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"param1": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_UNSPECIFIED,
							},
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				fmt.Errorf("failed to decode parameter type for parameter 'param1': unknown condition parameter type `TYPE_NAME_UNSPECIFIED`"),
			),
		},
		{
			name: "condition_fails_invalid_output_type",
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
										typesystem.ConditionedRelationReference(typesystem.WildcardRelationReference("user"), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "param1",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"param1": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
							},
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				fmt.Errorf("expected a bool condition expression output, but got 'string'"),
			),
		},
		{
			name: "condition_fails_key_condition_name_mismatch",
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
										typesystem.ConditionedRelationReference(typesystem.WildcardRelationReference("user"), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "param1 == 'ok'",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"param1": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
							},
						},
					},
					"condition2": {
						Name:       "condition3",
						Expression: "param1 == 'ok'",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"param1": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
							},
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				fmt.Errorf("condition key 'condition2' does not match condition name 'condition3'"),
			),
		},
		{
			name: "condition_fails_missing_parameters",
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
										typesystem.ConditionedRelationReference(typesystem.WildcardRelationReference("user"), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "param1 == 'ok'",
						Parameters: nil,
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				fmt.Errorf("failed to compile condition expression: ERROR: <input>:1:1: undeclared reference to 'param1' (in container '')\n | param1 == 'ok'\n | ^"),
			),
		},
		{
			name: "condition_fails_missing_parameter",
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
										typesystem.ConditionedRelationReference(typesystem.WildcardRelationReference("user"), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "param1 == 'ok'",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"param2": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
							},
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				fmt.Errorf("failed to compile condition expression: ERROR: <input>:1:1: undeclared reference to 'param1' (in container '')\n | param1 == 'ok'\n | ^"),
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
