package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/pkg/server/commands"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
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
			// TODO remove, same as union_has_entrypoint
			name: "self_referencing_type_restriction_with_entrypoint",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user

					type document
						relations
							define editor: [user]
							define viewer: [document#viewer] or editor`).GetTypeDefinitions(),
				SchemaVersion: typesystem.SchemaVersion1_1,
			},
		},
		{
			// TODO remove, same as this_has_no_entrypoints
			name: "self_referencing_type_restriction_without_entrypoint_1",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user
					type document
						relations
							define viewer: [document#viewer]`).GetTypeDefinitions(),
				SchemaVersion: typesystem.SchemaVersion1_1,
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			// TODO remove, same as intersection_has_no_entrypoint_and_no_cycle
			name: "self_referencing_type_restriction_without_entrypoint_2",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user
					type document
						relations
							define editor: [user]
							define viewer: [document#viewer] and editor`).GetTypeDefinitions(),
				SchemaVersion: typesystem.SchemaVersion1_1,
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			// TODO remove - same as difference_has_no_entrypoint_and_no_cycle
			name: "self_referencing_type_restriction_without_entrypoint_3",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user
					type document
						relations
							define restricted: [user]
							define viewer: [document#viewer] but not restricted`).GetTypeDefinitions(),
				SchemaVersion: typesystem.SchemaVersion1_1,
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			// TODO remove - same as intersection_has_no_entrypoint_and_has_cycle_2
			name: "rewritten_relation_in_intersection_unresolvable",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user

					type document
						relations
							define admin: [user]
							define action1: admin and action2 and action3
							define action2: admin and action1 and action3
							define action3: admin and action1 and action2`).GetTypeDefinitions(),
				SchemaVersion: typesystem.SchemaVersion1_1,
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			// TODO remove - same as this_has_entrypoints_through_user
			name: "direct_relationship_with_entrypoint",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user

					type document
						relations
							define viewer: [user]`).GetTypeDefinitions(),
			},
		},
		{
			// TODO remove - same as computed_relation_has_entrypoint_through_user
			name: "computed_relationship_with_entrypoint",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user

					type document
						relations
							define editor: [user]
							define viewer: editor`).GetTypeDefinitions(),
			},
		},
		{
			// TODO remove - same as difference_has_no_entrypoint_and_has_cycle
			name: "rewritten_relation_in_exclusion_unresolvable",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user

					type document
						relations
							define admin: [user]
							define action1: admin but not action2
							define action2: admin but not action3
							define action3: admin but not action1`).GetTypeDefinitions(),
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			// TODO remove - same as intersection_has_no_entrypoint_and_no_cycle
			name: "no_entrypoint_3a",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user

					type document
						relations
							define viewer: [document#viewer] and editor
							define editor: [user]`).GetTypeDefinitions(),
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			// TODO remove - same as difference_has_no_entrypoint_and_no_cycle
			name: "no_entrypoint_3b",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user

					type document
						relations
							define viewer: [document#viewer] but not editor
							define editor: [user]`).GetTypeDefinitions(),
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			// TODO this test is invalid - "editor from parent" is invalid - "folder#editor" is not defined
			// Replaced by computed_relation_has_no_entrypoints
			name: "no_entrypoint_4",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user

					type folder
						relations
							define parent: [document]
							define viewer: editor from parent

					type document
						relations
							define parent: [folder]
							define editor: viewer
							define viewer: editor from parent`).GetTypeDefinitions(),
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			// TODO remove - same as difference_has_entrypoints_and_no_cycle_2
			name: "self_referencing_type_restriction_with_entrypoint_1",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user

					type document
						relations
							define restricted: [user]
							define editor: [user]
							define viewer: [document#viewer] or editor
							define can_view: viewer but not restricted
							define can_view_actual: can_view`).GetTypeDefinitions(),
			},
		},
		{
			// TODO remove - same as union_has_entrypoint_through_user
			name: "self_referencing_type_restriction_with_entrypoint_2",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user

					type document
						relations
							define editor: [user]
							define viewer: [document#viewer] or editor`).GetTypeDefinitions(),
			},
		},
		{
			name: "relation_with_union_of_ttu_rewrites",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user
					type org
						relations
							define admin: [user]
							define member: [user]
					type group
						relations
							define member: [user]
					type feature
						relations
							define accessible: admin from subscriber_org or member from subscriber_group
							define subscriber_group: [group]
							define subscriber_org: [org]`).GetTypeDefinitions(),
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
			// TODO remove - it's in TestHasCycle
			name: "many_circular_computed_relations",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user

					type canvas
						relations
							define can_edit: editor or owner
							define editor: [user, account#member]
							define owner: [user]
							define viewer: [user, account#member]

					type account
						relations
							define admin: [user] or member or super_admin or owner
							define member: [user] or owner or admin or super_admin
							define owner: [user]
							define super_admin: [user] or admin or member`).GetTypeDefinitions(),
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			// TODO remove - it's in TestHasCycle (intersection_and_union)
			name: "circular_relations_involving_intersection",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user

					type other
						relations
							define x: [user] and y
							define y: [user] and z
							define z: [user] or x`).GetTypeDefinitions(),
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			// TODO remove - it's in TestHasCycle (exclusion_and_union)
			name: "circular_relations_involving_exclusion",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user

					type other
						relations
							define x: [user] but not y
							define y: [user] but not z
							define z: [user] or x`).GetTypeDefinitions(),
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
		{
			name: "validate_model_size",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				SchemaVersion: testutils.CreateRandomString(
					serverconfig.DefaultMaxAuthorizationModelSizeInBytes,
				),
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user`).GetTypeDefinitions(),
			},
			errCode: codes.Code(openfgav1.ErrorCode_exceeded_entity_limit),
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
										typesystem.ConditionedRelationReference(
											typesystem.WildcardRelationReference("user"),
											"condition1",
										),
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
										typesystem.ConditionedRelationReference(
											typesystem.WildcardRelationReference("user"),
											"invalid_condition_name",
										),
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
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
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
										typesystem.ConditionedRelationReference(
											typesystem.WildcardRelationReference("user"),
											"condition1",
										),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "{",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"param1": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
							},
						},
					},
				},
			},
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
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
										typesystem.ConditionedRelationReference(
											typesystem.WildcardRelationReference("user"),
											"condition1",
										),
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
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
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
										typesystem.ConditionedRelationReference(
											typesystem.WildcardRelationReference("user"),
											"condition1",
										),
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
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
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
										typesystem.ConditionedRelationReference(
											typesystem.WildcardRelationReference("user"),
											"condition1",
										),
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
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
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
										typesystem.ConditionedRelationReference(
											typesystem.WildcardRelationReference("user"),
											"condition1",
										),
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
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
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
										typesystem.ConditionedRelationReference(
											typesystem.WildcardRelationReference("user"),
											"condition1",
										),
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
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
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
										typesystem.ConditionedRelationReference(
											typesystem.WildcardRelationReference("user"),
											"invalid_condition_name",
										),
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
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
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
										typesystem.ConditionedRelationReference(
											typesystem.WildcardRelationReference("user"),
											"condition1",
										),
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
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
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
										typesystem.ConditionedRelationReference(
											typesystem.WildcardRelationReference("user"),
											"condition1",
										),
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
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
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
										typesystem.ConditionedRelationReference(
											typesystem.WildcardRelationReference("user"),
											"condition1",
										),
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
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
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
										typesystem.ConditionedRelationReference(
											typesystem.WildcardRelationReference("user"),
											"condition1",
										),
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
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
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
										typesystem.ConditionedRelationReference(
											typesystem.WildcardRelationReference("user"),
											"condition1",
										),
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
			errCode: codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
		},
	}

	ctx := context.Background()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := commands.NewWriteAuthorizationModelCommand(datastore)
			resp, err := cmd.Execute(ctx, test.request)
			status, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, test.errCode, status.Code())

			if err == nil {
				_, err = ulid.Parse(resp.GetAuthorizationModelId())
				require.NoError(t, err)
			}
		})
	}
}
