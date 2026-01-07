package commands

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	mockstorage "github.com/openfga/openfga/internal/mocks"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestWriteAuthorizationModel(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()
	storeID := ulid.Make().String()
	const maxTypesPerAuthorizationModel = 100

	typeDefinitions := buildModelWithManyTypes(maxTypesPerAuthorizationModel)

	var tests = map[string]struct {
		setMock    func(*mockstorage.MockOpenFGADatastore)
		request    *openfgav1.WriteAuthorizationModelRequest
		errCode    codes.Code
		errMessage string
	}{
		`allow_schema_1.1`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), storeID, gomock.AssignableToTypeOf(&openfgav1.AuthorizationModel{})).Return(nil)
			},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
				},
				SchemaVersion: typesystem.SchemaVersion1_1,
			},
		},
		`allow_schema_1.2`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), storeID, gomock.AssignableToTypeOf(&openfgav1.AuthorizationModel{})).Return(nil)
			},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId:       storeID,
				SchemaVersion: typesystem.SchemaVersion1_2,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
						Metadata: &openfgav1.Metadata{
							Relations:  nil,
							Module:     "usermanagement",
							SourceInfo: nil,
						},
					},
				},
			},
		},
		`allow_empty_schema`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), storeID, gomock.AssignableToTypeOf(&openfgav1.AuthorizationModel{})).Return(nil)
			},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
				},
			},
		},
		`allow_direct_relation`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), storeID, gomock.AssignableToTypeOf(&openfgav1.AuthorizationModel{})).Return(nil)
			},
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
		`allow_computed_relation`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), storeID, gomock.AssignableToTypeOf(&openfgav1.AuthorizationModel{})).Return(nil)
			},
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
		`allow_relation_with_union_of_ttu_rewrites`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), storeID, gomock.AssignableToTypeOf(&openfgav1.AuthorizationModel{})).Return(nil)
			},
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
		`allow_ttu`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), storeID, gomock.AssignableToTypeOf(&openfgav1.AuthorizationModel{})).Return(nil)
			},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type folder
					relations
						define viewer: [user]
				type document
					relations
						define parent: [folder]
						define viewer: viewer from parent`).GetTypeDefinitions(),
			},
		},
		`allow_union_with_repeated_relations`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), storeID, gomock.AssignableToTypeOf(&openfgav1.AuthorizationModel{})).Return(nil)
			},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type document
					relations
						define editor: [user]
						define viewer: editor or editor`).GetTypeDefinitions(),
			},
		},
		`allow_intersection_with_repeated_relations`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), storeID, gomock.AssignableToTypeOf(&openfgav1.AuthorizationModel{})).Return(nil)
			},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type document
					relations
						define editor: [user]
						define viewer: editor and editor`).GetTypeDefinitions(),
			},
		},
		`allow_exclusion_with_repeated_relations`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), storeID, gomock.AssignableToTypeOf(&openfgav1.AuthorizationModel{})).Return(nil)
			},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type document
					relations
						define editor: [user]
						define viewer: editor but not editor`).GetTypeDefinitions(),
			},
		},
		`allow_ttu_as_long_as_one_computed_userset_type_is_valid`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), storeID, gomock.AssignableToTypeOf(&openfgav1.AuthorizationModel{})).Return(nil)
			},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type group
					relations
						define parent: [group, team]
						define viewer: reader from parent
				type team
					relations
						define reader: [user]`).GetTypeDefinitions(),
			},
		},
		`allow_self_referencing_type_restriction_with_entrypoint`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), storeID, gomock.AssignableToTypeOf(&openfgav1.AuthorizationModel{})).Return(nil)
			},
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
		`condition_valid`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), storeID, gomock.Any()).Return(nil)
			},
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
		`condition_fails_undefined`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
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
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "condition invalid_condition_name is undefined for relation viewer",
		},
		`condition_fails_syntax_error`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
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
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "failed to compile expression on condition 'condition1' - ERROR: condition1:1:2: Syntax error: mismatched input '<EOF>'",
		},
		`condition_fails_invalid_parameter_type`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
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
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "failed to compile expression on condition 'condition1' - failed to decode parameter type for parameter 'param1': unknown condition parameter type `TYPE_NAME_UNSPECIFIED`",
		},
		`condition_fails_invalid_output_type`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
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
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "failed to compile expression on condition 'condition1' - expected a bool condition expression output, but got 'string'",
		},
		`condition_fails_key_condition_name_mismatch`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
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
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "condition key 'condition2' does not match condition name 'condition3'",
		},
		`condition_fails_missing_parameters`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
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
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "failed to compile expression on condition 'condition1' - ERROR: condition1:1:1: undeclared reference to 'param1'",
		},
		`condition_fails_missing_parameter`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
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
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "failed to compile expression on condition 'condition1' - ERROR: condition1:1:1: undeclared reference to 'param1'",
		},
		`fail_if_writing_schema_1_0`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId:       storeID,
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": typesystem.This(),
						},
					},
				},
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "invalid schema version",
		},
		`fail_if_type_name_is_empty_string`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "",
					},
				},
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the type name of a type definition cannot be an empty string",
		},
		`fail_if_too_many_types`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId:         storeID,
				TypeDefinitions: typeDefinitions,
				SchemaVersion:   typesystem.SchemaVersion1_1,
			},
			errCode:    codes.Code(openfgav1.ErrorCode_exceeded_entity_limit),
			errMessage: "number of type definitions in an authorization model exceeds the allowed limit of 100",
		},
		`fail_if_a_relation_is_not_defined`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
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
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the definition of relation 'owner' in object type 'repo' is invalid",
		},
		`fail_if_type_info_metadata_is_omitted_in_1.1_model`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
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
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the assignable relation 'reader' in object type 'document' must contain at least one relation type",
		},
		`fail_if_relation_name_is_empty_string`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
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
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "type 'user' defines a relation with an empty string for a name",
		},
		`fail_if_no_entrypoint`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
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
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the definition of relation 'viewer' in object type 'document' is invalid: no entrypoints defined",
		},
		`fail_if_no_entrypoint_intersection`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
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
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the definition of relation 'viewer' in object type 'document' is invalid: no entrypoints defined",
		},
		`fail_if_no_entrypoint_exclusion`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
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
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the definition of relation 'viewer' in object type 'document' is invalid: no entrypoints defined",
		},
		`fail_if_loop_of_intersections`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
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
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the definition of relation 'action1' in object type 'document' is invalid: potential loop",
		},
		`fail_if_loop_of_exclusions`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
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
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the definition of relation 'action1' in object type 'document' is invalid: potential loop",
		},
		`fail_if_cycle`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
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
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the definition of relation 'x' in object type 'other' is invalid: an authorization model cannot contain a cycle",
		},
		`fail_model_size`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
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
			errCode:    codes.Code(openfgav1.ErrorCode_exceeded_entity_limit),
			errMessage: "model exceeds size limit: 262184 bytes vs 262144 bytes",
		},
		`fail_if_datastore_errors`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), storeID, gomock.AssignableToTypeOf(&openfgav1.AuthorizationModel{})).
					Return(fmt.Errorf("internal"))
			},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
				},
			},
			errCode:    codes.Code(openfgav1.InternalErrorCode_internal_error),
			errMessage: "Error writing authorization model configuration",
		},
		`fail_if_using_self_as_type_name`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user
					type self
					  relations
						define member: [user]
				`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the definition of type 'self' is invalid",
		},
		`fail_if_using_this_as_type_name`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user
					type this
					  relations
						define member: [user]
				`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the definition of type 'this' is invalid",
		},
		`fail_if_using_self_as_relation_name`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user
					type group
					  relations
						define self: [user]
				`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the definition of relation 'self' in object type 'group' is invalid: self and this are reserved keywords",
		},
		`fail_if_using_this_as_relation_name`: {
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user
					type group
					  relations
						define this: [user]
				`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the definition of relation 'this' in object type 'group' is invalid: self and this are reserved keywords",
		},
		`fail_if_computed_of_tupleset_not_defined_2`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user
					type group
						relations
							define group: group from group`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the 'group#group' relation is referenced in at least one tupleset and thus must be a direct relation",
		},
		`fail_no_entrypoints`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user
					type group
						relations
							define parent: [group]
							define viewer: viewer from parent`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the definition of relation 'viewer' in object type 'group' is invalid: no entrypoints defined",
		},
		`fail_no_entrypoints_2`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type group
						relations
							define viewer: [group#viewer]`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the definition of relation 'viewer' in object type 'group' is invalid: no entrypoints defined",
		},
		`fail_tupleset_1`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type org
					relations
						define member: [user]
				type group
					relations
						define parent: [org]
						define viewer: viewer from parent`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "undefined relation: viewer does not appear as a relation in any of the directly related user types [type:\"org\"]",
		},
		`fail_tupleset_2`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type group
					relations
						define parent: [group]
						define viewer: reader from parent`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "undefined relation: reader does not appear as a relation in any of the directly related user types [type:\"group\"]",
		},
		`fail_undefined_tupleset_relation`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type org
				type group
					relations
						define parent: [group]
						define viewer: viewer from org`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "'group#org' relation is undefined",
		},
		`fail_undefined_computed_relation_in_tupleset`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type org
				type group
					relations
						define parent: [group]
						define viewer: org from parent`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "undefined relation: org does not appear as a relation in any of the directly related user types [type:\"group\"]",
		},
		`fail_undefined_relation_in_userset`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type org
				type group
					relations
						define parent: [group, group#org]`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the relation type 'group#org' on 'parent' in object type 'group' is not valid",
		},
		`fail_no_entrypoints_3`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type org
					relations
						define viewer: [user]
				type group
					relations
						define parent: [group]
						define viewer: viewer from parent`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the definition of relation 'viewer' in object type 'group' is invalid: no entrypoints defined",
		},
		`fail_potential_loop`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type document
					relations
						define reader: writer
						define writer: reader`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the definition of relation 'reader' in object type 'document' is invalid: potential loop",
		},
		`fail_computed_relation_not_defined_in_tupleset`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type folder
					relations
						define parent: [folder] or parent from parent
						define viewer: [user] or viewer from parent`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the 'folder#parent' relation is referenced in at least one tupleset and thus must be a direct relation",
		},
		`fail_direct_relation_needed_1`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type folder
					relations
						define root: [folder]
						define parent: [folder] or root
						define viewer: [user] or viewer from parent`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the 'folder#parent' relation is referenced in at least one tupleset and thus must be a direct relation",
		},
		`fail_direct_relation_needed_2`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type folder
					relations
						define root: [folder]
						define parent: root
						define viewer: [user] or viewer from parent`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the 'folder#parent' relation is referenced in at least one tupleset and thus must be a direct relation",
		},
		`fail_invalid_type_restriction_on_tupleset`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type folder
					relations
						define root: [folder]
						define parent: [folder, folder#parent]
						define viewer: [user] or viewer from parent`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the relation type 'folder#parent' on 'parent' in object type 'folder' is not valid",
		},
		`fail_undefined_relation_2`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]
						define reader: member and allowed`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "'group#allowed' relation is undefined",
		},
		`fail_undefined_relation_in_union`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]
						define reader: member or allowed`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "'group#allowed' relation is undefined",
		},
		`fail_undefined_relation`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]
						define reader: allowed but not member`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "'group#allowed' relation is undefined",
		},
		`fail_undefined_relation_in_intersection`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]
						define reader: member but not allowed`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "'group#allowed' relation is undefine",
		},
		`fail_same_type_twice`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type user`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "an authorization model cannot contain duplicate types",
		},
		`fail_difference_includes_itself_in_subtract`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: [user] but not viewer`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the definition of relation 'viewer' in object type 'document' is invalid: invalid userset rewrite definition",
		},
		`fail_union_includes_itself`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: [user] or viewer`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the definition of relation 'viewer' in object type 'document' is invalid: invalid userset rewrite definition",
		},
		`fail_intersection_includes_itself`: {
			setMock: func(datastore *mockstorage.MockOpenFGADatastore) {},
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: [user] and viewer`).GetTypeDefinitions(),
			},
			errCode:    codes.Code(openfgav1.ErrorCode_invalid_authorization_model),
			errMessage: "the definition of relation 'viewer' in object type 'document' is invalid: invalid userset rewrite definition",
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			mockController := gomock.NewController(t)
			defer mockController.Finish()

			mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
			mockDatastore.EXPECT().MaxTypesPerAuthorizationModel().AnyTimes().Return(maxTypesPerAuthorizationModel)
			test.setMock(mockDatastore)

			resp, err := NewWriteAuthorizationModelCommand(mockDatastore).Execute(ctx, test.request)
			if test.errCode != 0 {
				require.NotEmpty(t, test.errMessage, "expected an error message to be set in the test", err.Error())
				status, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, test.errCode, status.Code())
				require.Contains(t, status.Message(), test.errMessage)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Empty(t, test.errMessage)
			_, err = ulid.Parse(resp.GetAuthorizationModelId())
			require.NoError(t, err)
		})
	}
}

func buildModelWithManyTypes(maxTypesPerAuthorizationModel int) []*openfgav1.TypeDefinition {
	items := make([]*openfgav1.TypeDefinition, maxTypesPerAuthorizationModel+1)
	items[0] = &openfgav1.TypeDefinition{
		Type: "user",
	}
	for i := 1; i <= maxTypesPerAuthorizationModel; i++ {
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
	return items
}
