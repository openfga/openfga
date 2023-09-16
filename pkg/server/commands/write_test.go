package commands

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestValidateNoDuplicatesAndCorrectSize(t *testing.T) {
	type test struct {
		name          string
		deletes       []*openfgav1.TupleKey
		writes        []*openfgav1.TupleKey
		expectedError error
	}

	logger := logger.NewNoopLogger()

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	maxTuplesInWriteOp := 10
	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().MaxTuplesPerWrite().AnyTimes().Return(maxTuplesInWriteOp)

	items := make([]*openfgav1.TupleKey, maxTuplesInWriteOp+1)
	for i := 0; i < maxTuplesInWriteOp+1; i++ {
		items[i] = &openfgav1.TupleKey{
			Object:   fmt.Sprintf("%s:1", testutils.CreateRandomString(459)),
			Relation: testutils.CreateRandomString(50),
			User:     testutils.CreateRandomString(512),
		}
	}

	cmd := NewWriteCommand(mockDatastore, logger)

	tests := []test{
		{
			name:    "empty_deletes_and_writes",
			deletes: []*openfgav1.TupleKey{},
			writes:  []*openfgav1.TupleKey{},
		},
		{
			name:    "good_deletes_and_writes",
			deletes: []*openfgav1.TupleKey{items[0], items[1]},
			writes:  []*openfgav1.TupleKey{items[2], items[3]},
		},
		{
			name:          "duplicate_deletes",
			deletes:       []*openfgav1.TupleKey{items[0], items[1], items[0]},
			writes:        []*openfgav1.TupleKey{},
			expectedError: serverErrors.DuplicateTupleInWrite(items[0]),
		},
		{
			name:          "duplicate_writes",
			deletes:       []*openfgav1.TupleKey{},
			writes:        []*openfgav1.TupleKey{items[0], items[1], items[0]},
			expectedError: serverErrors.DuplicateTupleInWrite(items[0]),
		},
		{
			name:          "same_item_appeared_in_writes_and_deletes",
			deletes:       []*openfgav1.TupleKey{items[2], items[1]},
			writes:        []*openfgav1.TupleKey{items[0], items[1]},
			expectedError: serverErrors.DuplicateTupleInWrite(items[1]),
		},
		{
			name:          "too_many_items_writes_and_deletes",
			deletes:       items[:5],
			writes:        items[5:],
			expectedError: serverErrors.ExceededEntityLimit("write operations", maxTuplesInWriteOp),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := cmd.validateNoDuplicatesAndCorrectSize(test.deletes, test.writes)
			require.ErrorIs(t, err, test.expectedError)
		})
	}
}

func TestValidateWriteRequest(t *testing.T) {
	type test struct {
		name          string
		deletes       []*openfgav1.WriteRequestTupleKey
		writes        []*openfgav1.WriteRequestTupleKey
		expectedError error
	}

	badItem := &openfgav1.TupleKey{
		Object:   fmt.Sprintf("%s:1", testutils.CreateRandomString(20)),
		Relation: testutils.CreateRandomString(50),
		User:     "",
	}
	badItemTk := tuple.ConvertTupleKeyToWriteTupleKey(badItem)

	tests := []test{
		{
			name:          "nil_for_deletes_and_writes",
			deletes:       nil,
			writes:        nil,
			expectedError: serverErrors.InvalidWriteInput,
		},
		{
			name:    "write_failure_with_invalid_user",
			deletes: []*openfgav1.WriteRequestTupleKey{},
			writes:  []*openfgav1.WriteRequestTupleKey{badItemTk},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause:    fmt.Errorf("the 'user' field is malformed"),
					TupleKey: badItem,
				},
			),
		},
		{
			name:    "delete_failure_with_invalid_user",
			deletes: []*openfgav1.WriteRequestTupleKey{badItemTk},
			writes:  []*openfgav1.WriteRequestTupleKey{},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause:    fmt.Errorf("the 'user' field is malformed"),
					TupleKey: badItem,
				},
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger := logger.NewNoopLogger()

			mockController := gomock.NewController(t)
			defer mockController.Finish()
			maxTuplesInWriteOp := 10
			mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
			mockDatastore.EXPECT().MaxTuplesPerWrite().AnyTimes().Return(maxTuplesInWriteOp)
			cmd := NewWriteCommand(mockDatastore, logger)

			if len(test.writes) > 0 {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), gomock.Any(), gomock.Any()).Return(&openfgav1.AuthorizationModel{
					SchemaVersion: typesystem.SchemaVersion1_1,
				}, nil)
			}

			ctx := context.Background()
			req := &openfgav1.WriteRequest{
				StoreId: "abcd123",
				Writes:  test.writes,
				Deletes: test.deletes,
			}

			err := cmd.validateWriteRequest(ctx, req)
			require.ErrorIs(t, err, test.expectedError)
		})
	}
}

func conditionedRelation(rel *openfgav1.RelationReference, condition string) *openfgav1.RelationReference {
	rel.Condition = condition
	return rel
}

func TestValidateWriteRequestWhenModelWithConditions(t *testing.T) {
	invalidConditionTK := tuple.NewTupleKeyWithCondition("document:budget", "viewer", "user:anne", &openfgav1.RelationshipCondition{
		ConditionName: "condition2",
		Context: &structpb.Struct{Fields: map[string]*structpb.Value{
			"x": {Kind: &structpb.Value_NumberValue{NumberValue: 2}},
		}},
	})
	undefinedConditionTK := tuple.NewTupleKeyWithCondition("document:budget", "viewer", "user:anne", &openfgav1.RelationshipCondition{
		ConditionName: "condition2",
		Context: &structpb.Struct{Fields: map[string]*structpb.Value{
			"x": {Kind: &structpb.Value_NumberValue{NumberValue: 2}},
		}},
	})
	missingConditionUserTK := tuple.NewTupleKeyWithCondition("document:budget", "viewer", "user:bob", nil)
	missingConditionUsersetTK := tuple.NewTupleKeyWithCondition("document:budget", "viewer", "group:finance#member", nil)
	missingConditionWildcardTK := tuple.NewTupleKeyWithCondition("document:budget", "viewer", "user:*", nil)

	undefinedParameterTK := tuple.NewTupleKeyWithCondition("document:budget", "viewer", "user:charles", &openfgav1.RelationshipCondition{
		ConditionName: "condition1",
		Context: &structpb.Struct{Fields: map[string]*structpb.Value{
			"z": {Kind: &structpb.Value_NumberValue{NumberValue: 2}},
		}},
	})
	invalidTypeStringTK := tuple.NewTupleKeyWithCondition("document:budget", "viewer", "user:charles", &openfgav1.RelationshipCondition{
		ConditionName: "condition1",
		Context: &structpb.Struct{Fields: map[string]*structpb.Value{
			"x": {Kind: &structpb.Value_StringValue{StringValue: "invalid"}},
		}},
	})
	invalidTypeNumberTK := tuple.NewTupleKeyWithCondition("document:budget", "viewer", "user:charles", &openfgav1.RelationshipCondition{
		ConditionName: "condition1",
		Context: &structpb.Struct{Fields: map[string]*structpb.Value{
			"x": {Kind: &structpb.Value_NumberValue{NumberValue: 1}},
		}},
	})

	type test struct {
		name          string
		model         *openfgav1.AuthorizationModel
		deletes       []*openfgav1.WriteRequestTupleKey
		writes        []*openfgav1.WriteRequestTupleKey
		expectedError error
	}

	tests := []test{
		{
			name: "success",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
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
										conditionedRelation(typesystem.DirectRelationReference("user", ""), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "x > 1",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"x": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_DOUBLE,
							},
						},
					},
				},
			},
			deletes: nil,
			writes: []*openfgav1.WriteRequestTupleKey{{
				User:     "user:anne",
				Relation: "viewer",
				Object:   "document:budget",
				Condition: &openfgav1.RelationshipCondition{
					ConditionName: "condition1",
					Context: &structpb.Struct{Fields: map[string]*structpb.Value{
						"x": {Kind: &structpb.Value_NumberValue{NumberValue: 2}},
					}},
				},
			}},
			expectedError: nil,
		},
		{
			name: "condition_for_userset_required_but_not_provided",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
						Relations: map[string]*openfgav1.Userset{
							"member": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										typesystem.DirectRelationReference("user", ""),
									},
								},
							},
						},
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
										conditionedRelation(typesystem.DirectRelationReference("group", "member"), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "x > 1",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"x": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_UINT,
							},
						},
					},
				},
			},
			deletes: nil,
			writes:  []*openfgav1.WriteRequestTupleKey{tuple.ConvertTupleKeyToWriteTupleKey(missingConditionUsersetTK)},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidConditionalTupleError{
					Cause:    fmt.Errorf("condition is missing"),
					TupleKey: missingConditionUsersetTK,
				},
			),
		},
		{
			name: "condition_for_user_required_but_not_provided",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
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
										conditionedRelation(typesystem.DirectRelationReference("user", ""), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "x > 1",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"x": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_UINT,
							},
						},
					},
				},
			},
			deletes: nil,
			writes:  []*openfgav1.WriteRequestTupleKey{tuple.ConvertTupleKeyToWriteTupleKey(missingConditionUserTK)},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidConditionalTupleError{
					Cause:    fmt.Errorf("condition is missing"),
					TupleKey: missingConditionUserTK,
				},
			),
		},
		{
			name: "condition_for_wildcard_required_but_not_provided",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
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
										conditionedRelation(typesystem.WildcardRelationReference("user"), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "x > 1",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"x": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_UINT,
							},
						},
					},
				},
			},
			deletes: nil,
			writes:  []*openfgav1.WriteRequestTupleKey{tuple.ConvertTupleKeyToWriteTupleKey(missingConditionWildcardTK)},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidConditionalTupleError{
					Cause:    fmt.Errorf("condition is missing"),
					TupleKey: missingConditionWildcardTK,
				},
			),
		},
		{
			name: "invalid_condition_for_type_restriction",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
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
										conditionedRelation(typesystem.DirectRelationReference("user", ""), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "x > 1",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"x": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_UINT,
							},
						},
					},
					"condition2": {
						Name:       "condition2",
						Expression: "y > 2",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"y": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_UINT,
							},
						},
					},
				},
			},
			deletes: nil,
			writes:  []*openfgav1.WriteRequestTupleKey{tuple.ConvertTupleKeyToWriteTupleKey(invalidConditionTK)},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidConditionalTupleError{
					Cause:    fmt.Errorf("invalid condition for type restriction"),
					TupleKey: invalidConditionTK,
				},
			),
		},
		{
			name: "undefined_condition",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
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
										conditionedRelation(typesystem.DirectRelationReference("user", ""), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "x > 1",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"x": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_UINT,
							},
						},
					},
				},
			},
			deletes: nil,
			writes:  []*openfgav1.WriteRequestTupleKey{tuple.ConvertTupleKeyToWriteTupleKey(undefinedConditionTK)},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidConditionalTupleError{
					Cause:    fmt.Errorf("undefined condition"),
					TupleKey: undefinedConditionTK,
				},
			),
		},
		{
			name: "undefined_parameter",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
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
										conditionedRelation(typesystem.DirectRelationReference("user", ""), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "x > 1",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"x": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_UINT,
							},
						},
					},
				},
			},
			deletes: nil,
			writes:  []*openfgav1.WriteRequestTupleKey{tuple.ConvertTupleKeyToWriteTupleKey(undefinedParameterTK)},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidConditionalTupleError{
					Cause:    fmt.Errorf("undefined parameter"),
					TupleKey: undefinedParameterTK,
				},
			),
		},
		{
			name: "invalid_type_for_parameter_string",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
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
										conditionedRelation(typesystem.DirectRelationReference("user", ""), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "x > 1",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"x": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_UINT,
							},
						},
					},
				},
			},
			deletes: nil,
			writes:  []*openfgav1.WriteRequestTupleKey{tuple.ConvertTupleKeyToWriteTupleKey(invalidTypeStringTK)},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidConditionalTupleError{
					Cause:    fmt.Errorf("invalid type for parameter"),
					TupleKey: invalidTypeStringTK,
				},
			),
		},
		{
			name: "invalid_type_for_parameter_number",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
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
										conditionedRelation(typesystem.DirectRelationReference("user", ""), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "x > 1",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"x": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
							},
						},
					},
				},
			},
			deletes: nil,
			writes:  []*openfgav1.WriteRequestTupleKey{tuple.ConvertTupleKeyToWriteTupleKey(invalidTypeNumberTK)},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidConditionalTupleError{
					Cause:    fmt.Errorf("invalid type for parameter"),
					TupleKey: invalidTypeStringTK,
				},
			),
		},
		// TODO more tests for remaining types
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ts, err := typesystem.NewAndValidate(context.Background(), test.model)
			require.NoError(t, err)
			err = validateConditionsInTuples(ts, test.deletes, test.writes)
			require.ErrorIs(t, err, test.expectedError)
		})
	}
}
