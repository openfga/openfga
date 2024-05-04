package commands

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/internal/server/config"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestValidateNoDuplicatesAndCorrectSize(t *testing.T) {
	type test struct {
		name          string
		deletes       []*openfgav1.TupleKeyWithoutCondition
		writes        []*openfgav1.TupleKey
		expectedError error
	}

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	maxTuplesInWriteOp := 10
	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().MaxTuplesPerWrite().AnyTimes().Return(maxTuplesInWriteOp)

	items := make([]*openfgav1.TupleKeyWithoutCondition, maxTuplesInWriteOp+1)
	for i := 0; i < maxTuplesInWriteOp+1; i++ {
		items[i] = &openfgav1.TupleKeyWithoutCondition{
			Object:   fmt.Sprintf("%s:1", testutils.CreateRandomString(459)),
			Relation: testutils.CreateRandomString(50),
			User:     testutils.CreateRandomString(512),
		}
	}

	cmd := NewWriteCommand(mockDatastore)

	tests := []test{
		{
			name:    "empty_deletes_and_writes",
			deletes: []*openfgav1.TupleKeyWithoutCondition{},
			writes:  []*openfgav1.TupleKey{},
		},
		{
			name:    "good_deletes_and_writes",
			deletes: []*openfgav1.TupleKeyWithoutCondition{items[0], items[1]},
			writes: []*openfgav1.TupleKey{
				tuple.TupleKeyWithoutConditionToTupleKey(items[2]),
				tuple.TupleKeyWithoutConditionToTupleKey(items[3]),
			},
		},
		{
			name:          "duplicate_deletes",
			deletes:       []*openfgav1.TupleKeyWithoutCondition{items[0], items[1], items[0]},
			writes:        []*openfgav1.TupleKey{},
			expectedError: serverErrors.DuplicateTupleInWrite(items[0]),
		},
		{
			name:    "duplicate_writes",
			deletes: []*openfgav1.TupleKeyWithoutCondition{},
			writes: []*openfgav1.TupleKey{
				tuple.TupleKeyWithoutConditionToTupleKey(items[0]),
				tuple.TupleKeyWithoutConditionToTupleKey(items[1]),
				tuple.TupleKeyWithoutConditionToTupleKey(items[0]),
			},
			expectedError: serverErrors.DuplicateTupleInWrite(items[0]),
		},
		{
			name:    "same_item_appeared_in_writes_and_deletes",
			deletes: []*openfgav1.TupleKeyWithoutCondition{items[2], items[1]},
			writes: []*openfgav1.TupleKey{
				tuple.TupleKeyWithoutConditionToTupleKey(items[0]),
				tuple.TupleKeyWithoutConditionToTupleKey(items[1]),
			},
			expectedError: serverErrors.DuplicateTupleInWrite(items[1]),
		},
		{
			name:          "too_many_items_writes_and_deletes",
			deletes:       items[:5],
			writes:        tuple.TupleKeysWithoutConditionToTupleKeys(items[5:]...),
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
		deletes       *openfgav1.WriteRequestDeletes
		writes        *openfgav1.WriteRequestWrites
		expectedError error
	}

	implicitTk := tuple.NewTupleKey("document:1", "viewer", "document:1#viewer")
	implicitTkUnconditioned := &openfgav1.TupleKeyWithoutCondition{
		Object:   implicitTk.GetObject(),
		Relation: implicitTk.GetRelation(),
		User:     implicitTk.GetUser(),
	}
	badItemTk := &openfgav1.TupleKey{
		Object:   "document:1",
		Relation: "viewer",
		User:     "",
	}
	badItemUnconditionedTk := &openfgav1.TupleKeyWithoutCondition{
		Object:   badItemTk.GetObject(),
		Relation: badItemTk.GetRelation(),
		User:     badItemTk.GetUser(),
	}

	tests := []test{
		{
			name:          "nil_for_deletes_and_writes",
			deletes:       nil,
			writes:        nil,
			expectedError: serverErrors.InvalidWriteInput,
		},
		{
			name: "write_failure_with_invalid_user",
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{},
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{badItemTk},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause:    fmt.Errorf("the 'user' field is malformed"),
					TupleKey: badItemTk,
				},
			),
		},
		{
			name: "delete_failure_with_invalid_user",
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{badItemUnconditionedTk},
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause:    fmt.Errorf("the 'user' field is malformed"),
					TupleKey: badItemTk,
				},
			),
		},
		{
			name: "write_of_implicit_tuple_should_fail",
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{implicitTk},
			},
			deletes: nil,
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause:    fmt.Errorf("cannot write a tuple that is implicit"),
					TupleKey: implicitTk,
				},
			),
		},
		{
			name: "delete_of_implicit_tuple_should_succeed",
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{implicitTkUnconditioned},
			},
			writes:        nil,
			expectedError: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockController := gomock.NewController(t)
			defer mockController.Finish()
			maxTuplesInWriteOp := 10
			mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
			mockDatastore.EXPECT().MaxTuplesPerWrite().AnyTimes().Return(maxTuplesInWriteOp)
			cmd := NewWriteCommand(mockDatastore)

			if test.writes != nil && len(test.writes.GetTupleKeys()) > 0 {
				mockDatastore.EXPECT().
					ReadAuthorizationModel(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(&openfgav1.AuthorizationModel{
						SchemaVersion: typesystem.SchemaVersion1_1,
						TypeDefinitions: parser.MustTransformDSLToProto(`
								model
									schema 1.1
								type user
								
								type document
									relations
										define viewer: [document#viewer]`).GetTypeDefinitions(),
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

func TestTransactionalWriteFailedError(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	mockDatastore.EXPECT().MaxTuplesPerWrite().AnyTimes().Return(10)

	mockDatastore.EXPECT().
		ReadAuthorizationModel(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(
			testutils.MustTransformDSLToProtoWithID(`model
				schema 1.1
				type user

				type document
					relations
						define viewer: [user]`), nil)

	mockDatastore.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(storage.ErrTransactionalWriteFailed)

	cmd := NewWriteCommand(mockDatastore)

	resp, err := cmd.Execute(context.Background(), &openfgav1.WriteRequest{
		StoreId: ulid.Make().String(),
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:jon",
				},
			},
		},
	})

	require.ErrorIs(t, err, status.Error(codes.Aborted, storage.ErrTransactionalWriteFailed.Error()))
	require.Nil(t, resp)
}

func TestValidateConditionsInTuples(t *testing.T) {
	type test struct {
		name          string
		tuple         *openfgav1.TupleKey
		expectedError error
	}

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
					"owner":  typesystem.This(),
					"writer": typesystem.This(),
					"viewer": typesystem.This(),
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"owner": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								typesystem.DirectRelationReference("user", ""),
								typesystem.ConditionedRelationReference(
									typesystem.DirectRelationReference("user", ""),
									"condition1",
								),
							},
						},
						"writer": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								typesystem.DirectRelationReference("user", ""),
							},
						},
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
	}

	contextStructGood := testutils.MustNewStruct(t, map[string]interface{}{"param1": "ok"})
	contextStructBad := testutils.MustNewStruct(t, map[string]interface{}{"param1": "ok", "param2": 1})

	contextStructExceedesLimit := testutils.MustNewStruct(t, map[string]any{
		"param1": testutils.CreateRandomString(config.DefaultWriteContextByteLimit + 1),
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().MaxTuplesPerWrite().AnyTimes().Return(10)
	mockDatastore.EXPECT().
		ReadAuthorizationModel(gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes().
		Return(model, nil)

	cmd := NewWriteCommand(mockDatastore)

	tests := []test{
		{
			name: "valid_with_required_condition",
			tuple: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:*",
				Condition: &openfgav1.RelationshipCondition{
					Name:    "condition1",
					Context: contextStructGood,
				},
			},
		},
		{
			name: "valid_without_optional_condition",
			tuple: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "owner",
				User:     "user:jon",
			},
		},
		{
			name: "valid_with_optional_condition",
			tuple: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "owner",
				User:     "user:jon",
				Condition: &openfgav1.RelationshipCondition{
					Name:    "condition1",
					Context: contextStructGood,
				},
			},
		},
		{
			name: "invalid_condition_missing",
			tuple: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:*",
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidConditionalTupleError{
					Cause: fmt.Errorf("condition is missing"),
					TupleKey: &openfgav1.TupleKey{
						Object:   "document:1",
						Relation: "viewer",
						User:     "user:*",
					},
				},
			),
		},
		{
			name: "invalid_condition",
			tuple: &openfgav1.TupleKey{
				Object:   "document:2",
				Relation: "writer",
				User:     "user:jon",
				Condition: &openfgav1.RelationshipCondition{
					Name:    "condition1",
					Context: contextStructGood,
				},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidConditionalTupleError{
					Cause: fmt.Errorf("invalid condition for type restriction"),
					TupleKey: &openfgav1.TupleKey{
						Object:   "document:2",
						Relation: "writer",
						User:     "user:jon",
						Condition: &openfgav1.RelationshipCondition{
							Name:    "condition1",
							Context: contextStructGood,
						},
					},
				},
			),
		},
		{
			name: "invalid_condition_parameters",
			tuple: &openfgav1.TupleKey{
				Object:   "document:2",
				Relation: "viewer",
				User:     "user:*",
				Condition: &openfgav1.RelationshipCondition{
					Name:    "condition1",
					Context: contextStructBad,
				},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidConditionalTupleError{
					Cause: fmt.Errorf("found invalid context parameter: param2"),
					TupleKey: &openfgav1.TupleKey{
						Object:   "document:2",
						Relation: "viewer",
						User:     "user:*",
						Condition: &openfgav1.RelationshipCondition{
							Name:    "condition1",
							Context: contextStructBad,
						},
					},
				},
			),
		},
		{
			name: "undefined_condition",
			tuple: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:*",
				Condition: &openfgav1.RelationshipCondition{
					Name:    "condition2",
					Context: contextStructGood,
				},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidConditionalTupleError{
					Cause: fmt.Errorf("undefined condition"),
					TupleKey: &openfgav1.TupleKey{
						Object:   "document:1",
						Relation: "viewer",
						User:     "user:*",
						Condition: &openfgav1.RelationshipCondition{
							Name:    "condition2",
							Context: contextStructGood,
						},
					},
				},
			),
		},
		{
			name: "condition_context_exceeds_limit",
			tuple: &openfgav1.TupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:*",
				Condition: &openfgav1.RelationshipCondition{
					Name:    "condition1",
					Context: contextStructExceedesLimit,
				},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause: fmt.Errorf("condition context size limit exceeded: %d bytes exceeds %d bytes", proto.Size(contextStructExceedesLimit), config.DefaultWriteContextByteLimit),
					TupleKey: &openfgav1.TupleKey{
						Object:   "document:1",
						Relation: "viewer",
						User:     "user:*",
						Condition: &openfgav1.RelationshipCondition{
							Name:    "condition1",
							Context: contextStructExceedesLimit,
						},
					},
				},
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := cmd.validateWriteRequest(context.Background(), &openfgav1.WriteRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: model.GetId(),
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						test.tuple,
					},
				},
			})

			if test.expectedError != nil {
				require.ErrorIs(t, err, test.expectedError)
				require.ErrorContains(t, err, test.expectedError.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
