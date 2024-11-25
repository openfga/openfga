package commands

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/internal/server/config"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestWriteCommand(t *testing.T) {
	const (
		storeID                   = "01JCC8Z5S039R3X661KQGTNAFG"
		modelID                   = "01JCC8ZD4X84K2W0H0ZA5AQ947"
		maxTuplesInWriteOperation = 10
	)

	items := make([]*openfgav1.TupleKeyWithoutCondition, maxTuplesInWriteOperation+1)
	for i := 0; i < maxTuplesInWriteOperation+1; i++ {
		items[i] = &openfgav1.TupleKeyWithoutCondition{
			Object:   "document:" + testutils.CreateRandomString(4),
			Relation: "viewer",
			User:     "user:" + testutils.CreateRandomString(4),
		}
	}
	model := parser.MustTransformDSLToProto(`
	model
		schema 1.1
	type user
	type org
		relations
			define viewer: [user]
	type document
		relations
			define writer: [user]
			define viewer: [user, user:* with condition1]
			define owner: [user, user with condition1]
		
			define require_condition: [user with condition1]
			define implicit: [document#implicit]

			define computed: viewer
			define union: viewer or writer
			define intersection: viewer and writer
			define difference: viewer but not writer

			define org: [org]
			define ttu: viewer from org

	condition condition1(param1: string) {
		param1 == 'ok'
	}`)

	contextStructGood := testutils.MustNewStruct(t, map[string]interface{}{"param1": "ok"})
	contextStructBad := testutils.MustNewStruct(t, map[string]interface{}{"param1": "ok", "param2": 1})
	contextStructExceedsLimit := testutils.MustNewStruct(t, map[string]any{
		"param1": testutils.CreateRandomString(config.DefaultWriteContextByteLimit + 1),
	})

	var tests = []struct {
		name             string
		setMock          func(*mockstorage.MockOpenFGADatastore)
		deletes          *openfgav1.WriteRequestDeletes
		writes           *openfgav1.WriteRequestWrites
		expectedError    error
		expectedResponse *openfgav1.WriteResponse
	}{
		{
			name:          "nil_for_deletes_and_writes",
			setMock:       func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
			deletes:       nil,
			writes:        nil,
			expectedError: serverErrors.ErrInvalidWriteInput,
		},
		{
			name:    "empty_deletes_and_writes",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{},
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{},
			},
			expectedError: serverErrors.ErrInvalidWriteInput,
		},
		{
			name:    "duplicate_deletes",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{items[0], items[1], items[0]},
			},
			expectedError: serverErrors.DuplicateTupleInWrite(items[0]),
		},
		{
			name: "duplicate_writes",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.TupleKeyWithoutConditionToTupleKey(items[0]),
					tuple.TupleKeyWithoutConditionToTupleKey(items[1]),
					tuple.TupleKeyWithoutConditionToTupleKey(items[0]),
				},
			},
			expectedError: serverErrors.DuplicateTupleInWrite(items[0]),
		},
		{
			name: "same_item_appeared_in_writes_and_deletes",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{items[2], items[1]},
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.TupleKeyWithoutConditionToTupleKey(items[0]),
					tuple.TupleKeyWithoutConditionToTupleKey(items[1]),
				},
			},
			expectedError: serverErrors.DuplicateTupleInWrite(items[1]),
		},
		{
			name: "too_many_items_writes_and_deletes",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: items[:5],
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: tuple.TupleKeysWithoutConditionToTupleKeys(items[5:]...),
			},
			expectedError: serverErrors.ExceededEntityLimit("write operations", maxTuplesInWriteOperation),
		},
		{
			name: "write_failure_with_type_in_object_not_found",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "unknown:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause: &tuple.TypeNotFoundError{TypeName: "unknown"},
					TupleKey: &openfgav1.TupleKey{
						Object:   "unknown:1",
						Relation: "viewer",
						User:     "user:maria",
					},
				},
			),
		},
		{
			name: "write_failure_with_type_in_user_not_found",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "group:eng#member",
				}},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause: &tuple.TypeNotFoundError{TypeName: "group"},
					TupleKey: &openfgav1.TupleKey{
						Object:   "document:1",
						Relation: "viewer",
						User:     "group:eng#member",
					},
				},
			),
		},
		{
			name: "write_failure_with_relation_in_user_not_found",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria#member",
				}},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause: &tuple.RelationNotFoundError{
						TypeName: "user",
						Relation: "member",
					},
					TupleKey: &openfgav1.TupleKey{
						Object:   "document:1",
						Relation: "viewer",
						User:     "user:maria#member",
					},
				},
			),
		},
		{
			name: "write_failure_with_missing_relation",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "",
					User:     "user:maria",
				}},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause: fmt.Errorf("the 'relation' field is malformed"),
					TupleKey: &openfgav1.TupleKey{
						Object:   "document:1",
						Relation: "",
						User:     "user:maria",
					},
				},
			),
		},
		{
			name: "write_failure_with_relation_not_found",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "unknown",
					User:     "user:maria",
				}},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause: &tuple.RelationNotFoundError{TypeName: "document", Relation: "unknown"},
					TupleKey: &openfgav1.TupleKey{
						Object:   "document:1",
						Relation: "unknown",
						User:     "user:maria",
					},
				},
			),
		},
		{
			name: "write_failure_with_invalid_user",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "",
				}},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause: fmt.Errorf("the 'user' field is malformed"),
					TupleKey: &openfgav1.TupleKey{
						Object:   "document:1",
						Relation: "viewer",
						User:     "",
					},
				},
			),
		},
		{
			name: "write_failure_with_invalid_user_wildcard",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "writer",
					User:     "user:*",
				}},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause: fmt.Errorf("the typed wildcard 'user:*' is not an allowed type restriction for 'document#writer'"),
					TupleKey: &openfgav1.TupleKey{
						Object:   "document:1",
						Relation: "writer",
						User:     "user:*",
					},
				},
			),
		},
		{
			name: "write_failure_with_attempt_to_write_union",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "union",
					User:     "user:maria",
				}},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause: fmt.Errorf("type 'user' is not an allowed type restriction for 'document#union"),
					TupleKey: &openfgav1.TupleKey{
						Object:   "document:1",
						Relation: "union",
						User:     "user:maria",
					},
				},
			),
		},
		{
			name: "write_failure_with_attempt_to_write_intersection",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "intersection",
					User:     "user:maria",
				}},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause: fmt.Errorf("type 'user' is not an allowed type restriction for 'document#intersection"),
					TupleKey: &openfgav1.TupleKey{
						Object:   "document:1",
						Relation: "intersection",
						User:     "user:maria",
					},
				},
			),
		},
		{
			name: "write_failure_with_attempt_to_write_difference",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "difference",
					User:     "user:maria",
				}},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause: fmt.Errorf("type 'user' is not an allowed type restriction for 'document#difference"),
					TupleKey: &openfgav1.TupleKey{
						Object:   "document:1",
						Relation: "difference",
						User:     "user:maria",
					},
				},
			),
		},
		{
			name: "write_failure_with_attempt_to_write_computed",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "computed",
					User:     "user:maria",
				}},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause: fmt.Errorf("type 'user' is not an allowed type restriction for 'document#computed"),
					TupleKey: &openfgav1.TupleKey{
						Object:   "document:1",
						Relation: "computed",
						User:     "user:maria",
					},
				},
			),
		},
		{
			name: "write_failure_with_attempt_to_write_incorrect_type_restriction",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "org:fga",
				}},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause: fmt.Errorf("type 'org' is not an allowed type restriction for 'document#viewer"),
					TupleKey: &openfgav1.TupleKey{
						Object:   "document:1",
						Relation: "viewer",
						User:     "org:fga",
					},
				},
			),
		},
		{
			name: "write_failure_with_attempt_to_write_ttu",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "ttu",
					User:     "user:maria",
				}},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause: fmt.Errorf("type 'user' is not an allowed type restriction for 'document#ttu"),
					TupleKey: &openfgav1.TupleKey{
						Object:   "document:1",
						Relation: "ttu",
						User:     "user:maria",
					},
				},
			),
		},
		{
			name: "write_failure_with_missing_object",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "",
					Relation: "viewer",
					User:     "user:maria",
				}},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause: fmt.Errorf("invalid 'object' field format"),
					TupleKey: &openfgav1.TupleKey{
						Object:   "",
						Relation: "viewer",
						User:     "user:maria",
					},
				},
			),
		},
		{
			name: "write_failure_with_invalid_object",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "invalid",
					Relation: "viewer",
					User:     "user:maria",
				}},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause: fmt.Errorf("invalid 'object' field format"),
					TupleKey: &openfgav1.TupleKey{
						Object:   "invalid",
						Relation: "viewer",
						User:     "user:maria",
					},
				},
			),
		},
		{
			name: "write_of_implicit_tuple_should_fail",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "implicit",
					User:     "document:1#implicit",
				}},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause:    fmt.Errorf("cannot write a tuple that is implicit"),
					TupleKey: tuple.NewTupleKey("document:1", "implicit", "document:1#implicit"),
				},
			),
		},
		{
			name: "delete_of_tuple_with_object_type_not_in_model_should_succeed",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any()).Return(nil)
			},
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{{
					Object:   "unknown:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
			},
			expectedResponse: &openfgav1.WriteResponse{},
		},
		{
			name: "delete_of_tuple_with_user_type_not_in_model_should_succeed",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any()).Return(nil)
			},
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "unknown:maria",
				}},
			},
			expectedResponse: &openfgav1.WriteResponse{},
		},
		{
			name: "delete_of_tuple_with_relation_not_in_model_should_succeed",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any()).Return(nil)
			},
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{{
					Object:   "document:1",
					Relation: "old_relation",
					User:     "user:maria",
				}},
			},
			expectedResponse: &openfgav1.WriteResponse{},
		},
		{
			name: "delete_of_implicit_tuple_should_succeed",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any()).Return(nil)
			},
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{{
					Object:   "document:1",
					Relation: "implicit",
					User:     "document:1#implicit",
				}},
			},
			expectedResponse: &openfgav1.WriteResponse{},
		},
		{
			name:    "delete_failure_with_invalid_user",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "",
				}},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause: fmt.Errorf("the 'user' field is malformed"),
					TupleKey: &openfgav1.TupleKey{
						Object:   "document:1",
						Relation: "viewer",
						User:     "",
					},
				},
			),
		},
		{
			name: "good_deletes_and_writes",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any()).Return(nil)
			},
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{items[0], items[1]},
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.TupleKeyWithoutConditionToTupleKey(items[2]),
					tuple.TupleKeyWithoutConditionToTupleKey(items[3]),
				},
			},
			expectedResponse: &openfgav1.WriteResponse{},
		},
		{
			name: "valid_with_required_condition",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any()).Return(nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:*",
					Condition: &openfgav1.RelationshipCondition{
						Name:    "condition1",
						Context: contextStructGood,
					},
				}},
			},
			expectedResponse: &openfgav1.WriteResponse{},
		},
		{
			name: "valid_without_optional_condition",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any()).Return(nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "owner",
					User:     "user:jon",
				}},
			},
			expectedResponse: &openfgav1.WriteResponse{},
		},
		{
			name: "valid_with_optional_condition",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any()).Return(nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "owner",
					User:     "user:jon",
					Condition: &openfgav1.RelationshipCondition{
						Name:    "condition1",
						Context: contextStructGood,
					},
				}},
			},
			expectedResponse: &openfgav1.WriteResponse{},
		},
		{
			name: "invalid_condition_missing",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "require_condition",
					User:     "user:maria",
				}},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidConditionalTupleError{
					Cause: fmt.Errorf("condition is missing"),
					TupleKey: &openfgav1.TupleKey{
						Object:   "document:1",
						Relation: "require_condition",
						User:     "user:maria",
					},
				},
			),
		},
		{
			name: "invalid_condition",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:2",
					Relation: "writer",
					User:     "user:jon",
					Condition: &openfgav1.RelationshipCondition{
						Name:    "condition1",
						Context: contextStructGood,
					},
				}},
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
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:2",
					Relation: "viewer",
					User:     "user:*",
					Condition: &openfgav1.RelationshipCondition{
						Name:    "condition1",
						Context: contextStructBad,
					},
				}},
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
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:*",
					Condition: &openfgav1.RelationshipCondition{
						Name:    "condition2",
						Context: contextStructGood,
					},
				}},
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
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:*",
					Condition: &openfgav1.RelationshipCondition{
						Name:    "condition1",
						Context: contextStructExceedsLimit,
					},
				}},
			},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause: fmt.Errorf("condition context size limit exceeded: %d bytes exceeds %d bytes", proto.Size(contextStructExceedsLimit), config.DefaultWriteContextByteLimit),
					TupleKey: &openfgav1.TupleKey{
						Object:   "document:1",
						Relation: "viewer",
						User:     "user:*",
						Condition: &openfgav1.RelationshipCondition{
							Name:    "condition1",
							Context: contextStructExceedsLimit,
						},
					},
				},
			),
		},
		{
			name: "error_model_schema_not_supported_when_writing",
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				model10 := parser.MustTransformDSLToProto(`
					model
						schema 1.0
					type user`)
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Return(model10, nil)
			},
			expectedError: serverErrors.ValidationError(typesystem.ErrInvalidSchemaVersion),
		},
		{
			name: "error_model_not_found",
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Return(nil, storage.ErrNotFound)
			},
			expectedError: serverErrors.AuthorizationModelNotFound(modelID),
		},
		{
			name: "error_from_database_when_reading_model",
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Return(nil, errors.New("error"))
			},
			expectedError: serverErrors.NewInternalError("", fmt.Errorf("error")),
		},
		{
			name: "error_transactional_write_failed_from_database_when_writing",
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Return(model, nil)
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any()).Return(storage.ErrTransactionalWriteFailed)
			},
			expectedError: status.Error(codes.Aborted, storage.ErrTransactionalWriteFailed.Error()),
		},
		{
			name: "error_invalid_write_input_from_database_when_writing",
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Return(model, nil)
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any()).Return(storage.ErrInvalidWriteInput)
			},
			expectedError: serverErrors.WriteFailedDueToInvalidInput(storage.ErrInvalidWriteInput),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockController := gomock.NewController(t)
			defer mockController.Finish()
			mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
			mockDatastore.EXPECT().MaxTuplesPerWrite().AnyTimes().Return(maxTuplesInWriteOperation)
			test.setMock(mockDatastore)

			resp, err := NewWriteCommand(mockDatastore).Execute(context.Background(), &openfgav1.WriteRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Writes:               test.writes,
				Deletes:              test.deletes,
			})
			if test.expectedError != nil {
				require.Nil(t, resp)
				require.Error(t, err)
				require.ErrorContains(t, err, test.expectedError.Error())
				return
			}
			require.NoError(t, err)

			cmpOpts := []cmp.Option{
				cmpopts.IgnoreUnexported(openfgav1.WriteResponse{}),
				protocmp.Transform(),
			}
			if diff := cmp.Diff(test.expectedResponse, resp, cmpOpts...); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
