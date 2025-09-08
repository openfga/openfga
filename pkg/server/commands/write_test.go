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
	"google.golang.org/protobuf/testing/protocmp"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

type writeOptionsMatcher struct {
	expectedOnDuplicateInsert storage.OnDuplicateInsert
	expectedOnMissingDelete   storage.OnMissingDelete
}

func (w *writeOptionsMatcher) Matches(x interface{}) bool {
	opts, ok := x.([]storage.TupleWriteOption)
	if !ok || len(opts) != 2 {
		return false
	}
	dut := storage.NewTupleWriteOptions(opts...)
	return dut.OnMissingDelete == w.expectedOnMissingDelete && dut.OnDuplicateInsert == w.expectedOnDuplicateInsert
}

func (w *writeOptionsMatcher) String() string {
	return fmt.Sprintf("is equal to %v %v", w.expectedOnDuplicateInsert, w.expectedOnDuplicateInsert)
}

func NewWriteOptsMatcher(insertOpts storage.OnDuplicateInsert, deleteOpts storage.OnMissingDelete) gomock.Matcher {
	return &writeOptionsMatcher{expectedOnDuplicateInsert: insertOpts, expectedOnMissingDelete: deleteOpts}
}

func TestWriteCommand(t *testing.T) {
	const (
		storeID                   = "01JCC8Z5S039R3X661KQGTNAFG"
		modelID                   = "01JCC8ZD4X84K2W0H0ZA5AQ947"
		maxTuplesInWriteOperation = 10
	)

	items := make([]*openfgav1.TupleKeyWithoutCondition, maxTuplesInWriteOperation+1)
	for i := 0; i < maxTuplesInWriteOperation+1; i++ {
		items[i] = &openfgav1.TupleKeyWithoutCondition{
			Object:   fmt.Sprintf("document:%d", i),
			Relation: "viewer",
			User:     fmt.Sprintf("user:%d", i),
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
		expectedError    string
		expectedResponse *openfgav1.WriteResponse
	}{
		{
			name:          "nil_for_deletes_and_writes",
			setMock:       func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
			deletes:       nil,
			writes:        nil,
			expectedError: "rpc error: code = Code(2003) desc = Invalid input. Make sure you provide at least one write, or at least one delete",
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
			expectedError: "rpc error: code = Code(2003) desc = Invalid input. Make sure you provide at least one write, or at least one delete",
		},
		{
			name:    "duplicate_deletes",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{items[0], items[1], items[0]},
			},
			expectedError: "rpc error: code = Code(2004) desc = duplicate tuple in write: user: 'user:0', relation: 'viewer', object: 'document:0'",
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
			expectedError: "rpc error: code = Code(2004) desc = duplicate tuple in write: user: 'user:0', relation: 'viewer', object: 'document:0'",
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
			expectedError: "rpc error: code = Code(2004) desc = duplicate tuple in write: user: 'user:1', relation: 'viewer', object: 'document:1'",
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
			expectedError: "rpc error: code = Code(2053) desc = The number of write operations exceeds the allowed limit of 10",
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'unknown:1#viewer@user:maria'. Reason: type 'unknown' not found",
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'document:1#viewer@group:eng#member'. Reason: type 'group' not found",
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'document:1#viewer@user:maria#member'. Reason: relation 'user#member' not found",
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'document:1#@user:maria'. Reason: the 'relation' field is malformed",
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'document:1#unknown@user:maria'. Reason: relation 'document#unknown' not found",
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'document:1#viewer@'. Reason: the 'user' field is malformed",
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'document:1#writer@user:*'. Reason: the typed wildcard 'user:*' is not an allowed type restriction for 'document#writer'",
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'document:1#union@user:maria'. Reason: type 'user' is not an allowed type restriction for 'document#union'",
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'document:1#intersection@user:maria'. Reason: type 'user' is not an allowed type restriction for 'document#intersection'",
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'document:1#difference@user:maria'. Reason: type 'user' is not an allowed type restriction for 'document#difference'",
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'document:1#computed@user:maria'. Reason: type 'user' is not an allowed type restriction for 'document#computed'",
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'document:1#viewer@org:fga'. Reason: type 'org' is not an allowed type restriction for 'document#viewer'",
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'document:1#ttu@user:maria'. Reason: type 'user' is not an allowed type restriction for 'document#ttu'",
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple '#viewer@user:maria'. Reason: invalid 'object' field format",
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'invalid#viewer@user:maria'. Reason: invalid 'object' field format",
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'document:1#implicit@document:1#implicit'. Reason: cannot write a tuple that is implicit",
		},
		{
			name: "delete_of_tuple_with_object_type_not_in_model_should_succeed",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
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
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
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
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
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
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'document:1#viewer@'. Reason: the 'user' field is malformed",
		},
		{
			name: "good_deletes_and_writes",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
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
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
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
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
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
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'document:1#require_condition@user:maria'. Reason: condition is missing",
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'document:2#writer@user:jon (condition condition1)'. Reason: invalid condition for type restriction",
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'document:2#viewer@user:* (condition condition1)'. Reason: found invalid context parameter: param2",
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'document:1#viewer@user:* (condition condition2)'. Reason: undefined condition",
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
			expectedError: "rpc error: code = Code(2000) desc = Invalid tuple 'document:1#viewer@user:*'. Reason: condition context size limit exceeded: 32789 bytes exceeds 32768 bytes",
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
			expectedError: "rpc error: code = Code(2000) desc = invalid schema version",
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
			expectedError: "rpc error: code = Code(2001) desc = Authorization Model '01JCC8ZD4X84K2W0H0ZA5AQ947' not found",
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
			expectedError: "rpc error: code = Code(4000) desc = Internal Server Error",
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
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any(), gomock.Any()).Return(storage.ErrTransactionalWriteFailed)
			},
			expectedError: "rpc error: code = Aborted desc = transactional write failed due to conflict",
		},
		{
			name: "error_transactional_write_failed_from_database_when_writing_wrapped_error",
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Return(model, nil)
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any(), gomock.Any()).Return(storage.ErrWriteConflictOnInsert)
			},
			expectedError: "rpc error: code = Aborted desc = transactional write failed due to conflict",
		},
		{
			name: "error_transactional_write_failed_from_database_when_writing_wrapped_conflict",
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Return(model, nil)
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any(), gomock.Any()).Return(storage.TupleConditionConflictError(&openfgav1.TupleKey{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}))
			},
			expectedError: "rpc error: code = Aborted desc = transactional write failed due to conflict",
		},
		{
			name: "error_transactional_write_failed_from_database_when_deleting_wrapped_error",
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any(), gomock.Any()).Return(storage.ErrWriteConflictOnDelete)
			},
			expectedError: "rpc error: code = Aborted desc = transactional write failed due to conflict",
		},
		{
			name: "error_transactional_write_failed_from_database_when_writing_wrapped_error",
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Return(model, nil)
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any()).Return(storage.ErrWriteConflictOnInsert)
			},
			expectedError: "rpc error: code = Aborted desc = transactional write failed due to conflict",
		},
		{
			name: "error_transactional_write_failed_from_database_when_writing_wrapped_conflict",
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Return(model, nil)
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any()).Return(storage.TupleConditionConflictError(&openfgav1.TupleKey{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}))
			},
			expectedError: "rpc error: code = Aborted desc = transactional write failed due to conflict",
		},
		{
			name: "error_transactional_write_failed_from_database_when_deleting_wrapped_error",
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any()).Return(storage.ErrWriteConflictOnDelete)
			},
			expectedError: "rpc error: code = Aborted desc = transactional write failed due to conflict",
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
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any(), gomock.Any()).
					Return(storage.InvalidWriteInputError(tuple.NewTupleKey("document:1", "viewer", "user:maria"), openfgav1.TupleOperation_TUPLE_OPERATION_WRITE))
			},
			expectedError: "rpc error: code = Code(2017) desc = cannot write a tuple which already exists: user: 'user:maria', relation: 'viewer', object: 'document:1': tuple to be written already existed or the tuple to be deleted did not exist",
		},
		{
			name: "error_invalid_write_input_from_database_when_deleting",
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any(), gomock.Any()).
					Return(storage.InvalidWriteInputError(tuple.NewTupleKey("document:1", "viewer", "user:maria"), openfgav1.TupleOperation_TUPLE_OPERATION_DELETE))
			},
			expectedError: "rpc error: code = Code(2017) desc = cannot delete a tuple which does not exist: user: 'user:maria', relation: 'viewer', object: 'document:1': tuple to be written already existed or the tuple to be deleted did not exist",
		},
		{
			name:    "delete_with_bad_on_missing_option",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {},
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
				OnMissing: "bad_option",
			},
			expectedError: "rpc error: code = Code(2000) desc = invalid on_missing option: bad_option",
		},
		{
			name: "delete_with_default_option",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any(),
					NewWriteOptsMatcher(storage.OnDuplicateInsertError, storage.OnMissingDeleteError)).Return(nil)
			},
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
			},
			expectedResponse: &openfgav1.WriteResponse{},
		},
		{
			name: "delete_with_on_error_option",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any(),
					NewWriteOptsMatcher(storage.OnDuplicateInsertError, storage.OnMissingDeleteError)).Return(nil)
			},
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
				OnMissing: "error",
			},
			expectedResponse: &openfgav1.WriteResponse{},
		},
		{
			name: "delete_with_on_ignore_option",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any(),
					NewWriteOptsMatcher(storage.OnDuplicateInsertError, storage.OnMissingDeleteIgnore)).Return(nil)
			},
			deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
				OnMissing: "ignore",
			},
			expectedResponse: &openfgav1.WriteResponse{},
		},
		{
			name: "writes_with_bad_on_duplicate_option",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Return(model, nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
				OnDuplicate: "bad_option",
			},
			expectedError: "rpc error: code = Code(2000) desc = invalid on_duplicate option: bad_option",
		},
		{
			name: "writes_with_default_option",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Return(model, nil)
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any(),
					NewWriteOptsMatcher(storage.OnDuplicateInsertError, storage.OnMissingDeleteError)).Return(nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
			},
			expectedResponse: &openfgav1.WriteResponse{},
		},
		{
			name: "writes_with_error_option",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Return(model, nil)
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any(),
					NewWriteOptsMatcher(storage.OnDuplicateInsertError, storage.OnMissingDeleteError)).Return(nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
				OnDuplicate: "error",
			},
			expectedResponse: &openfgav1.WriteResponse{},
		},
		{
			name: "writes_with_ignore_option",
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Return(model, nil)
				mockDatastore.EXPECT().Write(gomock.Any(), storeID, gomock.Any(), gomock.Any(),
					NewWriteOptsMatcher(storage.OnDuplicateInsertIgnore, storage.OnMissingDeleteError)).Return(nil)
			},
			writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:maria",
				}},
				OnDuplicate: "ignore",
			},
			expectedResponse: &openfgav1.WriteResponse{},
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
			if test.expectedError != "" {
				require.Nil(t, resp)
				require.Error(t, err)
				require.ErrorContains(t, err, test.expectedError)
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
