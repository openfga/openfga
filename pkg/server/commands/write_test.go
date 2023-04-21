package commands

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	mockstorage "github.com/openfga/openfga/pkg/storage/mocks"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestValidateNoDuplicatesAndCorrectSize(t *testing.T) {
	type test struct {
		name          string
		deletes       []*openfgapb.TupleKey
		writes        []*openfgapb.TupleKey
		expectedError error
	}

	logger := logger.NewNoopLogger()

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	maxTuplesInWriteOp := 10
	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().MaxTuplesPerWrite().AnyTimes().Return(maxTuplesInWriteOp)

	items := make([]*openfgapb.TupleKey, maxTuplesInWriteOp+1)
	for i := 0; i < maxTuplesInWriteOp+1; i++ {
		items[i] = &openfgapb.TupleKey{
			Object:   fmt.Sprintf("%s:1", testutils.CreateRandomString(459)),
			Relation: testutils.CreateRandomString(50),
			User:     testutils.CreateRandomString(512),
		}
	}

	cmd := NewWriteCommand(mockDatastore, logger, true)

	tests := []test{
		{
			name:    "empty_deletes_and_writes",
			deletes: []*openfgapb.TupleKey{},
			writes:  []*openfgapb.TupleKey{},
		},
		{
			name:    "good_deletes_and_writes",
			deletes: []*openfgapb.TupleKey{items[0], items[1]},
			writes:  []*openfgapb.TupleKey{items[2], items[3]},
		},
		{
			name:          "duplicate_deletes",
			deletes:       []*openfgapb.TupleKey{items[0], items[1], items[0]},
			writes:        []*openfgapb.TupleKey{},
			expectedError: serverErrors.DuplicateTupleInWrite(items[0]),
		},
		{
			name:          "duplicate_writes",
			deletes:       []*openfgapb.TupleKey{},
			writes:        []*openfgapb.TupleKey{items[0], items[1], items[0]},
			expectedError: serverErrors.DuplicateTupleInWrite(items[0]),
		},
		{
			name:          "same_item_appeared_in_writes_and_deletes",
			deletes:       []*openfgapb.TupleKey{items[2], items[1]},
			writes:        []*openfgapb.TupleKey{items[0], items[1]},
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
		storeID       string
		deletes       []*openfgapb.TupleKey
		writes        []*openfgapb.TupleKey
		expectedError error
	}

	badItem := &openfgapb.TupleKey{
		Object:   fmt.Sprintf("%s:1", testutils.CreateRandomString(20)),
		Relation: testutils.CreateRandomString(50),
		User:     "",
	}

	tests := []test{
		{
			name:          "nil_for_deletes_and_writes",
			storeId:       "abcd123",
			deletes:       nil,
			writes:        nil,
			expectedError: serverErrors.InvalidWriteInput,
		},
		{
			name:    "write_failure_with_invalid_user",
			storeId: "abcd123",
			deletes: []*openfgapb.TupleKey{},
			writes:  []*openfgapb.TupleKey{badItem},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause:    fmt.Errorf("the 'user' field is malformed"),
					TupleKey: badItem,
				},
			),
		},
		{
			name:    "delete_failure_with_invalid_user",
			storeId: "abcd123",
			deletes: []*openfgapb.TupleKey{badItem},
			writes:  []*openfgapb.TupleKey{},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause:    fmt.Errorf("the 'user' field is malformed"),
					TupleKey: badItem,
				},
			),
		},
		{
			name:          "write_failure_with_nonexistent_store",
			storeId:       "",
			deletes:       []*openfgapb.TupleKey{},
			writes:        []*openfgapb.TupleKey{},
			expectedError: serverErrors.NonExistentStoreID,
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
			cmd := NewWriteCommand(mockDatastore, logger, true)

			if len(test.writes) > 0 {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), gomock.Any(), gomock.Any()).Return(&openfgapb.AuthorizationModel{}, nil)
			}

			ctx := context.Background()
			req := &openfgapb.WriteRequest{
				StoreId: test.storeId,
				Writes:  &openfgapb.TupleKeys{TupleKeys: test.writes},
				Deletes: &openfgapb.TupleKeys{TupleKeys: test.deletes},
			}

			err := cmd.validateWriteRequest(ctx, req)
			require.ErrorIs(t, err, test.expectedError)
		})
	}
}
