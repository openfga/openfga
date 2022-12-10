package commands

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	serverErrors "github.com/openfga/openfga/server/errors"
	mockstorage "github.com/openfga/openfga/storage/mocks"
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

	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	maxTuplesInWriteOp := 10
	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().MaxTuplesInWriteOperation().AnyTimes().Return(maxTuplesInWriteOp)

	items := make([]*openfgapb.TupleKey, maxTuplesInWriteOp+1)
	for i := 0; i < maxTuplesInWriteOp+1; i++ {
		items[i] = &openfgapb.TupleKey{
			Object:   fmt.Sprintf("%s:1", testutils.CreateRandomString(459)),
			Relation: testutils.CreateRandomString(50),
			User:     testutils.CreateRandomString(512),
		}
	}

	cmd := NewWriteCommand(mockDatastore, tracer, logger)

	tests := []test{
		{
			name:    "empty deletes and writes",
			deletes: []*openfgapb.TupleKey{},
			writes:  []*openfgapb.TupleKey{},
		},
		{
			name:    "good deletes and writes",
			deletes: []*openfgapb.TupleKey{items[0], items[1]},
			writes:  []*openfgapb.TupleKey{items[2], items[3]},
		},
		{
			name:          "duplicate deletes",
			deletes:       []*openfgapb.TupleKey{items[0], items[1], items[0]},
			writes:        []*openfgapb.TupleKey{},
			expectedError: serverErrors.DuplicateTupleInWrite(items[0]),
		},
		{
			name:          "duplicate writes",
			deletes:       []*openfgapb.TupleKey{},
			writes:        []*openfgapb.TupleKey{items[0], items[1], items[0]},
			expectedError: serverErrors.DuplicateTupleInWrite(items[0]),
		},
		{
			name:          "same item appeared in writes and deletes",
			deletes:       []*openfgapb.TupleKey{items[2], items[1]},
			writes:        []*openfgapb.TupleKey{items[0], items[1]},
			expectedError: serverErrors.DuplicateTupleInWrite(items[1]),
		},
		{
			name:          "too many items writes and deletes",
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
			name:          "nil for deletes and writes",
			deletes:       nil,
			writes:        nil,
			expectedError: serverErrors.InvalidWriteInput,
		},
		{
			name:          "write failure with invalid user",
			deletes:       []*openfgapb.TupleKey{},
			writes:        []*openfgapb.TupleKey{badItem},
			expectedError: serverErrors.InvalidTuple("the 'user' field is invalid", badItem),
		},
		{
			name:          "delete failure with invalid user",
			deletes:       []*openfgapb.TupleKey{badItem},
			writes:        []*openfgapb.TupleKey{},
			expectedError: serverErrors.InvalidTuple("the 'user' field is invalid", badItem),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tracer := telemetry.NewNoopTracer()
			logger := logger.NewNoopLogger()

			mockController := gomock.NewController(t)
			defer mockController.Finish()
			maxTuplesInWriteOp := 10
			mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
			mockDatastore.EXPECT().MaxTuplesInWriteOperation().AnyTimes().Return(maxTuplesInWriteOp)
			cmd := NewWriteCommand(mockDatastore, tracer, logger)

			if len(test.writes) > 0 {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), gomock.Any(), gomock.Any()).Return(&openfgapb.AuthorizationModel{}, nil)
			}

			ctx := context.Background()
			req := &openfgapb.WriteRequest{
				StoreId: "abcd123",
				Writes:  &openfgapb.TupleKeys{TupleKeys: test.writes},
				Deletes: &openfgapb.TupleKeys{TupleKeys: test.deletes},
			}

			err := cmd.validateWriteRequest(ctx, req)
			require.ErrorIs(t, err, test.expectedError)
		})
	}
}
