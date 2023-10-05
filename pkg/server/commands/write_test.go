package commands

import (
	"context"
	"fmt"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/golang/mock/gomock"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
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
		deletes       []*openfgav1.TupleKey
		writes        []*openfgav1.TupleKey
		expectedError error
	}

	badItem := &openfgav1.TupleKey{
		Object:   fmt.Sprintf("%s:1", testutils.CreateRandomString(20)),
		Relation: testutils.CreateRandomString(50),
		User:     "",
	}

	tests := []test{
		{
			name:          "nil_for_deletes_and_writes",
			deletes:       nil,
			writes:        nil,
			expectedError: serverErrors.InvalidWriteInput,
		},
		{
			name:    "write_failure_with_invalid_user",
			deletes: []*openfgav1.TupleKey{},
			writes:  []*openfgav1.TupleKey{badItem},
			expectedError: serverErrors.ValidationError(
				&tuple.InvalidTupleError{
					Cause:    fmt.Errorf("the 'user' field is malformed"),
					TupleKey: badItem,
				},
			),
		},
		{
			name:    "delete_failure_with_invalid_user",
			deletes: []*openfgav1.TupleKey{badItem},
			writes:  []*openfgav1.TupleKey{},
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
				Writes:  &openfgav1.TupleKeys{TupleKeys: test.writes},
				Deletes: &openfgav1.TupleKeys{TupleKeys: test.deletes},
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
			&openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustParse(`
				type user

				type document
				  relations
				    define viewer: [user] as self
				`),
			}, nil)

	mockDatastore.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(storage.ErrTransactionalWriteFailed)

	cmd := NewWriteCommand(mockDatastore, logger.NewNoopLogger())

	resp, err := cmd.Execute(context.Background(), &openfgav1.WriteRequest{
		StoreId: ulid.Make().String(),
		Writes: &openfgav1.TupleKeys{
			TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			},
		},
	})
	require.ErrorIs(t, err, serverErrors.NewInternalError("concurrent write conflict", storage.ErrTransactionalWriteFailed))
	require.Nil(t, resp)
}
