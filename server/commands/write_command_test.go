package commands

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	serverErrors "github.com/openfga/openfga/server/errors"
	mockstorage "github.com/openfga/openfga/storage/mocks"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestValidateWriteTuples(t *testing.T) {
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
		err := cmd.validateNoDuplicatesAndCorrectSize(test.deletes, test.writes)
		if !reflect.DeepEqual(err, test.expectedError) {
			t.Errorf("%s: Expected error %v, got %v", test.name, test.expectedError, err)
		}
	}

}
