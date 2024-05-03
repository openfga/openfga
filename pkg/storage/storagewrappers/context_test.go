package storagewrappers

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
)

func TestContextWrapper_Exits_Early_If_Context_Error(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	dut := NewContextWrapper(mockDatastore)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var testCases = map[string]struct {
		requestFunc func() (any, error)
	}{
		`read`: {
			requestFunc: func() (any, error) {
				return dut.Read(ctx, ulid.Make().String(), &openfgav1.TupleKey{})
			},
		},
		`read_page`: {
			requestFunc: func() (any, error) {
				resp, _, err := dut.ReadPage(ctx, ulid.Make().String(), &openfgav1.TupleKey{}, storage.PaginationOptions{})
				return resp, err
			},
		},
		`read_user_tuple`: {
			requestFunc: func() (any, error) {
				return dut.ReadUserTuple(ctx, ulid.Make().String(), &openfgav1.TupleKey{})
			},
		},
		`read_userset_tuples`: {
			requestFunc: func() (any, error) {
				return dut.ReadUsersetTuples(ctx, ulid.Make().String(), storage.ReadUsersetTuplesFilter{})
			},
		},
		`read_starting_with_user`: {
			requestFunc: func() (any, error) {
				return dut.ReadStartingWithUser(ctx, ulid.Make().String(), storage.ReadStartingWithUserFilter{})
			},
		},
	}

	for testName, test := range testCases {
		t.Run(testName, func(t *testing.T) {
			resp, err := test.requestFunc()
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, resp)
		})
	}
}
