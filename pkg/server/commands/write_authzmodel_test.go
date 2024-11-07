package commands

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestWriteAuthorizationModel(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()
	storeID := ulid.Make().String()

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().MaxTypesPerAuthorizationModel().AnyTimes().Return(100)

	testCases := map[string]struct {
		inputSchema string
	}{
		`allow 1.2`: {
			inputSchema: typesystem.SchemaVersion1_2,
		},
		`allow 1.1`: {
			inputSchema: typesystem.SchemaVersion1_1,
		},
	}

	for testName, test := range testCases {
		t.Run(testName, func(t *testing.T) {
			mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), storeID, gomock.Any()).Return(nil)

			cmd := NewWriteAuthorizationModelCommand(mockDatastore)
			_, err := cmd.Execute(ctx, &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
				},
				SchemaVersion: test.inputSchema,
			})
			require.NoError(t, err)
		})
	}
}
