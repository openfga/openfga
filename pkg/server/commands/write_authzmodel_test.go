package commands

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestWriteAuthorizationModelWithExperimentalEnableModularModels(t *testing.T) {
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
		enableModularModules bool
		inputSchema          string
		expectAllowed        bool
	}{
		`allow 1.2 when enableModularModules true`: {
			enableModularModules: true,
			inputSchema:          typesystem.SchemaVersion1_2,
			expectAllowed:        true,
		},
		`forbid 1.2 when not enableModularModules false`: {
			enableModularModules: false,
			inputSchema:          typesystem.SchemaVersion1_2,
			expectAllowed:        false,
		},
		`allow 1.1 when enableModularModules true`: {
			enableModularModules: true,
			inputSchema:          typesystem.SchemaVersion1_1,
			expectAllowed:        true,
		},
		`allow 1.1 when enableModularModules false`: {
			enableModularModules: false,
			inputSchema:          typesystem.SchemaVersion1_1,
			expectAllowed:        true,
		},
	}

	for testName, test := range testCases {
		t.Run(testName, func(t *testing.T) {
			if test.expectAllowed {
				mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), storeID, gomock.Any()).Return(nil)
			}

			cmd := NewWriteAuthorizationModelCommand(mockDatastore,
				WithEnableModularModels(test.enableModularModules),
			)
			_, err := cmd.Execute(ctx, &openfgav1.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
				},
				SchemaVersion: test.inputSchema,
			})
			if test.expectAllowed {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, status.Error(codes.InvalidArgument, "modular models (schema version 1.2) are not supported"))
			}
		})
	}
}
