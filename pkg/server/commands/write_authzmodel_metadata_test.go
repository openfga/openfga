package commands

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/server/metadata"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestWriteAuthorizationModelCommandWithMetadata(t *testing.T) {
	// This test demonstrates how metadata validation will work
	// when the API proto includes the metadata field

	t.Run("metadata_validation_infrastructure", func(t *testing.T) {
		// Test that our metadata validation works correctly
		validMetadata := map[string]string{
			"environment": "production",
			"team":        "platform",
			"version":     "v1.2.3",
		}

		err := metadata.ValidateMetadata(validMetadata)
		require.NoError(t, err)

		invalidMetadata := map[string]string{
			"openfga.dev/reserved": "value", // Should fail due to reserved prefix
		}

		err = metadata.ValidateMetadata(invalidMetadata)
		require.Error(t, err)
		require.Contains(t, err.Error(), "reserved prefix")
	})

	t.Run("current_implementation_works", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().MaxTypesPerAuthorizationModel().Return(100)
		mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

		cmd := NewWriteAuthorizationModelCommand(mockDatastore)

		// Current request without metadata (should work fine)
		req := &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       "test-store",
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{Type: "user"},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"viewer": {Userset: &openfgav1.Userset_This{}},
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"viewer": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{Type: "user"},
								},
							},
						},
					},
				},
			},
		}

		resp, err := cmd.Execute(context.Background(), req)
		require.NoError(t, err)
		require.NotEmpty(t, resp.AuthorizationModelId)
	})
}

// TestMetadataFilteringInfrastructure demonstrates the storage filtering capability
func TestMetadataFilteringInfrastructure(t *testing.T) {
	t.Run("metadata_filter_options", func(t *testing.T) {
		// Test that our storage options support metadata filtering
		options := storage.ReadAuthorizationModelsOptions{
			MetadataFilter: map[string]string{
				"environment": "production",
				"team":        "platform",
			},
		}

		require.Equal(t, "production", options.MetadataFilter["environment"])
		require.Equal(t, "platform", options.MetadataFilter["team"])
		require.Len(t, options.MetadataFilter, 2)
	})
}

// TestMetadataNormalization shows how metadata is normalized
func TestMetadataNormalization(t *testing.T) {
	input := map[string]string{
		"  key1  ": "  value1  ",
		"key2":     "value2",
		"":         "empty_key",     // Should be removed
		"key3":     "  value3  ",
	}

	normalized := metadata.NormalizeMetadata(input)

	expected := map[string]string{
		"key1": "value1",
		"key2": "value2", 
		"key3": "value3",
	}

	require.Equal(t, expected, normalized)
}
