package commands

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/openfga/openfga/pkg/testutils"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	mockstorage "github.com/openfga/openfga/internal/mocks"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestWriteAssertions(t *testing.T) {
	t.Run("validates_total_size_in_bytes_less_than_64kb", func(t *testing.T) {
		storeID := ulid.Make().String()
		modelID := ulid.Make().String()

		maxBytesPerUser := 512
		maxBytesPerRelation := 50
		maxBytesPerObject := 256
		numAssertions := 10
		ctxTuples := 10

		longRelationName := strings.Repeat("a", maxBytesPerRelation)

		contextualTuples := make([]*openfgav1.TupleKey, 0, ctxTuples)
		assertions := make([]*openfgav1.Assertion, 0, numAssertions)

		for ct := 0; ct < ctxTuples; ct++ {
			contextualTuples = append(contextualTuples, &openfgav1.TupleKey{
				Object:   "document:" + strings.Repeat(strconv.Itoa(ct), maxBytesPerObject-len("document:")),
				Relation: longRelationName,
				User:     "user:" + strings.Repeat(strconv.Itoa(ct), maxBytesPerUser-len("user:")),
			})
		}

		for a := 0; a < numAssertions; a++ {
			assertions = append(assertions, &openfgav1.Assertion{
				TupleKey: &openfgav1.AssertionTupleKey{
					Object:   "document:" + strings.Repeat(strconv.Itoa(a), maxBytesPerObject-len("document:")),
					Relation: longRelationName,
					User:     "user:" + strings.Repeat(strconv.Itoa(a), maxBytesPerUser-len("user:")),
				},
				Expectation:      true,
				ContextualTuples: contextualTuples,
				Context: testutils.MustNewStruct(t, map[string]interface{}{
					"x": 10,
				}),
			})
		}

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().WriteAssertions(gomock.Any(), storeID, modelID, gomock.Any()).Times(0)
		mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).
			Times(1).
			Return(&openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							longRelationName: typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								longRelationName: {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "user"},
									},
								},
							},
						},
					},
				},
			}, nil)

		resp, err := NewWriteAssertionsCommand(mockDatastore).
			Execute(context.Background(), &openfgav1.WriteAssertionsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Assertions:           assertions,
			})
		require.Nil(t, resp)
		require.ErrorIs(t, err, serverErrors.ExceededEntityLimit("bytes", DefaultMaxAssertionSizeInBytes))
	})

	t.Run("validates_total_size_in_bytes_less_than_64kb_when_too_much_in_context", func(t *testing.T) {
		storeID := ulid.Make().String()
		modelID := ulid.Make().String()
		maxBytesPerContextField := 512

		fgaContext := map[string]interface{}{}
		for index := 0; index < 100; index++ {
			key := strings.Repeat("a", maxBytesPerContextField) + strconv.Itoa(index)
			fgaContext[key] = key
		}

		assertions := []*openfgav1.Assertion{
			{
				TupleKey: &openfgav1.AssertionTupleKey{
					Object:   "document:roadmap",
					Relation: "can_view",
					User:     "user:anne",
				},
				Expectation: true,
				Context:     testutils.MustNewStruct(t, fgaContext),
			},
		}

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
		mockDatastore.EXPECT().WriteAssertions(gomock.Any(), storeID, modelID, gomock.Any()).Times(0)
		mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).
			Times(1).
			Return(&openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"can_view": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"can_view": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "user"},
									},
								},
							},
						},
					},
				},
			}, nil)

		resp, err := NewWriteAssertionsCommand(mockDatastore).
			Execute(context.Background(), &openfgav1.WriteAssertionsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Assertions:           assertions,
			})
		require.Nil(t, resp)
		require.ErrorIs(t, err, serverErrors.ExceededEntityLimit("bytes", DefaultMaxAssertionSizeInBytes))
	})
}
