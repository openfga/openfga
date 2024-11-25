package commands

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	mockstorage "github.com/openfga/openfga/internal/mocks"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
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

	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	model := parser.MustTransformDSLToProto(`
	model
		schema 1.1
	type user

	type repo
		relations
			define reader: [user, user with condX]
			define can_read: reader

	condition condX(x :int) {
		x > 0
	}`)

	modelInvalidVersion := &openfgav1.AuthorizationModel{
		SchemaVersion: "1.0",
	}

	var tests = []struct {
		name          string
		input         *openfgav1.WriteAssertionsRequest
		setMock       func(*mockstorage.MockOpenFGADatastore)
		expectedError string
	}{
		{
			name: "succeeds",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Assertions: []*openfgav1.Assertion{
					{
						TupleKey:    tuple.NewAssertionTupleKey("repo:test", "reader", "user:elbuo"),
						Expectation: false,
					},
					{
						TupleKey:    tuple.NewAssertionTupleKey("repo:test", "reader", "user:maria"),
						Expectation: true,
					},
					{
						TupleKey:    tuple.NewAssertionTupleKey("repo:test", "reader", "user:jon"),
						Expectation: false,
					},
					{
						TupleKey:    tuple.NewAssertionTupleKey("repo:test", "reader", "user:jose"),
						Expectation: true,
					},
				},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
				mockDatastore.EXPECT().WriteAssertions(gomock.Any(), storeID, modelID, gomock.Any()).Times(1).Return(nil)
			},
		},
		{
			name: "succeeds_when_empty_assertions",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Assertions:           []*openfgav1.Assertion{},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
				mockDatastore.EXPECT().WriteAssertions(gomock.Any(), storeID, modelID, gomock.Any()).Times(1).Return(nil)
			},
		},
		{
			name: "succeeds_when_it_is_not_directly_assignable",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Assertions: []*openfgav1.Assertion{
					{
						TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
						Expectation: false,
					},
					{
						TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:maria"),
						Expectation: false,
					},
					{
						TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:jon"),
						Expectation: true,
					},
				},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
				mockDatastore.EXPECT().WriteAssertions(gomock.Any(), storeID, modelID, gomock.Any()).Times(1).Return(nil)
			},
		},
		{
			name: "succeeds_with_contextual_tuple",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Assertions: []*openfgav1.Assertion{
					{
						TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
						Expectation: false,
						ContextualTuples: []*openfgav1.TupleKey{
							tuple.NewTupleKey("repo:test", "reader", "user:elbuo"),
						},
					},
				},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
				mockDatastore.EXPECT().WriteAssertions(gomock.Any(), storeID, modelID, gomock.Any()).Times(1).Return(nil)
			},
		},
		{
			name: "succeeds_with_context",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Assertions: []*openfgav1.Assertion{
					{
						TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
						Expectation: false,
						Context: testutils.MustNewStruct(t, map[string]interface{}{
							"x": 10,
						}),
					},
				},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
				mockDatastore.EXPECT().WriteAssertions(gomock.Any(), storeID, modelID, gomock.Any()).Times(1).Return(nil)
			},
		},
		{
			name: "succeeds_with_contextual_tuple_with_condition",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Assertions: []*openfgav1.Assertion{
					{
						TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
						Expectation: false,
						ContextualTuples: []*openfgav1.TupleKey{
							tuple.NewTupleKeyWithCondition("repo:test", "reader", "user:elbuo", "condX",
								testutils.MustNewStruct(t, map[string]interface{}{"x": 0})),
						},
					},
				},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
				mockDatastore.EXPECT().WriteAssertions(gomock.Any(), storeID, modelID, gomock.Any()).Times(1).Return(nil)
			},
		},
		{
			name: "fails_with_invalid_model_version",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Assertions:           []*openfgav1.Assertion{},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(modelInvalidVersion, nil)
			},
			expectedError: "invalid schema version",
		},
		{
			name: "fails_with_contextual_tuple_and_condition_because_invalid_context_parameter",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Assertions: []*openfgav1.Assertion{
					{
						TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
						Expectation: false,
						ContextualTuples: []*openfgav1.TupleKey{
							tuple.NewTupleKeyWithCondition("repo:test", "reader", "user:elbuo", "condX",
								testutils.MustNewStruct(t, map[string]interface{}{"unknownparam": 0})),
						},
					},
				},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			expectedError: "Invalid tuple 'repo:test#reader@user:elbuo (condition condX)'. Reason: found invalid context parameter: unknownparam",
		},
		{
			name: "fails_with_contextual_tuple_with_condition_because_undefined_condition",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Assertions: []*openfgav1.Assertion{
					{
						TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
						Expectation: false,
						ContextualTuples: []*openfgav1.TupleKey{
							tuple.NewTupleKeyWithCondition("repo:test", "reader", "user:elbuo", "condundefined", nil),
						},
					},
				},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			expectedError: "Invalid tuple 'repo:test#reader@user:elbuo (condition condundefined)'. Reason: undefined condition",
		},
		{
			name: "fails_with_contextual_tuple_because_contextual_tuple_is_not_directly_assignable",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Assertions: []*openfgav1.Assertion{
					{
						TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
						Expectation: false,
						ContextualTuples: []*openfgav1.TupleKey{
							tuple.NewTupleKey("repo:test", "can_read", "user:elbuo"),
						},
					},
				},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			expectedError: "Invalid tuple 'repo:test#can_read@user:elbuo'. Reason: type 'user' is not an allowed type restriction for 'repo#can_read'",
		},
		{
			name: "fails_with_contextual_tuple_because_invalid_relation_in_contextual_tuple",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Assertions: []*openfgav1.Assertion{
					{
						TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
						Expectation: false,
						ContextualTuples: []*openfgav1.TupleKey{
							tuple.NewTupleKey("repo:test", "invalidrelation", "user:elbuo"),
						},
					},
				},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			expectedError: "Invalid tuple 'repo:test#invalidrelation@user:elbuo'. Reason: relation 'repo#invalidrelation' not found",
		},
		{
			name: "fails_with_contextual_tuple_because_invalid_type_in_contextual_tuple",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Assertions: []*openfgav1.Assertion{
					{
						TupleKey:    tuple.NewAssertionTupleKey("repo:test", "can_read", "user:elbuo"),
						Expectation: false,
						ContextualTuples: []*openfgav1.TupleKey{
							tuple.NewTupleKey("unknown:test", "reader", "user:elbuo"),
						},
					},
				},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			expectedError: "Invalid tuple 'unknown:test#reader@user:elbuo'. Reason: type 'unknown' not found",
		},
		{
			name: "fails_with_invalid_relation",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Assertions: []*openfgav1.Assertion{
					{
						TupleKey: tuple.NewAssertionTupleKey(
							"repo:test",
							"invalidrelation",
							"user:elbuo",
						),
						Expectation: false,
					},
				},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
			},
			expectedError: "relation 'repo#invalidrelation' not found",
		},
		{
			name: "fails_if_model_not_found",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              storeID,
				AuthorizationModelId: "123",
				Assertions:           []*openfgav1.Assertion{},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, "123").Times(1).Return(nil, storage.ErrNotFound)
			},
			expectedError: "Authorization Model '123' not found",
		},
		{
			name: "fails_if_database_error_when_reading_model",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Assertions:           []*openfgav1.Assertion{},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(nil, errors.New("internal"))
			},
			expectedError: "Internal Server Error",
		},
		{
			name: "fails_if_database_error_when_writing_assertions",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Assertions:           []*openfgav1.Assertion{},
			},
			setMock: func(mockDatastore *mockstorage.MockOpenFGADatastore) {
				mockDatastore.EXPECT().ReadAuthorizationModel(gomock.Any(), storeID, modelID).Times(1).Return(model, nil)
				mockDatastore.EXPECT().WriteAssertions(gomock.Any(), storeID, modelID, gomock.Any()).Times(1).Return(errors.New("internal"))
			},
			expectedError: "Internal Server Error",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockController := gomock.NewController(t)
			defer mockController.Finish()

			mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
			test.setMock(mockDatastore)

			resp, err := NewWriteAssertionsCommand(mockDatastore).Execute(context.Background(), test.input)
			if test.expectedError != "" {
				require.Nil(t, resp)
				require.Error(t, err)
				require.ErrorContains(t, err, test.expectedError)
				return
			}
			require.NoError(t, err)
		})
	}
}
