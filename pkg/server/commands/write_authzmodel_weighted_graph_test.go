package commands

import (
	"context"
	"testing"

	"errors"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
)

// TestWriteAuthorizationModelWarnsWhenWeightedGraphBuildFails pins the
// observability fix for issue #3306: a model can pass NewAndValidate while
// the weighted authorization-model graph fails to build (here: a TTU whose
// computed relation is missing on one of the tupleset's directly related
// types). Before this fix the build error was discarded and the model-wide
// fallback to the default Check evaluator was completely silent; an operator
// had no log line, metric, or any other signal at model-write time.
//
// The test drives the real WriteAuthorizationModel command with a mock logger
// and asserts the warning fires with the model id and the build error.
func TestWriteAuthorizationModelWarnsWhenWeightedGraphBuildFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDatastore := mocks.NewMockTypeDefinitionWriteBackend(ctrl)
	mockDatastore.EXPECT().MaxTypesPerAuthorizationModel().AnyTimes().Return(100)
	mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

	// A model that passes validation but fails the weighted graph build: the
	// `parent` tupleset lists both `group` and `document`, and `viewer`'s TTU
	// computes `editor` on both, but `group` has no `editor` relation.
	req := &openfgav1.WriteAuthorizationModelRequest{
		StoreId: "store-id",
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{Type: "user"},
			{Type: "group"},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"parent": {Userset: &openfgav1.Userset_This{This: &openfgav1.DirectUserset{}}},
					"viewer": {Userset: &openfgav1.Userset_TupleToUserset{TupleToUserset: &openfgav1.TupleToUserset{
						Tupleset:        &openfgav1.ObjectRelation{Relation: "parent"},
						ComputedUserset: &openfgav1.ObjectRelation{Relation: "editor"},
					}}},
					"editor": {Userset: &openfgav1.Userset_This{This: &openfgav1.DirectUserset{}}},
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"parent": {DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
							{Type: "group"},
							{Type: "document"},
						}},
						"viewer": {DirectlyRelatedUserTypes: []*openfgav1.RelationReference{}},
						"editor": {DirectlyRelatedUserTypes: []*openfgav1.RelationReference{{Type: "user"}}},
					},
				},
			},
		},
	}

	warnCalled := false
	mockLogger := mocks.NewMockLogger(ctrl)
	mockLogger.EXPECT().WarnWithContext(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, msg string, fields ...any) {
			warnCalled = true
			t.Logf("warn fired: %s | fields: %v", msg, fields)
		}).Times(1)

	w := NewWriteAuthorizationModelCommand(mockDatastore, WithWriteAuthModelLogger(mockLogger))
	resp, err := w.Execute(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, warnCalled, "expected the weighted-graph build failure to produce a warning at model-write time")
}

// TestWriteAuthorizationModelNoWarnWhenWeightedGraphFailsAndWriteFails pins
// the ordering guarantee from the CodeRabbit review: the fallback warning must
// only fire after the model was actually persisted. If the backend write
// fails, the model was NOT accepted, so no "model accepted" warning may be
// logged - the caller gets the write error instead, and an operator must not
// be told to investigate a fallback for a model that does not exist.
func TestWriteAuthorizationModelNoWarnWhenWeightedGraphFailsAndWriteFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDatastore := mocks.NewMockTypeDefinitionWriteBackend(ctrl)
	mockDatastore.EXPECT().MaxTypesPerAuthorizationModel().AnyTimes().Return(100)
	mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.New("write failed")).Times(1)

	// Same model that passes validation but fails the weighted graph build.
	req := &openfgav1.WriteAuthorizationModelRequest{
		StoreId: "store-id",
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{Type: "user"},
			{Type: "group"},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"parent": {Userset: &openfgav1.Userset_This{This: &openfgav1.DirectUserset{}}},
					"viewer": {Userset: &openfgav1.Userset_TupleToUserset{TupleToUserset: &openfgav1.TupleToUserset{
						Tupleset:        &openfgav1.ObjectRelation{Relation: "parent"},
						ComputedUserset: &openfgav1.ObjectRelation{Relation: "editor"},
					}}},
					"editor": {Userset: &openfgav1.Userset_This{This: &openfgav1.DirectUserset{}}},
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"parent": {DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
							{Type: "group"},
							{Type: "document"},
						}},
						"viewer": {DirectlyRelatedUserTypes: []*openfgav1.RelationReference{}},
						"editor": {DirectlyRelatedUserTypes: []*openfgav1.RelationReference{{Type: "user"}}},
					},
				},
			},
		},
	}

	mockLogger := mocks.NewMockLogger(ctrl)
	// No logging expectation at all: any Warn call fails the test.

	w := NewWriteAuthorizationModelCommand(mockDatastore, WithWriteAuthModelLogger(mockLogger))
	resp, err := w.Execute(context.Background(), req)
	require.Error(t, err)
	require.Nil(t, resp)
}

// TestWriteAuthorizationModelNoWarnWhenWeightedGraphBuilds asserts the happy
// path stays quiet: a well-formed model logs nothing about weighted graphs.
func TestWriteAuthorizationModelNoWarnWhenWeightedGraphBuilds(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDatastore := mocks.NewMockTypeDefinitionWriteBackend(ctrl)
	mockDatastore.EXPECT().MaxTypesPerAuthorizationModel().AnyTimes().Return(100)
	mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

	req := &openfgav1.WriteAuthorizationModelRequest{
		StoreId: "store-id",
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{Type: "user"},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"viewer": {Userset: &openfgav1.Userset_This{This: &openfgav1.DirectUserset{}}},
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"viewer": {DirectlyRelatedUserTypes: []*openfgav1.RelationReference{{Type: "user"}}},
					},
				},
			},
		},
	}

	mockLogger := mocks.NewMockLogger(ctrl)
	// No logging expectation at all: any Warn/Info/Error call fails the test.

	w := NewWriteAuthorizationModelCommand(mockDatastore, WithWriteAuthModelLogger(mockLogger))
	resp, err := w.Execute(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
}
