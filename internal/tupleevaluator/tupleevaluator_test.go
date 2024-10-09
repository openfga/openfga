package tupleevaluator

import (
	"context"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/pkg/tuple"

	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestNestedUsersetEvaluator(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	baseEvaluator := newNestedUsersetEvaluator(mockDatastore, EvaluationRequest{
		StoreID:     "ABC",
		Consistency: openfgav1.ConsistencyPreference_MINIMIZE_LATENCY,
		Object:      "group:1",
		Relation:    "member",
	})
	require.NotNil(t, baseEvaluator)

	mockDatastore.EXPECT().
		ReadUsersetTuples(gomock.Any(), "ABC", storage.ReadUsersetTuplesFilter{
			Object:                      "group:1",
			Relation:                    "member",
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{typesystem.DirectRelationReference("group", "member")},
		}, storage.ReadUsersetTuplesOptions{Consistency: storage.ConsistencyOptions{Preference: openfgav1.ConsistencyPreference_MINIMIZE_LATENCY}}).
		Return(nil, nil).Times(1)

	_, err := baseEvaluator.Start(context.Background())
	require.NoError(t, err)

	t.Run("clone", func(t *testing.T) {
		clonedEvaluator := baseEvaluator.Clone("new_group:2", "new_relation")
		require.NotNil(t, clonedEvaluator)

		// original inputs should not be affected
		require.Equal(t, "group:1", baseEvaluator.Filter.Object)
		require.Equal(t, "member", baseEvaluator.Filter.Relation)

		mockDatastore.EXPECT().
			ReadUsersetTuples(gomock.Any(), "ABC", storage.ReadUsersetTuplesFilter{
				Object:   "new_group:2",
				Relation: "new_relation",
				// keep the same filter as the original
				AllowedUserTypeRestrictions: []*openfgav1.RelationReference{typesystem.DirectRelationReference("group", "member")},
			}, storage.ReadUsersetTuplesOptions{Consistency: storage.ConsistencyOptions{Preference: openfgav1.ConsistencyPreference_MINIMIZE_LATENCY}}).
			Return(nil, nil).Times(1)

		_, err := clonedEvaluator.Start(context.Background())
		require.NoError(t, err)
	})
}

func TestNestedTTUEvaluator(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	baseEvaluator := newNestedTTUEvaluator(mockDatastore, EvaluationRequest{
		StoreID:     "ABC",
		Consistency: openfgav1.ConsistencyPreference_MINIMIZE_LATENCY,
		Object:      "group:1",
		Relation:    "parent",
	})
	require.NotNil(t, baseEvaluator)

	mockDatastore.EXPECT().
		Read(gomock.Any(), "ABC", tuple.NewTupleKey("group:1", "parent", ""),
			storage.ReadOptions{Consistency: storage.ConsistencyOptions{Preference: openfgav1.ConsistencyPreference_MINIMIZE_LATENCY}}).
		Return(nil, nil).Times(1)
	_, err := baseEvaluator.Start(context.Background())
	require.NoError(t, err)

	t.Run("clone", func(t *testing.T) {
		clonedEvaluator := baseEvaluator.Clone("new_group:2", "")
		require.NotNil(t, clonedEvaluator)

		// original inputs should not be affected
		require.Equal(t, "group:1", baseEvaluator.Filter.GetObject())
		require.Equal(t, "parent", baseEvaluator.Filter.GetRelation())

		mockDatastore.EXPECT().
			Read(gomock.Any(), "ABC", tuple.NewTupleKey("new_group:2", "parent", ""),
				storage.ReadOptions{Consistency: storage.ConsistencyOptions{Preference: openfgav1.ConsistencyPreference_MINIMIZE_LATENCY}}).
			Return(nil, nil).Times(1)
		_, err := clonedEvaluator.Start(context.Background())
		require.NoError(t, err)
	})

	t.Run("evaluate", func(t *testing.T) {
		res, err := baseEvaluator.Evaluate(tuple.NewTupleKey("doc:1", "parent", "group:1"))
		require.NoError(t, err)
		require.Equal(t, "group:1", res)
	})
}
