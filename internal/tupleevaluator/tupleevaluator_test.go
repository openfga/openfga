package tupleevaluator

import (
	"context"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestNestedEvaluator(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().
		ReadUsersetTuples(gomock.Any(), "ABC", storage.ReadUsersetTuplesFilter{
			Object:                      "group:1",
			Relation:                    "member",
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{typesystem.DirectRelationReference("group", "member")},
		}, storage.ReadUsersetTuplesOptions{Consistency: storage.ConsistencyOptions{Preference: openfgav1.ConsistencyPreference_MINIMIZE_LATENCY}}).
		Return(nil, nil).Times(1)

	baseEvaluator, err := NewNestedUsersetEvaluator(context.Background(), mockDatastore, EvaluationRequest{
		StoreID:     "ABC",
		Consistency: openfgav1.ConsistencyPreference_MINIMIZE_LATENCY,
		Object:      "group:1",
		Relation:    "member",
	})
	require.NoError(t, err)
	require.NotNil(t, baseEvaluator)

	t.Run("clone", func(t *testing.T) {
		mockDatastore.EXPECT().
			ReadUsersetTuples(gomock.Any(), "ABC", storage.ReadUsersetTuplesFilter{
				Object:   "new_group:2",
				Relation: "new_relation",
				// keep the same filter as the original
				AllowedUserTypeRestrictions: []*openfgav1.RelationReference{typesystem.DirectRelationReference("group", "member")},
			}, storage.ReadUsersetTuplesOptions{Consistency: storage.ConsistencyOptions{Preference: openfgav1.ConsistencyPreference_MINIMIZE_LATENCY}}).
			Return(nil, nil).Times(1)

		clonedEvaluator, err := baseEvaluator.Clone(context.Background(), "new_group:2", "new_relation")
		require.NoError(t, err)
		require.NotNil(t, clonedEvaluator)
		require.NotEqual(t, baseEvaluator, clonedEvaluator)

		// original inputs should not be affected
		require.Equal(t, "group:1", baseEvaluator.inputs.Object)
		require.Equal(t, "member", baseEvaluator.inputs.Relation)
	})
}
