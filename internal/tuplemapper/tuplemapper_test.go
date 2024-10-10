package tuplemapper

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

func TestNestedUsersetTupleMapper(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	mapper := newNestedUsersetTupleMapper(mockDatastore, Request{
		StoreID:     "ABC",
		Consistency: openfgav1.ConsistencyPreference_MINIMIZE_LATENCY,
		Object:      "group:1",
		Relation:    "member",
	})
	require.NotNil(t, mapper)

	mockDatastore.EXPECT().
		ReadUsersetTuples(gomock.Any(), "ABC", storage.ReadUsersetTuplesFilter{
			Object:                      "group:1",
			Relation:                    "member",
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{typesystem.DirectRelationReference("group", "member")},
		}, storage.ReadUsersetTuplesOptions{Consistency: storage.ConsistencyOptions{Preference: openfgav1.ConsistencyPreference_MINIMIZE_LATENCY}}).
		Return(nil, nil).Times(1)

	_, err := mapper.Start(context.Background())
	require.NoError(t, err)

	t.Run("map_success", func(t *testing.T) {
		res, err := mapper.Map(tuple.NewTupleKey("group:1", "member", "group:2#member"))
		require.NoError(t, err)
		require.Equal(t, "group:2", res)
	})

	t.Run("map_with_error", func(t *testing.T) {
		_, err := mapper.Map(tuple.NewTupleKey("group:1", "member", "group:2"))
		require.Error(t, err)
	})

	t.Run("clone", func(t *testing.T) {
		clonedMapper := mapper.Clone("new_group:2")
		require.NotNil(t, clonedMapper)

		// Original mapper should not be modified.
		require.Equal(t, "group:1", mapper.Filter.Object)

		mockDatastore.EXPECT().
			ReadUsersetTuples(gomock.Any(), "ABC", storage.ReadUsersetTuplesFilter{
				Object:   "new_group:2",
				Relation: "member", // same relation
				// Same AllowedUserTypeRestrictions as the original mapper.
				AllowedUserTypeRestrictions: []*openfgav1.RelationReference{typesystem.DirectRelationReference("group", "member")},
			}, storage.ReadUsersetTuplesOptions{Consistency: storage.ConsistencyOptions{Preference: openfgav1.ConsistencyPreference_MINIMIZE_LATENCY}}).
			Return(nil, nil).Times(1)

		_, err := clonedMapper.Start(context.Background())
		require.NoError(t, err)
	})
}
