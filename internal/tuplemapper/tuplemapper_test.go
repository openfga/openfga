package tuplemapper

import (
	"context"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/pkg/tuple"

	mockstorage "github.com/openfga/openfga/internal/mocks"
)

func TestNewMapper(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockIter := mockstorage.NewErrorTupleIterator(nil)

	t.Run("no_op", func(t *testing.T) {
		mapper := New(NoOpKind, mockIter)
		require.NotNil(t, mapper)
		_, ok := mapper.(*NoOpMapper)
		require.True(t, ok)
	})

	t.Run("nested_userset", func(t *testing.T) {
		mapper := New(NestedUsersetKind, mockIter)
		require.NotNil(t, mapper)
		_, ok := mapper.(*NestedUsersetMapper)
		require.True(t, ok)
	})
}

func TestUsersetTupleMapper(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockIter := mockstorage.NewErrorTupleIterator([]*openfgav1.Tuple{
		{Key: tuple.NewTupleKey("document:1", "viewer", "group:fga#member")},
	})

	mapper := New(NestedUsersetKind, mockIter)
	require.NotNil(t, mapper)
	actualMapper, ok := mapper.(Mapper[string])
	require.True(t, ok)

	_, err := actualMapper.Next(context.Background())
	require.NoError(t, err)

	t.Run("map_success", func(t *testing.T) {
		res, err := actualMapper.Map(tuple.NewTupleKey("group:1", "member", "group:2#member"))
		require.NoError(t, err)
		require.Equal(t, "group:2", res)
	})

	t.Run("map_with_error", func(t *testing.T) {
		_, err := actualMapper.Map(tuple.NewTupleKey("group:1", "member", "group:2"))
		require.Error(t, err)
	})
}
