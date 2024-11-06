package graph

import (
	"context"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/pkg/storage"

	"github.com/openfga/openfga/pkg/tuple"
)

func TestNestedUsersetTupleMapper(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("group:fga", "member", "group:2#member"),
		tuple.NewTupleKey("group:fga", "member", "group:2"),
	}

	innerIter := storage.NewStaticTupleKeyIterator(tks)

	mapper := wrapIterator(NestedUsersetKind, innerIter)
	require.NotNil(t, mapper)
	defer mapper.Stop()

	t.Run("head_success", func(t *testing.T) {
		res, err := mapper.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, "group:2", res)
	})

	t.Run("map_success", func(t *testing.T) {
		// first tk is a userset so can be mapped
		res, err := mapper.Next(context.Background())
		require.NoError(t, err)
		require.Equal(t, "group:2", res)
	})

	t.Run("map_error", func(t *testing.T) {
		// second tk is not a userset so can't be mapped
		res, err := mapper.Next(context.Background())
		require.Error(t, err)
		require.Equal(t, "", res)
	})
}

func TestNestedTTUTupleMapper(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("group:fga", "member", "group:2#member"),
	}

	innerIter := storage.NewStaticTupleKeyIterator(tks)

	mapper := wrapIterator(TTUKind, innerIter)
	require.NotNil(t, mapper)
	defer mapper.Stop()

	t.Run("head_success", func(t *testing.T) {
		res, err := mapper.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, "group:2#member", res)
	})

	t.Run("map_success", func(t *testing.T) {
		res, err := mapper.Next(context.Background())
		require.NoError(t, err)
		require.Equal(t, "group:2#member", res)
	})
}

func TestObjectIDTupleMapper(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("group:fga", "member", "group:2#member"),
	}

	innerIter := storage.NewStaticTupleKeyIterator(tks)

	mapper := wrapIterator(ObjectIDKind, innerIter)
	require.NotNil(t, mapper)
	defer mapper.Stop()

	t.Run("head_success", func(t *testing.T) {
		res, err := mapper.Head(context.Background())
		require.NoError(t, err)
		require.Equal(t, "group:fga", res)
	})

	t.Run("map_success", func(t *testing.T) {
		res, err := mapper.Next(context.Background())
		require.NoError(t, err)
		require.Equal(t, "group:fga", res)
	})
}
