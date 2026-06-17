package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
)

func TestTupleMappers(t *testing.T) {
	tk := &openfgav1.Tuple{
		Key: tuple.NewTupleKey("document:1", "viewer", "user:anne"),
	}
	userMapper := UserMapper()
	require.Equal(t, "user:anne", userMapper(tk))

	objectMapper := ObjectMapper()
	require.Equal(t, "document:1", objectMapper(tk))
}

func TestUsersetTupleMapper(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("group:fga", "member", "group:2#member"),
		tuple.NewTupleKey("group:fga", "member", "group:2"),
	}

	innerIter := NewUnorderedStaticTupleKeyIterator(tks)

	mapper := WrapIterator(UsersetKind, innerIter)
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
		require.Empty(t, res)
	})
}

func TestTTUTupleMapper(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("group:fga", "member", "group:2#member"),
	}

	innerIter := NewUnorderedStaticTupleKeyIterator(tks)

	mapper := WrapIterator(TTUKind, innerIter)
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

func TestMapperIsOrdered(t *testing.T) {
	tks := []*openfgav1.TupleKey{tuple.NewTupleKey("group:eng", "member", "user:alice#member")}
	inner := NewOrderedStaticTupleKeyIterator(tks)

	t.Run("UsersetMapper_forwards", func(t *testing.T) {
		m := WrapIterator(UsersetKind, inner)
		require.True(t, m.IsOrdered())
	})

	inner2 := NewOrderedStaticTupleKeyIterator(tks)
	t.Run("TTUMapper_forwards", func(t *testing.T) {
		m := WrapIterator(TTUKind, inner2)
		require.True(t, m.IsOrdered())
	})

	inner3 := NewOrderedStaticTupleKeyIterator(tks)
	t.Run("ObjectIDMapper_forwards", func(t *testing.T) {
		m := WrapIterator(ObjectIDKind, inner3)
		require.True(t, m.IsOrdered())
	})
}

func TestObjectIDTupleMapper(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	tks := []*openfgav1.TupleKey{
		tuple.NewTupleKey("group:fga", "member", "group:2#member"),
	}

	innerIter := NewUnorderedStaticTupleKeyIterator(tks)

	mapper := WrapIterator(ObjectIDKind, innerIter)
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
