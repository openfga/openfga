package commands

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/storage/memory"
)

func TestNewListObjectsQuery(t *testing.T) {
	t.Run("nil_datastore", func(t *testing.T) {
		q, err := NewListObjectsQuery(nil, graph.NewLocalCheckerWithCycleDetection())
		require.Nil(t, q)
		require.Error(t, err)
	})

	t.Run("nil_checkResolver", func(t *testing.T) {
		q, err := NewListObjectsQuery(memory.New(), nil)
		require.Nil(t, q)
		require.Error(t, err)
	})
}
