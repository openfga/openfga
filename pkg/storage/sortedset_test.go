package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedBlackTree(t *testing.T) {
	t.Run("empty_tree", func(t *testing.T) {
		tree := NewSortedSet()
		assert.Empty(t, tree.Min())
		assert.Empty(t, tree.Max())
		assert.Equal(t, []string{}, tree.Values())
		assert.Equal(t, 0, tree.Size())
		assert.False(t, tree.Exists("1"))
	})

	t.Run("non-empty_tree", func(t *testing.T) {
		tree := NewSortedSet()
		tree.Add("3")
		tree.Add("2")
		tree.Add("1")

		assert.Equal(t, "1", tree.Min())
		assert.Equal(t, "3", tree.Max())
		assert.Equal(t, []string{"1", "2", "3"}, tree.Values())
		assert.Equal(t, 3, tree.Size())
		assert.True(t, tree.Exists("1"))
		assert.True(t, tree.Exists("2"))
		assert.True(t, tree.Exists("3"))
		assert.False(t, tree.Exists("4"))
	})
}
