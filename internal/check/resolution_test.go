package check

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResolutionNode(t *testing.T) {
	t.Run("NewResolutionNode", func(t *testing.T) {
		node := NewResolutionNode("document:1", "view")
		require.Equal(t, "document:1#view", node.Type)
		require.False(t, node.Result)
		require.False(t, node.startTime.IsZero())
	})

	t.Run("NewResolutionNodeFromType", func(t *testing.T) {
		node := NewResolutionNodeFromType("document:1#view")
		require.Equal(t, "document:1#view", node.Type)
	})

	t.Run("Complete", func(t *testing.T) {
		node := NewResolutionNode("document:1", "view")
		time.Sleep(1 * time.Millisecond) // Ensure some duration
		node.Complete(true, 3)

		require.True(t, node.Result)
		require.Equal(t, 3, node.ItemCount)
		require.NotEmpty(t, node.Duration)
	})

	t.Run("SetUnion", func(t *testing.T) {
		node := NewResolutionNode("document:1", "view")
		branch1 := NewResolutionNode("document:1", "editor")
		branch2 := NewResolutionNode("document:1", "owner")

		node.SetUnion([]*ResolutionNode{branch1, branch2})

		require.NotNil(t, node.Union)
		require.Len(t, node.Union.Branches, 2)
	})

	t.Run("SetIntersection", func(t *testing.T) {
		node := NewResolutionNode("document:1", "view")
		branch1 := NewResolutionNode("document:1", "editor")
		branch2 := NewResolutionNode("document:1", "owner")

		node.SetIntersection([]*ResolutionNode{branch1, branch2})

		require.NotNil(t, node.Intersection)
		require.Len(t, node.Intersection.Branches, 2)
	})

	t.Run("SetExclusion", func(t *testing.T) {
		node := NewResolutionNode("document:1", "view")
		base := NewResolutionNode("document:1", "editor")
		subtract := NewResolutionNode("document:1", "blocked")

		node.SetExclusion(base, subtract)

		require.NotNil(t, node.Exclusion)
		require.Equal(t, base, node.Exclusion.Base)
		require.Equal(t, subtract, node.Exclusion.Subtract)
	})

	t.Run("AddTuple", func(t *testing.T) {
		node := NewResolutionNode("document:1", "view")
		tuple := NewTupleNodeFromString("document:1#viewer@user:alice")

		node.AddTuple(tuple)

		require.Len(t, node.Tuples, 1)
		require.Equal(t, "document:1#viewer@user:alice", node.Tuples[0].Tuple)
	})

	t.Run("CountTuples", func(t *testing.T) {
		// Create a tree:
		// root (union)
		//   - branch1 (2 tuples)
		//   - branch2 (1 tuple with computed child having 1 tuple)
		root := NewResolutionNode("document:1", "view")
		branch1 := NewResolutionNode("document:1", "editor")
		branch1.AddTuple(NewTupleNodeFromString("document:1#editor@user:alice"))
		branch1.AddTuple(NewTupleNodeFromString("document:1#editor@user:bob"))

		branch2 := NewResolutionNode("document:1", "owner")
		computedChild := NewResolutionNode("group:eng", "member")
		computedChild.AddTuple(NewTupleNodeFromString("group:eng#member@user:charlie"))
		branch2tuple := NewTupleNodeFromString("document:1#owner@group:eng#member")
		branch2tuple.Computed = computedChild
		branch2.AddTuple(branch2tuple)

		root.SetUnion([]*ResolutionNode{branch1, branch2})

		// root has 0 direct tuples
		// branch1 has 2 tuples
		// branch2 has 1 tuple with 1 computed child tuple
		// Total: 2 + 1 + 1 = 4 tuples
		count := root.CountTuples()
		require.Equal(t, 4, count)
	})
}

func TestTupleNode(t *testing.T) {
	t.Run("NewTupleNodeFromString", func(t *testing.T) {
		node := NewTupleNodeFromString("document:1#viewer@user:alice")
		require.Equal(t, "document:1#viewer@user:alice", node.Tuple)
		require.Nil(t, node.Computed)
	})
}

func TestResolutionTree(t *testing.T) {
	t.Run("basic_tree", func(t *testing.T) {
		root := NewResolutionNode("document:1", "view")
		root.AddTuple(NewTupleNodeFromString("document:1#viewer@user:alice"))
		root.Complete(true, 1)

		tree := &ResolutionTree{
			Check:  "document:1#view@user:alice",
			Result: true,
			Tree:   root,
		}

		require.Equal(t, "document:1#view@user:alice", tree.Check)
		require.True(t, tree.Result)
		require.NotNil(t, tree.Tree)
	})
}

func TestCountTuplesInBranches(t *testing.T) {
	t.Run("counts_only_successful_branches", func(t *testing.T) {
		branch1 := NewResolutionNode("document:1", "editor")
		branch1.Complete(true, 2)

		branch2 := NewResolutionNode("document:1", "owner")
		branch2.Complete(false, 0)

		branch3 := NewResolutionNode("document:1", "member")
		branch3.Complete(true, 3)

		branches := []*ResolutionNode{branch1, branch2, branch3}
		count := countTuplesInBranches(branches)

		// Only branch1 (2) and branch3 (3) count because they returned true
		require.Equal(t, 5, count)
	})

	t.Run("handles_nil_branches", func(t *testing.T) {
		branch1 := NewResolutionNode("document:1", "editor")
		branch1.Complete(true, 2)

		branches := []*ResolutionNode{nil, branch1, nil}
		count := countTuplesInBranches(branches)

		require.Equal(t, 2, count)
	})

	t.Run("returns_zero_for_empty", func(t *testing.T) {
		count := countTuplesInBranches(nil)
		require.Equal(t, 0, count)

		count = countTuplesInBranches([]*ResolutionNode{})
		require.Equal(t, 0, count)
	})
}

func TestMergeResolutionNodes(t *testing.T) {
	t.Run("returns_nil_for_empty", func(t *testing.T) {
		result := MergeResolutionNodes(nil)
		require.Nil(t, result)

		result = MergeResolutionNodes([]*ResolutionNode{})
		require.Nil(t, result)
	})

	t.Run("returns_single_node_unchanged", func(t *testing.T) {
		node := NewResolutionNode("document:1", "view")
		result := MergeResolutionNodes([]*ResolutionNode{node})
		require.Equal(t, node, result)
	})

	t.Run("merges_multiple_nodes_into_union", func(t *testing.T) {
		node1 := NewResolutionNode("document:1", "view")
		node2 := NewResolutionNode("document:1", "edit")

		result := MergeResolutionNodes([]*ResolutionNode{node1, node2})

		require.NotNil(t, result)
		require.NotNil(t, result.Union)
		require.Len(t, result.Union.Branches, 2)
	})
}
