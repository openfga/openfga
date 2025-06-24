package typesystem

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/pkg/testutils"
)

func TestGetEdgesForIntersection(t *testing.T) {
	t.Run("simple_direct_assignment_equal_weight", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type group
			relations
				define allowed: [user]
				define member: [user] and allowed
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)

		edges, _, err := typeSystem.GetEdgesForListObjects("group#member", "user")
		require.NoError(t, err)

		require.Len(t, edges, 1)
		rootIntersectionNode := edges[0].GetTo()
		require.Equal(t, graph.IntersectionOperator, rootIntersectionNode.GetLabel())
		require.Equal(t, graph.OperatorNode, rootIntersectionNode.GetNodeType())
		actualIntersectionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootIntersectionNode)
		require.True(t, ok)
		comparator, err := GetEdgesForIntersection(actualIntersectionEdges, "user")
		require.NoError(t, err)
		require.True(t, comparator.DirectEdgesAreLeastWeight)
		require.Nil(t, comparator.LowestEdge)
		require.Len(t, comparator.DirectEdges, 1)
		require.Equal(t, "user", comparator.DirectEdges[0].GetTo().GetUniqueLabel())
		require.Equal(t, graph.DirectEdge, comparator.DirectEdges[0].GetEdgeType())
		require.Len(t, comparator.Siblings, 1)
		require.Equal(t, "group#allowed", comparator.Siblings[0].GetTo().GetUniqueLabel())
		require.Equal(t, graph.RewriteEdge, comparator.Siblings[0].GetEdgeType())
	})
	t.Run("direct_assignment_lowest_weight", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type user2
		type team
			relations
				define member: [user]
				define member2: [user2]
				define member3: [user, user2]
		type group
			relations
				define parent: [team]
				define allowed: [user]
				define member: [user, user:*, user2] and allowed and member from parent and member3 from parent
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)

		edges, _, err := typeSystem.GetEdgesForListObjects("group#member", "user")
		require.NoError(t, err)

		require.Len(t, edges, 1)
		rootIntersectionNode := edges[0].GetTo()
		require.Equal(t, graph.IntersectionOperator, rootIntersectionNode.GetLabel())
		require.Equal(t, graph.OperatorNode, rootIntersectionNode.GetNodeType())
		actualIntersectionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootIntersectionNode)
		require.True(t, ok)
		comparator, err := GetEdgesForIntersection(actualIntersectionEdges, "user")
		require.NoError(t, err)
		require.True(t, comparator.DirectEdgesAreLeastWeight)
		require.Nil(t, comparator.LowestEdge)
		require.Len(t, comparator.DirectEdges, 2)
		require.Equal(t, "user", comparator.DirectEdges[0].GetTo().GetUniqueLabel())
		require.Equal(t, graph.DirectEdge, comparator.DirectEdges[0].GetEdgeType())
		require.Equal(t, "user:*", comparator.DirectEdges[1].GetTo().GetUniqueLabel())
		require.Equal(t, graph.DirectEdge, comparator.DirectEdges[1].GetEdgeType())
		require.Len(t, comparator.Siblings, 3)
		require.Equal(t, "group#allowed", comparator.Siblings[0].GetTo().GetUniqueLabel())
		require.Equal(t, graph.RewriteEdge, comparator.Siblings[0].GetEdgeType())
		require.Equal(t, "team#member", comparator.Siblings[1].GetTo().GetUniqueLabel())
		require.Equal(t, graph.TTUEdge, comparator.Siblings[1].GetEdgeType())
		require.Equal(t, "team#member3", comparator.Siblings[2].GetTo().GetUniqueLabel())
		require.Equal(t, graph.TTUEdge, comparator.Siblings[2].GetEdgeType())
	})

	t.Run("direct_assignment_not_lowest_weight_no_siblings", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type user2
		type team
			relations
				define member: [user]
				define member2: [user2]
				define member3: [user, user2]
		type group
			relations
				define allowed: [user]
				define member: [team#member, team#member2, team#member3] and allowed
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)

		edges, _, err := typeSystem.GetEdgesForListObjects("group#member", "user")
		require.NoError(t, err)

		require.Len(t, edges, 1)
		rootIntersectionNode := edges[0].GetTo()
		require.Equal(t, graph.IntersectionOperator, rootIntersectionNode.GetLabel())
		require.Equal(t, graph.OperatorNode, rootIntersectionNode.GetNodeType())
		actualIntersectionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootIntersectionNode)
		require.True(t, ok)
		comparator, err := GetEdgesForIntersection(actualIntersectionEdges, "user")
		require.NoError(t, err)
		require.False(t, comparator.DirectEdgesAreLeastWeight)
		require.NotNil(t, comparator.LowestEdge)
		require.Equal(t, graph.RewriteEdge, comparator.LowestEdge.GetEdgeType())
		require.Equal(t, "group#allowed", comparator.LowestEdge.GetTo().GetUniqueLabel())
		require.Len(t, comparator.DirectEdges, 2)
		require.Equal(t, "team#member", comparator.DirectEdges[0].GetTo().GetUniqueLabel())
		require.Equal(t, graph.DirectEdge, comparator.DirectEdges[0].GetEdgeType())
		require.Equal(t, "team#member3", comparator.DirectEdges[1].GetTo().GetUniqueLabel())
		require.Equal(t, graph.DirectEdge, comparator.DirectEdges[1].GetEdgeType())
		require.Empty(t, comparator.Siblings)
	})

	t.Run("direct_assignment_not_lowest_weight_siblings", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type subteam
			relations
				define member: [user]
		type adhoc
			relations
				define member: [user]
		type team
			relations
				define member: [subteam#member]
		type group
			relations
				define subteam: [subteam]
				define adhoc_member: [adhoc#member]
				define member: [team#member, user, user:*] and member from subteam and adhoc_member
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)

		edges, _, err := typeSystem.GetEdgesForListObjects("group#member", "user")
		require.NoError(t, err)

		require.Len(t, edges, 1)
		rootIntersectionNode := edges[0].GetTo()
		require.Equal(t, graph.IntersectionOperator, rootIntersectionNode.GetLabel())
		require.Equal(t, graph.OperatorNode, rootIntersectionNode.GetNodeType())
		actualIntersectionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootIntersectionNode)
		require.True(t, ok)
		comparator, err := GetEdgesForIntersection(actualIntersectionEdges, "user")
		require.NoError(t, err)
		require.False(t, comparator.DirectEdgesAreLeastWeight)
		require.NotNil(t, comparator.LowestEdge)
		require.Equal(t, graph.TTUEdge, comparator.LowestEdge.GetEdgeType())
		require.Equal(t, "subteam#member", comparator.LowestEdge.GetTo().GetUniqueLabel())
		require.Len(t, comparator.DirectEdges, 3)
		require.Equal(t, "team#member", comparator.DirectEdges[0].GetTo().GetUniqueLabel())
		require.Equal(t, graph.DirectEdge, comparator.DirectEdges[0].GetEdgeType())
		require.Equal(t, graph.SpecificTypeAndRelation, comparator.DirectEdges[0].GetTo().GetNodeType())
		require.Equal(t, "user", comparator.DirectEdges[1].GetTo().GetUniqueLabel())
		require.Equal(t, graph.DirectEdge, comparator.DirectEdges[1].GetEdgeType())
		require.Equal(t, graph.SpecificType, comparator.DirectEdges[1].GetTo().GetNodeType())
		require.Equal(t, "user:*", comparator.DirectEdges[2].GetTo().GetUniqueLabel())
		require.Equal(t, graph.DirectEdge, comparator.DirectEdges[2].GetEdgeType())
		require.Equal(t, graph.SpecificTypeWildcard, comparator.DirectEdges[2].GetTo().GetNodeType())
		require.Len(t, comparator.Siblings, 1)
		require.Equal(t, "group#adhoc_member", comparator.Siblings[0].GetTo().GetUniqueLabel())
		require.Equal(t, graph.RewriteEdge, comparator.Siblings[0].GetEdgeType())
	})

	t.Run("no_direct_assignment", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type subteam
			relations
				define member: [user]
		type adhoc
			relations
				define member: [user]
		type team
			relations
				define member: [subteam#member]
		type group
			relations
				define team: [team]
				define subteam: [subteam]
				define adhoc_member: [adhoc#member]
				define member: member from team and adhoc_member and member from subteam
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)

		edges, _, err := typeSystem.GetEdgesForListObjects("group#member", "user")
		require.NoError(t, err)

		require.Len(t, edges, 1)
		rootIntersectionNode := edges[0].GetTo()
		require.Equal(t, graph.IntersectionOperator, rootIntersectionNode.GetLabel())
		require.Equal(t, graph.OperatorNode, rootIntersectionNode.GetNodeType())
		actualIntersectionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootIntersectionNode)
		require.True(t, ok)
		comparator, err := GetEdgesForIntersection(actualIntersectionEdges, "user")
		require.NoError(t, err)
		require.False(t, comparator.DirectEdgesAreLeastWeight)
		require.NotNil(t, comparator.LowestEdge)
		require.Equal(t, graph.RewriteEdge, comparator.LowestEdge.GetEdgeType())
		require.Equal(t, "group#adhoc_member", comparator.LowestEdge.GetTo().GetUniqueLabel())
		require.Empty(t, comparator.DirectEdges)
		require.Len(t, comparator.Siblings, 2)
		require.Equal(t, "team#member", comparator.Siblings[0].GetTo().GetUniqueLabel())
		require.Equal(t, graph.TTUEdge, comparator.Siblings[0].GetEdgeType())
		require.Equal(t, "subteam#member", comparator.Siblings[1].GetTo().GetUniqueLabel())
		require.Equal(t, graph.TTUEdge, comparator.Siblings[1].GetEdgeType())
	})

	t.Run("direct_assignment_not_connected", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type user2
		type subteam
			relations
				define member: [user]
		type adhoc
			relations
				define member: [user]
		type team
			relations
				define member: [subteam#member]
		type group
			relations
				define team: [team]
				define subteam: [subteam]
				define adhoc_member: [adhoc#member]
				define member: [user2] and member from team and adhoc_member and member from subteam
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)

		edges, _, err := typeSystem.GetEdgesForListObjects("group#member", "user")
		require.NoError(t, err)

		require.Len(t, edges, 1)
		rootIntersectionNode := edges[0].GetTo()
		require.Equal(t, graph.IntersectionOperator, rootIntersectionNode.GetLabel())
		require.Equal(t, graph.OperatorNode, rootIntersectionNode.GetNodeType())
		actualIntersectionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootIntersectionNode)
		require.True(t, ok)
		comparator, err := GetEdgesForIntersection(actualIntersectionEdges, "user")
		require.NoError(t, err)
		require.Equal(t, IntersectionEdges{}, comparator)
	})

	t.Run("nested_operator", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type subteam
			relations
				define member: [user]
		type adhoc
			relations
				define member: [user]
		type team
			relations
				define member: [subteam#member]
		type group
			relations
				define team: [team]
				define subteam: [subteam]
				define adhoc_member: [adhoc#member]
				define member: (member from team and adhoc_member) and member from subteam
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)

		edges, _, err := typeSystem.GetEdgesForListObjects("group#member", "user")
		require.NoError(t, err)

		require.Len(t, edges, 1)
		rootIntersectionNode := edges[0].GetTo()
		require.Equal(t, graph.IntersectionOperator, rootIntersectionNode.GetLabel())
		require.Equal(t, graph.OperatorNode, rootIntersectionNode.GetNodeType())
		actualIntersectionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootIntersectionNode)
		require.True(t, ok)
		comparator, err := GetEdgesForIntersection(actualIntersectionEdges, "user")
		require.NoError(t, err)
		require.False(t, comparator.DirectEdgesAreLeastWeight)
		require.NotNil(t, comparator.LowestEdge)
		require.Equal(t, graph.TTUEdge, comparator.LowestEdge.GetEdgeType())
		require.Equal(t, "subteam#member", comparator.LowestEdge.GetTo().GetUniqueLabel())
		require.Empty(t, comparator.DirectEdges)
		require.Len(t, comparator.Siblings, 1)
		require.Equal(t, graph.OperatorNode, comparator.Siblings[0].GetTo().GetNodeType())
		require.Equal(t, graph.RewriteEdge, comparator.Siblings[0].GetEdgeType())
		require.Equal(t, graph.IntersectionOperator, comparator.Siblings[0].GetTo().GetLabel())
	})
	t.Run("non_directed_assignment_no_path", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type user2
		type subteam
			relations
				define member: [user,user2]
				define member2: [user2]
		type adhoc
			relations
				define member: [user]
		type team
			relations
				define member: [subteam#member]
		type group
			relations
				define subteam: [subteam]
				define adhoc_member: [adhoc#member]
				define member: [team#member, user, user:*] and member2 from subteam and adhoc_member
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)

		edges, _, err := typeSystem.GetEdgesForListObjects("group#member", "user")
		require.NoError(t, err)
		require.Len(t, edges, 1)
		rootIntersectionNode := edges[0].GetTo()
		require.Equal(t, graph.IntersectionOperator, rootIntersectionNode.GetLabel())
		require.Equal(t, graph.OperatorNode, rootIntersectionNode.GetNodeType())
		actualIntersectionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootIntersectionNode)
		require.True(t, ok)
		comparator, err := GetEdgesForIntersection(actualIntersectionEdges, "user")
		require.NoError(t, err)
		require.Equal(t, IntersectionEdges{}, comparator)
	})

	t.Run("error_non_intersection_node", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type user2
		type group
			relations
				define allowed: [user]
				define member: [user] and allowed
				define other: [user2]
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)
		currentNode, ok := typeSystem.authzWeightedGraph.GetNodeByID("group#other")
		require.True(t, ok)

		actualIntersectionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(currentNode)
		require.True(t, ok)
		_, err = GetEdgesForIntersection(actualIntersectionEdges, "user")
		require.Error(t, err)
	})
}

func TestGetEdgesForExclusion(t *testing.T) {
	t.Run("direct_edge_for_exclusion", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type user2
		type group
			relations
				define banned: [user]
				define not_relation: [user, user2] but not banned
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)

		edges, _, err := typeSystem.GetEdgesForListObjects("group#not_relation", "user")
		require.NoError(t, err)

		require.Len(t, edges, 1)
		rootExclusionNode := edges[0].GetTo()
		require.Equal(t, graph.ExclusionOperator, rootExclusionNode.GetLabel())
		require.Equal(t, graph.OperatorNode, rootExclusionNode.GetNodeType())
		actualExclusionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootExclusionNode)
		require.True(t, ok)
		baseEdges, exclusionEdge, err := GetEdgesForExclusion(actualExclusionEdges, "user")
		require.NoError(t, err)
		require.Len(t, baseEdges, 1)
		require.Equal(t, "user", baseEdges[0].GetTo().GetUniqueLabel())
		require.Equal(t, graph.SpecificType, baseEdges[0].GetTo().GetNodeType())
		require.Equal(t, graph.DirectEdge, baseEdges[0].GetEdgeType())

		require.NotNil(t, exclusionEdge)
		require.Equal(t, "group#banned", exclusionEdge.GetTo().GetUniqueLabel())
		require.Equal(t, graph.SpecificTypeAndRelation, exclusionEdge.GetTo().GetNodeType())
		require.Equal(t, graph.RewriteEdge, exclusionEdge.GetEdgeType())
	})

	t.Run("multiple_direct_edges_for_exclusion", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type team
			relations
				define member: [user]
		type group
			relations
				define banned: [user]
				define not_relation: [user, user:*, team#member] but not banned
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)

		edges, _, err := typeSystem.GetEdgesForListObjects("group#not_relation", "user")
		require.NoError(t, err)

		require.Len(t, edges, 1)
		rootExclusionNode := edges[0].GetTo()
		require.Equal(t, graph.ExclusionOperator, rootExclusionNode.GetLabel())
		require.Equal(t, graph.OperatorNode, rootExclusionNode.GetNodeType())
		actualExclusionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootExclusionNode)
		require.True(t, ok)
		baseEdges, exclusionEdge, err := GetEdgesForExclusion(actualExclusionEdges, "user")
		require.NoError(t, err)
		require.Len(t, baseEdges, 3)
		require.Equal(t, "user", baseEdges[0].GetTo().GetUniqueLabel())
		require.Equal(t, graph.SpecificType, baseEdges[0].GetTo().GetNodeType())
		require.Equal(t, graph.DirectEdge, baseEdges[0].GetEdgeType())
		require.Equal(t, "user:*", baseEdges[1].GetTo().GetUniqueLabel())
		require.Equal(t, graph.SpecificTypeWildcard, baseEdges[1].GetTo().GetNodeType())
		require.Equal(t, graph.DirectEdge, baseEdges[1].GetEdgeType())
		require.Equal(t, "team#member", baseEdges[2].GetTo().GetUniqueLabel())
		require.Equal(t, graph.SpecificTypeAndRelation, baseEdges[2].GetTo().GetNodeType())
		require.Equal(t, graph.DirectEdge, baseEdges[2].GetEdgeType())

		require.NotNil(t, exclusionEdge)
		require.Equal(t, "group#banned", exclusionEdge.GetTo().GetUniqueLabel())
		require.Equal(t, graph.SpecificTypeAndRelation, exclusionEdge.GetTo().GetNodeType())
		require.Equal(t, graph.RewriteEdge, exclusionEdge.GetEdgeType())
	})
	t.Run("no_direct_edges_for_exclusion", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type group
			relations
				define banned: [user]
				define member: [user]
				define not_relation: member but not banned
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)

		edges, _, err := typeSystem.GetEdgesForListObjects("group#not_relation", "user")
		require.NoError(t, err)

		require.Len(t, edges, 1)
		rootExclusionNode := edges[0].GetTo()
		require.Equal(t, graph.ExclusionOperator, rootExclusionNode.GetLabel())
		require.Equal(t, graph.OperatorNode, rootExclusionNode.GetNodeType())
		actualExclusionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootExclusionNode)
		require.True(t, ok)
		baseEdges, exclusionEdge, err := GetEdgesForExclusion(actualExclusionEdges, "user")
		require.NoError(t, err)
		require.Len(t, baseEdges, 1)
		require.Equal(t, "group#member", baseEdges[0].GetTo().GetUniqueLabel())
		require.Equal(t, graph.SpecificTypeAndRelation, baseEdges[0].GetTo().GetNodeType())
		require.Equal(t, graph.RewriteEdge, baseEdges[0].GetEdgeType())
		require.NotNil(t, exclusionEdge)
		require.Equal(t, "group#banned", exclusionEdge.GetTo().GetUniqueLabel())
		require.Equal(t, graph.SpecificTypeAndRelation, exclusionEdge.GetTo().GetNodeType())
		require.Equal(t, graph.RewriteEdge, exclusionEdge.GetEdgeType())
	})
	t.Run("ttu_edge_exclusion", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type team
			relations
				define member: [user]
		type group
			relations
				define banned_team: [team]
				define member: [user]
				define not_relation: member but not member from banned_team
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)

		edges, _, err := typeSystem.GetEdgesForListObjects("group#not_relation", "user")
		require.NoError(t, err)

		require.Len(t, edges, 1)
		rootExclusionNode := edges[0].GetTo()
		require.Equal(t, graph.ExclusionOperator, rootExclusionNode.GetLabel())
		require.Equal(t, graph.OperatorNode, rootExclusionNode.GetNodeType())
		actualExclusionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootExclusionNode)
		require.True(t, ok)
		baseEdges, exclusionEdge, err := GetEdgesForExclusion(actualExclusionEdges, "user")
		require.NoError(t, err)
		require.Len(t, baseEdges, 1)
		require.Equal(t, "group#member", baseEdges[0].GetTo().GetUniqueLabel())
		require.Equal(t, graph.SpecificTypeAndRelation, baseEdges[0].GetTo().GetNodeType())
		require.NotNil(t, exclusionEdge)
		require.Equal(t, graph.TTUEdge, exclusionEdge.GetEdgeType())
		require.Equal(t, "team#member", exclusionEdge.GetTo().GetUniqueLabel())
		require.Equal(t, graph.SpecificTypeAndRelation, exclusionEdge.GetTo().GetNodeType())
	})
	t.Run("exclusion_edge_not_connected", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type user2
		type team
			relations
				define member: [user]
		type group
			relations
				define member2: [user2]
				define member: [user]
				define not_relation: member but not member2
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)

		edges, _, err := typeSystem.GetEdgesForListObjects("group#not_relation", "user")
		require.NoError(t, err)

		require.Len(t, edges, 1)
		rootExclusionNode := edges[0].GetTo()
		require.Equal(t, graph.ExclusionOperator, rootExclusionNode.GetLabel())
		require.Equal(t, graph.OperatorNode, rootExclusionNode.GetNodeType())
		actualExclusionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootExclusionNode)
		require.True(t, ok)
		baseEdges, exclusionEdge, err := GetEdgesForExclusion(actualExclusionEdges, "user")
		require.NoError(t, err)
		require.Len(t, baseEdges, 1)
		require.Equal(t, "group#member", baseEdges[0].GetTo().GetUniqueLabel())
		require.Equal(t, graph.SpecificTypeAndRelation, baseEdges[0].GetTo().GetNodeType())
		require.Nil(t, exclusionEdge)
	})
	t.Run("nested_exclusion_edge", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type team
			relations
				define member: [user]
		type group
			relations
				define banned_user: [user]
				define banned_team: [team]
				define member: [user]
				define not_relation: member but not (banned_user or member from banned_team)
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)

		edges, _, err := typeSystem.GetEdgesForListObjects("group#not_relation", "user")
		require.NoError(t, err)

		require.Len(t, edges, 1)
		rootExclusionNode := edges[0].GetTo()
		require.Equal(t, graph.ExclusionOperator, rootExclusionNode.GetLabel())
		require.Equal(t, graph.OperatorNode, rootExclusionNode.GetNodeType())
		actualExclusionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootExclusionNode)
		require.True(t, ok)
		baseEdges, exclusionEdge, err := GetEdgesForExclusion(actualExclusionEdges, "user")
		require.NoError(t, err)
		require.Len(t, baseEdges, 1)
		require.Equal(t, "group#member", baseEdges[0].GetTo().GetUniqueLabel())
		require.Equal(t, graph.SpecificTypeAndRelation, baseEdges[0].GetTo().GetNodeType())
		require.NotNil(t, exclusionEdge)
		require.Equal(t, graph.RewriteEdge, exclusionEdge.GetEdgeType())
		bannedNode := exclusionEdge.GetTo()
		require.Equal(t, graph.OperatorNode, bannedNode.GetNodeType())
		require.Equal(t, graph.UnionOperator, bannedNode.GetLabel())
	})

	t.Run("error_non_intersection_node", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type user2
		type group
			relations
				define banned: [user]
				define not_relation: [user, user2] but not banned
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)

		currentNode, ok := typeSystem.authzWeightedGraph.GetNodeByID("group#banned")
		require.True(t, ok)
		dut, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(currentNode)
		require.True(t, ok)
		_, _, err = GetEdgesForExclusion(dut, "user")
		require.Error(t, err)
	})
}
