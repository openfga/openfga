package typesystem

import (
	"testing"

	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
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

func TestConstructUserset(t *testing.T) {
	t.Run("rewrite_edge", func(t *testing.T) {
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
		rootNode, ok := typeSystem.authzWeightedGraph.GetNodeByID("group#member")
		require.True(t, ok)
		rootEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootNode)
		require.True(t, ok)
		require.Len(t, rootEdges, 1)
		intersectionNode := rootEdges[0].GetTo()
		require.NotNil(t, intersectionNode)
		require.Equal(t, graph.IntersectionOperator, rootEdges[0].GetTo().GetLabel())
		intersectionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(intersectionNode)
		require.True(t, ok)
		require.Len(t, intersectionEdges, 2)
		dut := intersectionEdges[1]
		require.Equal(t, "group#allowed", dut.GetTo().GetLabel())
		require.Equal(t, graph.SpecificTypeAndRelation, dut.GetTo().GetNodeType())
		require.Equal(t, graph.RewriteEdge, dut.GetEdgeType())
		userset, err := typeSystem.ConstructUserset(dut)
		require.NoError(t, err)
		require.Equal(t, &openfgav1.Userset{
			Userset: &openfgav1.Userset_ComputedUserset{
				ComputedUserset: &openfgav1.ObjectRelation{
					Relation: "allowed",
				},
			},
		}, userset)
	})
	t.Run("ttu_edge", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type team
			relations
				define member: [user]
		type group
			relations
				define parent: [team]
				define member: [user] and member from parent
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)
		rootNode, ok := typeSystem.authzWeightedGraph.GetNodeByID("group#member")
		require.True(t, ok)
		rootEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootNode)
		require.True(t, ok)
		require.Len(t, rootEdges, 1)
		intersectionNode := rootEdges[0].GetTo()
		require.NotNil(t, intersectionNode)
		require.Equal(t, graph.IntersectionOperator, rootEdges[0].GetTo().GetLabel())
		intersectionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(intersectionNode)
		require.True(t, ok)
		require.Len(t, intersectionEdges, 2)
		dut := intersectionEdges[1]
		require.Equal(t, "team#member", dut.GetTo().GetLabel())
		require.Equal(t, graph.SpecificTypeAndRelation, dut.GetTo().GetNodeType())
		require.Equal(t, graph.TTUEdge, dut.GetEdgeType())
		userset, err := typeSystem.ConstructUserset(dut)
		require.NoError(t, err)
		require.Equal(t, &openfgav1.Userset{
			Userset: &openfgav1.Userset_TupleToUserset{
				TupleToUserset: &openfgav1.TupleToUserset{
					Tupleset: &openfgav1.ObjectRelation{
						Relation: "team",
					},
					ComputedUserset: &openfgav1.ObjectRelation{
						Relation: "member",
					},
				},
			},
		}, userset)
	})
	t.Run("direct_edge", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type team
			relations
				define member: [user]
		type group
			relations
				define allowed: [user]
				define member: [user, team#member] and allowed
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)
		rootNode, ok := typeSystem.authzWeightedGraph.GetNodeByID("group#member")
		require.True(t, ok)
		rootEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootNode)
		require.True(t, ok)
		require.Len(t, rootEdges, 1)
		intersectionNode := rootEdges[0].GetTo()
		require.NotNil(t, intersectionNode)
		require.Equal(t, graph.IntersectionOperator, rootEdges[0].GetTo().GetLabel())
		intersectionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(intersectionNode)
		require.True(t, ok)
		require.Len(t, intersectionEdges, 3)
		dut := intersectionEdges[0]
		require.Equal(t, "user", dut.GetTo().GetLabel())
		require.Equal(t, graph.SpecificType, dut.GetTo().GetNodeType())
		require.Equal(t, graph.DirectEdge, dut.GetEdgeType())
		userset, err := typeSystem.ConstructUserset(dut)
		require.NoError(t, err)
		require.Equal(t, This(), userset)
	})
	t.Run("direct_edge_public", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type team
			relations
				define member: [user]
		type group
			relations
				define allowed: [user]
				define member: [user:*, team#member] and allowed
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)
		rootNode, ok := typeSystem.authzWeightedGraph.GetNodeByID("group#member")
		require.True(t, ok)
		rootEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootNode)
		require.True(t, ok)
		require.Len(t, rootEdges, 1)
		intersectionNode := rootEdges[0].GetTo()
		require.NotNil(t, intersectionNode)
		require.Equal(t, graph.IntersectionOperator, rootEdges[0].GetTo().GetLabel())
		intersectionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(intersectionNode)
		require.True(t, ok)
		require.Len(t, intersectionEdges, 3)
		dut := intersectionEdges[0]
		require.Equal(t, "user:*", dut.GetTo().GetLabel())
		require.Equal(t, graph.SpecificTypeWildcard, dut.GetTo().GetNodeType())
		require.Equal(t, graph.DirectEdge, dut.GetEdgeType())
		userset, err := typeSystem.ConstructUserset(dut)
		require.NoError(t, err)
		require.Equal(t, This(), userset)
	})
	t.Run("direct_edge_userset", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type team
			relations
				define member: [user]
		type group
			relations
				define allowed: [user]
				define member: [user, team#member] and allowed
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)
		rootNode, ok := typeSystem.authzWeightedGraph.GetNodeByID("group#member")
		require.True(t, ok)
		rootEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootNode)
		require.True(t, ok)
		require.Len(t, rootEdges, 1)
		intersectionNode := rootEdges[0].GetTo()
		require.NotNil(t, intersectionNode)
		require.Equal(t, graph.IntersectionOperator, rootEdges[0].GetTo().GetLabel())
		intersectionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(intersectionNode)
		require.True(t, ok)
		require.Len(t, intersectionEdges, 3)
		dut := intersectionEdges[1]
		require.Equal(t, "team#member", dut.GetTo().GetLabel())
		require.Equal(t, graph.SpecificTypeAndRelation, dut.GetTo().GetNodeType())
		require.Equal(t, graph.DirectEdge, dut.GetEdgeType())
		userset, err := typeSystem.ConstructUserset(dut)
		require.NoError(t, err)
		require.Equal(t, &openfgav1.Userset{
			Userset: &openfgav1.Userset_ComputedUserset{
				ComputedUserset: &openfgav1.ObjectRelation{
					Object:   "team",
					Relation: "member",
				},
			},
		}, userset)
	})
	t.Run("operator_union", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type team
			relations
				define member: [user]
		type group
			relations
				define allowed: [user]
				define granted: [user]
				define member: [user] and (allowed or granted)
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)
		rootNode, ok := typeSystem.authzWeightedGraph.GetNodeByID("group#member")
		require.True(t, ok)
		rootEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootNode)
		require.True(t, ok)
		require.Len(t, rootEdges, 1)
		intersectionNode := rootEdges[0].GetTo()
		require.NotNil(t, intersectionNode)
		require.Equal(t, graph.IntersectionOperator, rootEdges[0].GetTo().GetLabel())
		intersectionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(intersectionNode)
		require.True(t, ok)
		require.Len(t, intersectionEdges, 2)
		dut := intersectionEdges[1]
		require.Equal(t, graph.UnionOperator, dut.GetTo().GetLabel())
		require.Equal(t, graph.OperatorNode, dut.GetTo().GetNodeType())
		require.Equal(t, graph.RewriteEdge, dut.GetEdgeType())
		userset, err := typeSystem.ConstructUserset(dut)
		require.NoError(t, err)
		require.Equal(t, &openfgav1.Userset{
			Userset: &openfgav1.Userset_Union{
				Union: &openfgav1.Usersets{
					Child: []*openfgav1.Userset{
						{
							Userset: &openfgav1.Userset_ComputedUserset{
								ComputedUserset: &openfgav1.ObjectRelation{
									Relation: "allowed",
								},
							},
						},
						{
							Userset: &openfgav1.Userset_ComputedUserset{
								ComputedUserset: &openfgav1.ObjectRelation{
									Relation: "granted",
								},
							},
						},
					},
				}}}, userset)
	})
	t.Run("operator_intersection", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type team
			relations
				define member: [user]
		type group
			relations
				define allowed: [user]
				define granted: [user]
				define member: [user] and (allowed and granted)
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)
		rootNode, ok := typeSystem.authzWeightedGraph.GetNodeByID("group#member")
		require.True(t, ok)
		rootEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootNode)
		require.True(t, ok)
		require.Len(t, rootEdges, 1)
		intersectionNode := rootEdges[0].GetTo()
		require.NotNil(t, intersectionNode)
		require.Equal(t, graph.IntersectionOperator, rootEdges[0].GetTo().GetLabel())
		intersectionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(intersectionNode)
		require.True(t, ok)
		require.Len(t, intersectionEdges, 2)
		dut := intersectionEdges[1]
		require.Equal(t, graph.IntersectionOperator, dut.GetTo().GetLabel())
		require.Equal(t, graph.OperatorNode, dut.GetTo().GetNodeType())
		require.Equal(t, graph.RewriteEdge, dut.GetEdgeType())
		userset, err := typeSystem.ConstructUserset(dut)
		require.NoError(t, err)
		require.Equal(t, &openfgav1.Userset{
			Userset: &openfgav1.Userset_Intersection{
				Intersection: &openfgav1.Usersets{
					Child: []*openfgav1.Userset{
						{
							Userset: &openfgav1.Userset_ComputedUserset{
								ComputedUserset: &openfgav1.ObjectRelation{
									Relation: "allowed",
								},
							},
						},
						{
							Userset: &openfgav1.Userset_ComputedUserset{
								ComputedUserset: &openfgav1.ObjectRelation{
									Relation: "granted",
								},
							},
						},
					},
				}}}, userset)
	})
	t.Run("operator_exclusion", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type user
		type team
			relations
				define member: [user]
		type group
			relations
				define allowed: [user]
				define banned: [user]
				define member: [user] and (allowed but not banned)
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)
		rootNode, ok := typeSystem.authzWeightedGraph.GetNodeByID("group#member")
		require.True(t, ok)
		rootEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootNode)
		require.True(t, ok)
		require.Len(t, rootEdges, 1)
		intersectionNode := rootEdges[0].GetTo()
		require.NotNil(t, intersectionNode)
		require.Equal(t, graph.IntersectionOperator, rootEdges[0].GetTo().GetLabel())
		intersectionEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(intersectionNode)
		require.True(t, ok)
		require.Len(t, intersectionEdges, 2)
		dut := intersectionEdges[1]
		require.Equal(t, graph.ExclusionOperator, dut.GetTo().GetLabel())
		require.Equal(t, graph.OperatorNode, dut.GetTo().GetNodeType())
		require.Equal(t, graph.RewriteEdge, dut.GetEdgeType())
		userset, err := typeSystem.ConstructUserset(dut)
		require.NoError(t, err)
		require.Equal(t, &openfgav1.Userset{
			Userset: &openfgav1.Userset_Difference{
				Difference: &openfgav1.Difference{
					Base: &openfgav1.Userset{
						Userset: &openfgav1.Userset_ComputedUserset{
							ComputedUserset: &openfgav1.ObjectRelation{
								Relation: "allowed",
							},
						},
					},
					Subtract: &openfgav1.Userset{
						Userset: &openfgav1.Userset_ComputedUserset{
							ComputedUserset: &openfgav1.ObjectRelation{
								Relation: "banned",
							},
						},
					},
				}}}, userset)
	})
	// the following are error cases requiring hand-crafted weighted graph
	t.Run("unknown_node_type", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type dont_care
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)
		dut := graph.NewWeightedAuthorizationModelGraph()
		dut.AddNode("root", "root", graph.SpecificTypeAndRelation)
		dut.AddNode("dut", "dut", -1) // unknown node type
		dut.AddEdge("root", "dut", graph.RewriteEdge, "", nil)
		typeSystem.authzWeightedGraph = dut
		rootNode, ok := dut.GetNodeByID("root")
		require.True(t, ok)
		rootEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootNode)
		require.True(t, ok)
		require.Len(t, rootEdges, 1)
		_, err = typeSystem.ConstructUserset(rootEdges[0])
		require.Error(t, err)
	})
	t.Run("unknown_edge_type_SpecificTypeAndRelation", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type dont_care
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)
		dut := graph.NewWeightedAuthorizationModelGraph()
		dut.AddNode("root", "root", graph.SpecificTypeAndRelation)
		dut.AddNode("dut", "dut", graph.SpecificTypeAndRelation)
		dut.AddEdge("root", "dut", -1, "", nil) // unknown edge type
		typeSystem.authzWeightedGraph = dut
		rootNode, ok := dut.GetNodeByID("root")
		require.True(t, ok)
		rootEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootNode)
		require.True(t, ok)
		require.Len(t, rootEdges, 1)
		_, err = typeSystem.ConstructUserset(rootEdges[0])
		require.Error(t, err)
	})
	t.Run("unknown_edge_type_OperatorNode", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type dont_care
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)
		dut := graph.NewWeightedAuthorizationModelGraph()
		dut.AddNode("root", "root", graph.SpecificTypeAndRelation)
		dut.AddNode("dut", "bad_label", graph.OperatorNode)
		dut.AddNode("child1", "child1", graph.SpecificTypeAndRelation)
		dut.AddNode("child2", "child2", graph.SpecificTypeAndRelation)

		dut.AddEdge("root", "dut", graph.RewriteEdge, "", nil)
		dut.AddEdge("dut", "child1", graph.RewriteEdge, "", nil)
		dut.AddEdge("dut", "child2", graph.RewriteEdge, "", nil)

		typeSystem.authzWeightedGraph = dut
		rootNode, ok := dut.GetNodeByID("root")
		require.True(t, ok)
		rootEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootNode)
		require.True(t, ok)
		require.Len(t, rootEdges, 1)
		_, err = typeSystem.ConstructUserset(rootEdges[0])
		require.Error(t, err)
	})
	t.Run("bad_exclusion_child", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type dont_care
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)
		dut := graph.NewWeightedAuthorizationModelGraph()
		dut.AddNode("root", "root", graph.SpecificTypeAndRelation)
		dut.AddNode("dut", graph.ExclusionOperator, graph.OperatorNode)
		dut.AddNode("child1", "child1", graph.SpecificTypeAndRelation)

		dut.AddEdge("root", "dut", graph.RewriteEdge, "", nil)
		dut.AddEdge("dut", "child1", graph.RewriteEdge, "", nil)

		typeSystem.authzWeightedGraph = dut
		rootNode, ok := dut.GetNodeByID("root")
		require.True(t, ok)
		rootEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootNode)
		require.True(t, ok)
		require.Len(t, rootEdges, 1)
		_, err = typeSystem.ConstructUserset(rootEdges[0])
		require.Error(t, err)
	})
	t.Run("no_edge_intersection", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type dont_care
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)
		dut := graph.NewWeightedAuthorizationModelGraph()
		dut.AddNode("root", "root", graph.SpecificTypeAndRelation)
		dut.AddNode("dut", graph.IntersectionOperator, graph.OperatorNode)

		dut.AddEdge("root", "dut", graph.RewriteEdge, "", nil)

		typeSystem.authzWeightedGraph = dut
		rootNode, ok := dut.GetNodeByID("root")
		require.True(t, ok)
		rootEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootNode)
		require.True(t, ok)
		require.Len(t, rootEdges, 1)
		_, err = typeSystem.ConstructUserset(rootEdges[0])
		require.Error(t, err)
	})
	t.Run("operator_children_error", func(t *testing.T) {
		model := `
		model
			schema 1.1
		type dont_care
		`
		typeSystem, err := New(testutils.MustTransformDSLToProtoWithID(model))
		require.NoError(t, err)
		dut := graph.NewWeightedAuthorizationModelGraph()
		dut.AddNode("root", "root", graph.SpecificTypeAndRelation)
		dut.AddNode("dut", graph.IntersectionOperator, graph.OperatorNode)
		dut.AddNode("child1", "child1", graph.SpecificTypeAndRelation)
		dut.AddNode("child2", "child2", -1) // bad node type

		dut.AddEdge("root", "dut", graph.RewriteEdge, "", nil)
		dut.AddEdge("dut", "child1", graph.RewriteEdge, "", nil)
		dut.AddEdge("dut", "child2", graph.RewriteEdge, "", nil)

		typeSystem.authzWeightedGraph = dut
		rootNode, ok := dut.GetNodeByID("root")
		require.True(t, ok)
		rootEdges, ok := typeSystem.authzWeightedGraph.GetEdgesFromNode(rootNode)
		require.True(t, ok)
		require.Len(t, rootEdges, 1)
		_, err = typeSystem.ConstructUserset(rootEdges[0])
		require.Error(t, err)
	})
}
