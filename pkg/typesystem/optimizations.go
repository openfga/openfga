package typesystem

import (
	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/pkg/tuple"
)

// This file contains methods to detect whether an authorization model exhibits certain characteristics.
// This information can then be used to increase performance of a Check request.

// IsRelationWithRecursiveTTUAndAlgebraicOperations returns true if all these conditions apply:
// - node[objectType#relation].weights[userType] = infinite
// - node[objectType#relation] has only 1 edge to an OR node
// - The OR node has one TTU edge with weight infinite for the terminal type and the computed relation for the TTU is the same
// - Any other edge coming out of the OR node has weight 1 for the terminal type.
func (t *TypeSystem) IsRelationWithRecursiveTTUAndAlgebraicOperations(objectType, relation, userType string) bool {
	if t.authzWeightedGraph == nil {
		return false
	}
	objRel := tuple.ToObjectRelationString(objectType, relation)
	objRelNode, ok := t.authzWeightedGraph.GetNodeByID(objRel)
	if !ok {
		return false
	}

	w, ok := objRelNode.GetWeight(userType)
	if !ok || w != graph.Infinite {
		return false
	}

	edges, ok := t.authzWeightedGraph.GetEdgesByNode(objRelNode)
	if !ok || len(edges) != 1 {
		return false
	}

	edge := edges[0]
	if edge.GetTo().GetLabel() != graph.UnionOperator {
		return false
	}

	unionNode := edge.GetTo()

	edgesFromUnionNode, ok := t.authzWeightedGraph.GetEdgesByNode(unionNode)
	if !ok {
		return false
	}

	ttuEdgeSatisfiesCond, restOfEdgesAreWeight1, ttuEdgeCount := false, true, 0

	for _, edgeFromUnionNode := range edgesFromUnionNode {
		if edgeFromUnionNode.GetEdgeType() == graph.TTUEdge {
			ttuEdgeCount++
			if ttuEdgeCount > 1 {
				ttuEdgeSatisfiesCond = false
				break // only one TTU edge must be leaving the union node
			}
			ttuEdge := edgeFromUnionNode
			w, ok := ttuEdge.GetWeight(userType)
			if ok && w == graph.Infinite && ttuEdge.GetTo() == objRelNode {
				ttuEdgeSatisfiesCond = true
			}
		} else {
			w, ok := edgeFromUnionNode.GetWeight(userType)
			if !ok || w != 1 {
				restOfEdgesAreWeight1 = false
				break
			}
		}
	}

	return ttuEdgeSatisfiesCond && restOfEdgesAreWeight1
}
