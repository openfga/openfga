package typesystem

import (
	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/pkg/tuple"
)

// This file contains methods to detect whether an authorization model exhibits certain characteristics.
// This information can then be used to increase performance of a Check request.

// IsRelationWithRecursiveTTUAndAlgebraicOperations returns true if all these conditions apply:
// 1. Node[objectType#relation].weights[userType] = infinite
// 2. Node[objectType#relation] has only 1 edge, and it's to an OR node
// 3. The OR node has one or more TTU edge with weight infinite for the terminal type and the computed relation for the TTU is the same
// 4. Any other edge coming out of the OR node that has a weight for terminal type, it should be weight 1
// If true, it returns a map of Operands (edges) leaving the OR.
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
		// 1. node[objectType#relation].weights[userType] = infinite
		return false
	}

	edges, ok := t.authzWeightedGraph.GetEdgesFromNode(objRelNode)
	if !ok || len(edges) != 1 {
		// 2. node[objectType#relation] has only 1 edge
		return false
	}

	edge := edges[0]
	if edge.GetTo().GetLabel() != graph.UnionOperator {
		// 2. node[objectType#relation] has 1 edge to an OR node
		return false
	}

	unionNode := edge.GetTo()

	edgesFromUnionNode, ok := t.authzWeightedGraph.GetEdgesFromNode(unionNode)
	if !ok {
		return false
	}

	for _, edge := range edgesFromUnionNode {
		// find and validate the TTUEdge which is infinite (the one being processed at the current time)
		if edge.GetEdgeType() == graph.TTUEdge {
			if w, ok := edge.GetWeight(userType); ok && w == graph.Infinite && edge.GetTo() == objRelNode {
				continue
			}
		}
		// everything else must comply with being weight = 1
		if w, ok := edge.GetWeight(userType); ok && w > 1 {
			return false
		}
	}

	return true
}

/*
func populateUsersetsMap(t *TypeSystem, edgeFromUnionNode *graph.WeightedAuthorizationModelEdge, usersets Operands, relation string) Operands {
	toNode := edgeFromUnionNode.GetTo()
	switch toNode.GetNodeType() {
	case graph.SpecificTypeWildcard, graph.SpecificType:
		// If there are two edges leaving the OR, e.g. one to node `userType` and one to node `userType:*`, only one Operand will be returned
		// because the caller (fastPathDirect) knows how to do the read for both.
		usersets[relation] = This()
	default:
		nodeLabel := toNode.GetLabel()
		objType, relationName := tuple.SplitObjectRelation(nodeLabel)
		getRelation, _ := t.GetRelation(objType, relationName)
		if relationName == "" {
			// if it's a relation such as `define viewer: (a or b) or ...`, (a or b) is an anonymous relation, so set it to "viewer"
			relationName = relation
		}
		usersets[relationName] = getRelation.GetRewrite()
	}
	return usersets
}
*/
