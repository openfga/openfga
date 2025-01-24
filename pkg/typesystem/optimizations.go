package typesystem

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/pkg/tuple"
)

// This file contains methods to detect whether an authorization model exhibits certain characteristics.
// This information can then be used to increase performance of a Check request.

// Operands is a map of relation names to their rewrites.
type Operands map[string]*openfgav1.Userset

// IsRelationWithRecursiveTTUAndAlgebraicOperations returns true if all these conditions apply:
// 1. Node[objectType#relation].weights[userType] = infinite
// 2. Node[objectType#relation] has only 1 edge, and it's to an OR node
// 3. The OR node has one TTU edge with weight infinite for the terminal type and the computed relation for the TTU is the same
// 4. Any other edge leaving the OR node has weight 1 for the terminal type.
// If true, it returns a map of Operands (edges) leaving the OR.
func (t *TypeSystem) IsRelationWithRecursiveTTUAndAlgebraicOperations(objectType, relation, userType string) (Operands, bool) {
	usersets := make(Operands)
	if t.authzWeightedGraph == nil {
		return usersets, false
	}
	objRel := tuple.ToObjectRelationString(objectType, relation)
	objRelNode, ok := t.authzWeightedGraph.GetNodeByID(objRel)
	if !ok {
		return usersets, false
	}

	w, ok := objRelNode.GetWeight(userType)
	if !ok || w != graph.Infinite {
		// 1. node[objectType#relation].weights[userType] = infinite
		return usersets, false
	}

	edges, ok := t.authzWeightedGraph.GetEdgesByNode(objRelNode)
	if !ok || len(edges) != 1 {
		// 2. node[objectType#relation] has only 1 edge
		return usersets, false
	}

	edge := edges[0]
	if edge.GetTo().GetLabel() != graph.UnionOperator {
		// 2. node[objectType#relation] has 1 edge to an OR node
		return usersets, false
	}

	unionNode := edge.GetTo()

	edgesFromUnionNode, ok := t.authzWeightedGraph.GetEdgesByNode(unionNode)
	if !ok {
		return usersets, false
	}

	ttuEdgeSatisfiesCond, ttuEdgeCount := false, 0

	for _, edgeFromUnionNode := range edgesFromUnionNode {
		if edgeFromUnionNode.GetEdgeType() == graph.TTUEdge {
			ttuEdgeCount++
			if ttuEdgeCount > 1 {
				// 3. The OR node has one TTU edge
				return nil, false
			}
			ttuEdge := edgeFromUnionNode
			w, ok := ttuEdge.GetWeight(userType)
			if ok && w == graph.Infinite && ttuEdge.GetTo() == objRelNode {
				// 3. The OR node has one TTU edge with weight infinite for the terminal type and the computed relation for the TTU is the same
				ttuEdgeSatisfiesCond = true
			}
		} else {
			w, ok := edgeFromUnionNode.GetWeight(userType)
			if !ok || w != 1 {
				// 4. Any other edge leaving the OR node has weight 1 for the terminal type.
				return nil, false
			}
			usersets = populateUsersetsMap(t, edgeFromUnionNode, usersets, relation)
		}
	}

	satisfies := ttuEdgeSatisfiesCond && len(usersets) >= 1
	return usersets, satisfies
}

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
