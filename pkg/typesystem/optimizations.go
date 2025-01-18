package typesystem

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/pkg/tuple"
)

// This file contains methods to detect whether an authorization model exhibits certain characteristics.
// This information can then be used to increase performance of a Check request.

type Operand struct {
	Rewrite      *openfgav1.Userset
	RelationName string
}

// IsRelationWithRecursiveTTUAndAlgebraicOperations returns true if all these conditions apply:
// - node[objectType#relation].weights[userType] = infinite
// - node[objectType#relation] has only 1 edge, and it's to an OR node
// - The OR node has one TTU edge with weight infinite for the terminal type and the computed relation for the TTU is the same
// - Any other edge leaving the OR node has weight 1 for the terminal type.
// If true, it returns an array containing all the other operands leaving the OR.
func (t *TypeSystem) IsRelationWithRecursiveTTUAndAlgebraicOperations(objectType, relation, userType string) (bool, []Operand) {
	usersets := make([]Operand, 0)
	if t.authzWeightedGraph == nil {
		return false, usersets
	}
	objRel := tuple.ToObjectRelationString(objectType, relation)
	objRelNode, ok := t.authzWeightedGraph.GetNodeByID(objRel)
	if !ok {
		return false, usersets
	}

	w, ok := objRelNode.GetWeight(userType)
	if !ok || w != graph.Infinite {
		return false, usersets
	}

	edges, ok := t.authzWeightedGraph.GetEdgesByNode(objRelNode)
	if !ok || len(edges) != 1 {
		return false, usersets
	}

	edge := edges[0]
	if edge.GetTo().GetLabel() != graph.UnionOperator {
		return false, usersets
	}

	unionNode := edge.GetTo()

	edgesFromUnionNode, ok := t.authzWeightedGraph.GetEdgesByNode(unionNode)
	if !ok {
		return false, usersets
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
			if ok && w == 1 {
				nodeLabel := edgeFromUnionNode.GetTo().GetLabel()
				objType, relationName := tuple.SplitObjectRelation(nodeLabel)
				if relationName != "" {
					getRelation, _ := t.GetRelation(objType, relationName)
					usersets = append(usersets, Operand{
						Rewrite:      getRelation.GetRewrite(),
						RelationName: relationName,
					})
				} else {
					usersets = append(usersets, Operand{
						Rewrite:      This(),
						RelationName: relationName,
					})
				}
			}
			if !ok || w != 1 {
				restOfEdgesAreWeight1 = false
				break
			}
		}
	}

	satisfies := ttuEdgeSatisfiesCond && restOfEdgesAreWeight1
	if !satisfies {
		usersets = nil
	}
	return satisfies, usersets
}
