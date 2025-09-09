package typesystem

import (
	"fmt"
	"math"

	"github.com/google/uuid"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/pkg/tuple"
)

type weightedGraphItem interface {
	GetWeight(destinationType string) (int, bool)
}

// hasPathTo returns a boolean indicating if a path exists from a node or edge to a terminal type. E.g
// can we reach "user" from "document".
func hasPathTo(nodeOrEdge weightedGraphItem, destinationType string) bool {
	_, ok := nodeOrEdge.GetWeight(destinationType)
	return ok
}

type IntersectionEdges struct {
	LowestEdges  []*graph.WeightedAuthorizationModelEdge   // lowest edges to apply list objects
	SiblingEdges [][]*graph.WeightedAuthorizationModelEdge // the rest of the edges to apply intersection
}

type ExclusionEdges struct {
	BaseEdges     []*graph.WeightedAuthorizationModelEdge // base edges to apply list objects
	ExcludedEdges []*graph.WeightedAuthorizationModelEdge // excluded edges to apply exclusion
}

// GetEdgesForIntersection returns the lowest weighted edge and
// its siblings edges for intersection based via the weighted graph.
// If the direct edges have equal weight as its sibling edges, it will choose
// the direct edges as preference.
// If any of the children are not connected, it will return empty IntersectionEdges.
func GetEdgesForIntersection(edges []*graph.WeightedAuthorizationModelEdge, sourceType string) (IntersectionEdges, error) {
	if len(edges) < 2 {
		// Intersection by definition must have at least 2 children
		return IntersectionEdges{}, fmt.Errorf("invalid edges for source type %s", sourceType)
	}

	// Group edges by type
	groupedEdges := make(map[string][]*graph.WeightedAuthorizationModelEdge, len(edges))
	const directEdgesKey = "direct_edges"

	for _, edge := range edges {
		_, ok := edge.GetWeight(sourceType)
		if !ok {
			// Skip edges that don't have a path to the source type
			continue
		}
		var key string
		switch edge.GetEdgeType() {
		case graph.DirectEdge:
			key = directEdgesKey
		case graph.TTUEdge:
			objtype, parent := tuple.SplitObjectRelation(edge.GetTuplesetRelation())
			_, relation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
			key = objtype + "#" + parent + "#" + relation
		default:
			// Other edge types are treated individually
			key = "other_" + uuid.New().String()
		}

		groupedEdges[key] = append(groupedEdges[key], edge)
	}

	// Find the group with the lowest maximum weight
	lowestWeight := math.MaxInt32
	lowestKey := ""
	intersectionEdges := make([][]*graph.WeightedAuthorizationModelEdge, 0, len(edges))

	for key, group := range groupedEdges {
		maxWeight := 0
		for _, edge := range group {
			weight, _ := edge.GetWeight(sourceType)
			// get the max weight of the grouping
			if weight > maxWeight {
				maxWeight = weight
			}
		}
		// if the max weight of the grouping is bigger than 0 and it is lower than the lowest weight found so far
		// take the lowest weight or in the case of direct edges have more priority if there are others with the same weight
		if maxWeight < lowestWeight || (key == directEdgesKey && maxWeight == lowestWeight) {
			if lowestKey != "" {
				// verify the intersection edges
				intersectionEdges = append(intersectionEdges, groupedEdges[lowestKey])
			}
			lowestWeight = maxWeight
			lowestKey = key
		} else {
			intersectionEdges = append(intersectionEdges, group)
		}
	}

	return IntersectionEdges{
		LowestEdges:  groupedEdges[lowestKey],
		SiblingEdges: intersectionEdges,
	}, nil
}

// GetEdgesForExclusion returns the base edges (i.e., edge A in "A but not B") and
// excluded edge (edge B in "A but not B") based on weighted graph for exclusion.
func GetEdgesForExclusion(
	edges []*graph.WeightedAuthorizationModelEdge,
	sourceType string,
) (ExclusionEdges, error) {
	if len(edges) < 2 {
		return ExclusionEdges{}, fmt.Errorf("invalid exclusion edges for source type %s", sourceType)
	}

	// Group edges by type
	groupedEdges := make(map[string][]*graph.WeightedAuthorizationModelEdge, 2)
	baseKey := ""
	exclusionKey := ""
	const directEdgesKey = "direct_edges"

	for _, edge := range edges {
		var key string
		_, ok := edge.GetWeight(sourceType)
		if !ok {
			// Skip edges that don't have a path to the source type
			continue
		}

		switch edge.GetEdgeType() {
		case graph.DirectEdge:
			key = directEdgesKey
		case graph.TTUEdge:
			objtype, parent := tuple.SplitObjectRelation(edge.GetTuplesetRelation())
			_, relation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
			key = objtype + "#" + parent + "#" + relation
		default:
			// Other edge types are treated individually
			key = "other_" + uuid.New().String()
		}
		if len(baseKey) == 0 {
			baseKey = key
		} else if baseKey != key {
			exclusionKey = key
		}
		groupedEdges[key] = append(groupedEdges[key], edge)
	}

	if len(groupedEdges) > 2 || len(baseKey) == 0 {
		return ExclusionEdges{}, fmt.Errorf("invalid exclusion edges for source type %s", sourceType)
	}

	// for the case of exclusion, we can have that the exlusion part does not have any edge that lead to
	// the user type in question, so no need to run check over it, as it will never negate it the results of the base
	if exclusionKey == "" {
		return ExclusionEdges{
			BaseEdges:     groupedEdges[baseKey],
			ExcludedEdges: nil,
		}, nil
	}

	return ExclusionEdges{
		BaseEdges:     groupedEdges[baseKey],
		ExcludedEdges: groupedEdges[exclusionKey],
	}, nil
}

// ConstructUserset returns the openfgav1.Userset to run CheckRewrite against list objects candidate when
// model has intersection / exclusion.
func (t *TypeSystem) ConstructUserset(currentEdge *graph.WeightedAuthorizationModelEdge) (*openfgav1.Userset, error) {
	currentNode := currentEdge.GetTo()
	edgeType := currentEdge.GetEdgeType()
	uniqueLabel := currentNode.GetUniqueLabel()

	switch currentNode.GetNodeType() {
	case graph.SpecificType, graph.SpecificTypeWildcard:
		return This(), nil
	case graph.SpecificTypeAndRelation:
		switch edgeType {
		case graph.DirectEdge:
			// userset use case
			return This(), nil
		case graph.RewriteEdge, graph.ComputedEdge:
			_, relation := tuple.SplitObjectRelation(uniqueLabel)
			return &openfgav1.Userset{
				Userset: &openfgav1.Userset_ComputedUserset{
					ComputedUserset: &openfgav1.ObjectRelation{
						Relation: relation,
					},
				},
			}, nil
		case graph.TTUEdge:
			_, parent := tuple.SplitObjectRelation(currentEdge.GetTuplesetRelation())
			_, relation := tuple.SplitObjectRelation(uniqueLabel)
			return &openfgav1.Userset{
				Userset: &openfgav1.Userset_TupleToUserset{
					TupleToUserset: &openfgav1.TupleToUserset{
						Tupleset: &openfgav1.ObjectRelation{
							Relation: parent, // parent
						},
						ComputedUserset: &openfgav1.ObjectRelation{
							Relation: relation,
						},
					},
				},
			}, nil
		default:
			// This should never happen.
			return nil, fmt.Errorf("unknown edge type: %v for node: %s", edgeType, currentNode.GetUniqueLabel())
		}

	case graph.OperatorNode:
		edges, ok := t.authzWeightedGraph.GetEdgesFromNode(currentNode)
		if !ok {
			// This should never happen.
			return nil, fmt.Errorf("no outgoing edges from node: %s", currentNode.GetUniqueLabel())
		}
		var usersets []*openfgav1.Userset
		for _, edge := range edges {
			currentUserset, err := t.ConstructUserset(edge)
			if err != nil {
				return nil, fmt.Errorf("failed to construct userset for edge %s: %w", edge.GetTo().GetUniqueLabel(), err)
			}
			usersets = append(usersets, currentUserset)
		}
		switch currentNode.GetLabel() {
		case graph.ExclusionOperator:
			if len(usersets) != 2 {
				// This should never happen.
				return nil, fmt.Errorf("node %s: exclusion operator requires exactly two usersets, got %d",
					currentNode.GetUniqueLabel(), len(usersets))
			}
			return &openfgav1.Userset{
				Userset: &openfgav1.Userset_Difference{
					Difference: &openfgav1.Difference{
						Base:     usersets[0],
						Subtract: usersets[1],
					}}}, nil
		case graph.IntersectionOperator:
			return &openfgav1.Userset{
				Userset: &openfgav1.Userset_Intersection{
					Intersection: &openfgav1.Usersets{
						Child: usersets,
					}}}, nil
		case graph.UnionOperator:
			return &openfgav1.Userset{
				Userset: &openfgav1.Userset_Union{
					Union: &openfgav1.Usersets{
						Child: usersets,
					}}}, nil
		default:
			// This should never happen.
			return nil, fmt.Errorf("unknown operator node label %s for node %s", currentNode.GetLabel(), currentNode.GetUniqueLabel())
		}
	default:
		// This should never happen.
		return nil, fmt.Errorf("unknown node type %v for node %s", currentNode.GetNodeType(), currentNode.GetUniqueLabel())
	}
}
