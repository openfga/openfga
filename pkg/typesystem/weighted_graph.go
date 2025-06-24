package typesystem

import (
	"fmt"
	"slices"

	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/utils"
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
	LowestEdge *graph.WeightedAuthorizationModelEdge   // nil if direct is lowest, otherwise lowest edge
	Siblings   []*graph.WeightedAuthorizationModelEdge // all non lowest and excluding direct edges siblings
	// We need to separate out the direct edges from the other edges so that in the case of direct edges are not the lowest weight,
	// the check against these direct edges do not require checking against these individual directly assigned edges.
	// Instead, these directly assigned edges should be treated as a group such that if one of these edges satisfy,
	// the intersection satisfy.
	DirectEdges               []*graph.WeightedAuthorizationModelEdge
	DirectEdgesAreLeastWeight bool // whether direct edges are the lowest weight edges
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

	directEdges := make([]*graph.WeightedAuthorizationModelEdge, 0, len(edges))
	var lowestEdge *graph.WeightedAuthorizationModelEdge
	directEdgesAreLowest := false
	lowestWeight := 0

	// Process all edges first to separate direct from non-direct
	nonDirectEdges := make([]*graph.WeightedAuthorizationModelEdge, 0, len(edges))

	// It is assumed that direct edge always appear first.
	for _, edge := range edges {
		weight, ok := edge.GetWeight(sourceType)
		if !ok {
			continue
		}
		if edge.GetEdgeType() == graph.DirectEdge {
			// The first step is to establish the largest weight for all the direct edges as they need to be
			// treated as a group. This weight will serve as the baseline for other siblings to compare against.
			directEdges = append(directEdges, edge)
			if weight > lowestWeight {
				directEdgesAreLowest = true
				lowestWeight = weight
			}
		} else {
			nonDirectEdges = append(nonDirectEdges, edge)
		}
	}

	if lowestWeight == 0 {
		if edges[0].GetEdgeType() == graph.DirectEdge {
			// this means that all the direct edges are not connected.
			// In reality, should not happen because the caller should have trimmed
			// the parent node already.
			return IntersectionEdges{}, nil
		}
	}
	for edgeNum, edge := range nonDirectEdges {
		if weight, _ := edge.GetWeight(sourceType); weight < lowestWeight || (edgeNum == 0 && len(directEdges) == 0) {
			// in the case of the first edge, we need to initialize as the least weight
			// for other edges to compare against it.
			// For non-first edges, only replace if it has the lowest weight.
			lowestEdge = edge
			lowestWeight = weight
			directEdgesAreLowest = false
		}
	}

	siblings := make([]*graph.WeightedAuthorizationModelEdge, 0, len(edges)-len(directEdges))

	// Now, assign all the non directly assigned edges that are not the lowest
	// weight to the sibling edges. Even if the directly assigned edges are not the lowest
	// weight, we want to treat these directly assigned edges separately. The reason is that
	// if one of these directly assigned edge satisfy, the list objects candidate may still
	// be valid.
	for _, edge := range edges {
		if edge.GetEdgeType() != graph.DirectEdge && edge != lowestEdge {
			if !hasPathTo(edge, sourceType) {
				// In reality, should never happen because the edge should have been trimmed
				return IntersectionEdges{}, nil
			}
			siblings = append(siblings, edge)
		}
	}

	return IntersectionEdges{
		LowestEdge:                lowestEdge,
		Siblings:                  siblings,
		DirectEdges:               directEdges,
		DirectEdgesAreLeastWeight: directEdgesAreLowest,
	}, nil
}

// GetEdgesForExclusion returns the base edges (i.e., edge A in "A but not B") and
// excluded edge (edge B in "A but not B") based on weighted graph for exclusion.
func GetEdgesForExclusion(
	edges []*graph.WeightedAuthorizationModelEdge,
	sourceType string,
) ([]*graph.WeightedAuthorizationModelEdge, *graph.WeightedAuthorizationModelEdge, error) {
	if len(edges) < 2 {
		return nil, nil, fmt.Errorf("invalid exclusion edges for source type %s", sourceType)
	}

	butNotEdge := edges[len(edges)-1] // this is the edge to 'b'
	// Filter to only return edges which have a path to the sourceType
	relevantEdges := slices.Collect(utils.Filter(edges[:len(edges)-1], func(edge *graph.WeightedAuthorizationModelEdge) bool {
		return hasPathTo(edge, sourceType)
	}))
	if !hasPathTo(butNotEdge, sourceType) {
		// if the but not path is not connected, there is no butNotEdge because
		// all list objects candidates are true.
		return relevantEdges, nil, nil
	}
	return relevantEdges, butNotEdge, nil
}
