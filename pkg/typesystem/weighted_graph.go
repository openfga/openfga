package typesystem

import (
	"errors"
	"fmt"
	"slices"

	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/utils"
)

var (
	errNilWeightedGraph = errors.New("weighted graph is nil")
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

// helper function to return 1) all edges from weighted graph for uniqueLabel for sourceType.  2) node for the uniqueLabel.
func (t *TypeSystem) getNodeAndEdgesFromWeightedGraph(
	uniqueLabel string,
	sourceType string,
) ([]*graph.WeightedAuthorizationModelEdge, *graph.WeightedAuthorizationModelNode, error) {
	if t.authzWeightedGraph == nil {
		return nil, nil, errNilWeightedGraph
	}

	wg := t.authzWeightedGraph

	currentNode, ok := wg.GetNodeByID(uniqueLabel)
	if !ok {
		return nil, nil, fmt.Errorf("could not find node with label: %s", uniqueLabel)
	}

	// This means we cannot reach the source type requested, so there are no relevant edges.
	if !hasPathTo(currentNode, sourceType) {
		return nil, nil, nil
	}

	edges, ok := wg.GetEdgesFromNode(currentNode)
	if !ok {
		// This should never happen because it would have been filtered because there is no path.
		return nil, nil, fmt.Errorf("no outgoing edges from node: %s", currentNode.GetUniqueLabel())
	}
	return edges, currentNode, nil
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
func (t *TypeSystem) GetEdgesForIntersection(intersectionUniqueLabel string, sourceType string) (IntersectionEdges, error) {
	edges, _, err := t.getNodeAndEdgesFromWeightedGraph(intersectionUniqueLabel, sourceType)
	if err != nil {
		return IntersectionEdges{}, err
	}
	if len(edges) == 0 {
		return IntersectionEdges{}, fmt.Errorf("no outgoing edges from intersection %s source type %s", intersectionUniqueLabel, sourceType)
	}

	directEdges := make([]*graph.WeightedAuthorizationModelEdge, 0, len(edges))
	var lowestEdge *graph.WeightedAuthorizationModelEdge
	directEdgesAreLowest := false
	lowestWeight := 0

	// It is assumed that direct edge always appear first.
	for edgeNum, edge := range edges {
		if hasPathTo(edge, sourceType) {
			if edge.GetEdgeType() == graph.DirectEdge {
				// The first step is to establish the largest weight for all the direct edges as they need to be
				// treated as a group. This weight will serve as the baseline for other siblings to compare against.
				directEdges = append(directEdges, edge)
				if weight, ok := edge.GetWeight(sourceType); ok && weight > lowestWeight {
					directEdgesAreLowest = true
					lowestWeight = weight
				}
			} else { // non-direct edges
				if lowestWeight == 0 && edgeNum != 0 {
					// this means that all the direct edges are not connected.
					// In reality, should not happen because the caller should have trimmed
					// the parent node already.
					return IntersectionEdges{}, nil
				}
				if weight, ok := edge.GetWeight(sourceType); ok {
					// in the case of the first edge, we need to initialize as the least weight
					// for other edges to compare against it.
					// For non-first edges, only replace if it has the lowest weight.
					if weight < lowestWeight || edgeNum == 0 {
						lowestEdge = edge
						lowestWeight = weight
						directEdgesAreLowest = false
					}
				}
			}
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
			if hasPathTo(edge, sourceType) {
				siblings = append(siblings, edge)
			} else {
				// In reality, should never happen because the edge should have been trimmed
				return IntersectionEdges{}, nil
			}
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
func (t *TypeSystem) GetEdgesForExclusion(
	exclusionUniqueLabel string,
	sourceType string,
) ([]*graph.WeightedAuthorizationModelEdge, *graph.WeightedAuthorizationModelEdge, error) {
	edges, _, err := t.getNodeAndEdgesFromWeightedGraph(exclusionUniqueLabel, sourceType)
	if err != nil {
		return nil, nil, err
	}
	if len(edges) < 2 {
		return nil, nil, fmt.Errorf("invalid edges from exclusion %s source type %s", exclusionUniqueLabel, sourceType)
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
