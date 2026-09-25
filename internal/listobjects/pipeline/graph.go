package pipeline

import (
	"slices"

	"github.com/openfga/language/pkg/go/graph"
)

func FlattenWildcardEdges(
	g *graph.WeightedAuthorizationModelGraph,
	edge *graph.WeightedAuthorizationModelEdge,
	target string,
) []*graph.WeightedAuthorizationModelEdge {
	var out []*graph.WeightedAuthorizationModelEdge

	stack := []*graph.WeightedAuthorizationModelEdge{edge}

	for len(stack) > 0 {
		ndx := len(stack) - 1
		e := stack[ndx]
		stack = stack[:ndx]

		// all edges must have a weight of one, or this optimization
		// cannot be applied.
		weight, _ := e.GetWeight(target)
		if weight != 1 {
			return nil
		}

		if !slices.Contains(e.GetWildcards(), target) {
			return nil
		}

		node := e.GetTo()

		var canFlatten bool

		switch e.GetEdgeType() {
		case graph.ComputedEdge, graph.DirectLogicalEdge:
			canFlatten = true
		case graph.RewriteEdge:
			switch node.GetNodeType() {
			case graph.SpecificTypeAndRelation:
				canFlatten = true
			case graph.OperatorNode:
				if node.GetLabel() == graph.UnionOperator {
					canFlatten = true
				}
			}
		case graph.DirectEdge:
			switch node.GetNodeType() {
			case graph.SpecificTypeWildcard:
				// this optimization specifically gathers all direct wildcards,
				// since we found one, append it to the output.
				out = append(out, e)

				// nothing more to see here; move onto next edge.
				continue
			case graph.SpecificType:
				// the path includes non-wildcard terminal nodes; this optimization
				// cannot be applied.
				return nil
			}
		}

		if !canFlatten {
			// if the edge cannot be flattened, and it is not a direct edge
			// then the optimization cannot be applied.
			return nil
		}

		// continue flattening this branch.
		edges, _ := g.GetEdgesFromNode(node)

		// avoid unnecessary allocations
		stack = slices.Grow(stack, len(stack)+len(edges))

		// append the edges in reverse order to preserve order during DFS
		// traversal. original sort order of the edges array is preserved.
		for _, e := range slices.Backward(edges) {
			stack = append(stack, e)
		}
	}
	return out
}
