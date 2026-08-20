package pipeline

import (
	"slices"

	"github.com/openfga/language/pkg/go/graph"
)

func FlattenTerminalEdges(
	g *graph.WeightedAuthorizationModelGraph,
	edge *graph.WeightedAuthorizationModelEdge,
	target string,
) []*graph.WeightedAuthorizationModelEdge {
	var out []*graph.WeightedAuthorizationModelEdge

	weight, _ := edge.GetWeight(target)
	if weight != 1 {
		return nil
	}

	var stack []*graph.WeightedAuthorizationModelEdge

	if canFlattenEdge(edge) {
		edges, _ := g.GetEdgesFromNode(edge.GetTo())

		stack = make([]*graph.WeightedAuthorizationModelEdge, 0, len(edges))

		for _, e := range slices.Backward(edges) {
			stack = append(stack, e)
		}
	}

	for len(stack) > 0 {
		ndx := len(stack) - 1
		e := stack[ndx]
		stack = stack[:ndx]

		if _, ok := e.GetWeight(target); !ok {
			continue
		}

		toNode := e.GetTo()

		if e.GetEdgeType() == graph.DirectEdge && toNode != nil {
			switch toNode.GetNodeType() {
			case graph.SpecificTypeWildcard:
				out = append(out, e)
				continue
			case graph.SpecificType:
				return nil
			}
		}

		if !canFlattenEdge(e) {
			return nil
		}

		edges, _ := g.GetEdgesFromNode(toNode)

		stack = slices.Grow(stack, len(stack)+len(edges))

		for _, e := range slices.Backward(edges) {
			stack = append(stack, e)
		}
	}
	return out
}

func canFlattenEdge(edge *graph.WeightedAuthorizationModelEdge) bool {
	switch edge.GetEdgeType() {
	case graph.ComputedEdge, graph.DirectLogicalEdge:
		return true
	case graph.RewriteEdge:
		switch edge.GetTo().GetNodeType() {
		case graph.SpecificTypeAndRelation:
			return true
		case graph.OperatorNode:
			if edge.GetTo().GetLabel() == graph.UnionOperator {
				return true
			}
		}
	}
	return false
}
