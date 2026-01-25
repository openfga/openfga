package pipeline

import (
	weightedGraph "github.com/openfga/language/pkg/go/graph"
)

// createOperatorResolver selects the appropriate resolver for set operation nodes.
// Union uses baseResolver for simple streaming; intersection and exclusion need
// specialized resolvers that collect all inputs before computing results.
func createOperatorResolver(label string, core resolverCore) resolver {
	switch label {
	case weightedGraph.IntersectionOperator:
		return &intersectionResolver{core}
	case weightedGraph.ExclusionOperator:
		return &exclusionResolver{core}
	case weightedGraph.UnionOperator:
		return &baseResolver{core}
	default:
		panic("unsupported operator node for pipeline resolver")
	}
}

// createResolver creates the appropriate resolver implementation for a graph node.
// Most nodes use baseResolver; only set operation nodes require specialized resolvers.
func createResolver(node *Node, core resolverCore) resolver {
	switch node.GetNodeType() {
	case nodeTypeSpecificType,
		nodeTypeSpecificTypeAndRelation,
		nodeTypeSpecificTypeWildcard,
		nodeTypeLogicalDirectGrouping,
		nodeTypeLogicalTTUGrouping:
		return &baseResolver{core}
	case nodeTypeOperator:
		return createOperatorResolver(node.GetLabel(), core)
	default:
		panic("unsupported node type for pipeline resolver")
	}
}

func strToItem(s string) Object {
	return Item{Value: s}
}
