package pipeline

import (
	weightedGraph "github.com/openfga/language/pkg/go/graph"
)

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

func strToItem(s string) Item {
	return Item{Value: s}
}
