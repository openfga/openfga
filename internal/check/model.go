package check

import (
	"errors"

	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/condition"
)

var ErrGraphError = errors.New("authorization model graph error")

// TODO: Move to its own public package.
type AuthorizationModelGraph struct {
	*authzGraph.WeightedAuthorizationModelGraph
	modelId       string
	schemaVersion string
	conditions    map[string]*condition.EvaluableCondition
}

func (m *AuthorizationModelGraph) GetConditionsEdgeForUserType(objectRelation string, userType string) (*authzGraph.WeightedAuthorizationModelEdge, error) {
	node, ok := m.GetNodeByID(objectRelation)
	if !ok {
		return nil, ErrGraphError
	}
	edges, ok := m.GetEdgesFromNode(node)
	if !ok {
		return nil, ErrGraphError
	}

	var edge *authzGraph.WeightedAuthorizationModelEdge
	for _, e := range edges {
		if e.GetTo().GetLabel() == userType {
			edge = e
			break
		}
	}
	if edges == nil {
		return nil, ErrGraphError
	}

	return edge, nil
}

func (m *AuthorizationModelGraph) FlattenNode(node *authzGraph.WeightedAuthorizationModelNode, userType string) ([]*authzGraph.WeightedAuthorizationModelEdge, error) {
	edges, ok := m.GetEdgesFromNode(node)
	if !ok {
		return nil, ErrGraphError
	}
	result := make([]*authzGraph.WeightedAuthorizationModelEdge, len(edges))
	for _, edge := range edges {
		_, ok := edge.GetWeight(userType)
		if !ok {
			continue // no relation to terminal type / pruning edge traversal
		}

		canFlatten := false

		switch edge.GetEdgeType() {
		case authzGraph.ComputedEdge:
			canFlatten = true
		case authzGraph.RewriteEdge:
			switch edge.GetTo().GetNodeType() {
			case authzGraph.SpecificTypeAndRelation:
				canFlatten = true
			case authzGraph.OperatorNode:
				if edge.GetTo().GetLabel() == authzGraph.UnionOperator {
					canFlatten = true
				}
			}
		}

		if canFlatten {
			res, err := m.FlattenNode(edge.GetTo(), userType)
			if err != nil {
				return nil, err
			}
			result = append(result, res...)
		} else {
			result = append(result, edge)
		}
	}

	return result, nil
}
