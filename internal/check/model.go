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
