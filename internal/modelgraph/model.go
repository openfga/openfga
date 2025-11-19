package modelgraph

import (
	"errors"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/tuple"
)

var ErrGraphError = errors.New("authorization model graph error")
var ErrModelNotFound = errors.New("authorization model not found")
var ErrInvalidModel = errors.New("invalid authorization model encountered")

type AuthorizationModelGraph struct {
	*authzGraph.WeightedAuthorizationModelGraph
	modelID       string
	schemaVersion string
	conditions    map[string]*condition.EvaluableCondition
}

func New(model *openfgav1.AuthorizationModel) (*AuthorizationModelGraph, error) {
	builder := authzGraph.NewWeightedAuthorizationModelGraphBuilder()
	graph, err := builder.Build(model)
	if err != nil {
		return nil, err
	}
	conditions := make(map[string]*condition.EvaluableCondition, len(model.GetConditions()))
	for name, cond := range model.GetConditions() {
		conditions[name] = condition.NewUncompiled(cond).
			WithTrackEvaluationCost().
			WithMaxEvaluationCost(config.MaxConditionEvaluationCost()).
			WithInterruptCheckFrequency(config.DefaultInterruptCheckFrequency)
	}
	return &AuthorizationModelGraph{
		WeightedAuthorizationModelGraph: graph,
		modelID:                         model.GetId(),
		schemaVersion:                   model.GetSchemaVersion(),
		conditions:                      conditions,
	}, nil
}

func (m *AuthorizationModelGraph) GetModelID() string {
	return m.modelID
}

func (m *AuthorizationModelGraph) GetConditions() map[string]*condition.EvaluableCondition {
	return m.conditions
}

func (m *AuthorizationModelGraph) GetDirectEdgeFromNodeForUserType(objectRelation string, userType string) (*authzGraph.WeightedAuthorizationModelEdge, error) {
	node, ok := m.GetNodeByID(objectRelation)
	if !ok {
		return nil, ErrGraphError
	}
	edges, ok := m.GetEdgesFromNode(node)
	if !ok {
		return nil, ErrGraphError
	}

	for _, e := range edges {
		if e.GetTo().GetLabel() == userType {
			return e, nil
		}
	}

	return nil, ErrGraphError
}

func (m *AuthorizationModelGraph) FlattenNode(node *authzGraph.WeightedAuthorizationModelNode, userType string, recursivePath bool) ([]*authzGraph.WeightedAuthorizationModelEdge, error) {
	edges, ok := m.GetEdgesFromNode(node)
	if !ok {
		return nil, ErrGraphError
	}

	result := make([]*authzGraph.WeightedAuthorizationModelEdge, 0, len(edges))
	for _, edge := range edges {
		_, ok := m.GetEdgeWeight(edge, userType)
		if !ok {
			continue // no relation to terminal type / pruning edge traversal
		}

		canFlatten := false

		switch edge.GetEdgeType() {
		case authzGraph.ComputedEdge, authzGraph.DirectLogicalEdge, authzGraph.TTULogicalEdge:
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
			res, err := m.FlattenNode(edge.GetTo(), userType, recursivePath)
			if err != nil {
				return nil, err
			}
			result = append(result, res...)
		} else if !recursivePath || edge.GetRecursiveRelation() == "" {
			result = append(result, edge)
		}
	}

	return result, nil
}

func (m *AuthorizationModelGraph) canApplyRecursiveOptimization(node *authzGraph.WeightedAuthorizationModelNode, recursiveRelation, userType string) (*authzGraph.WeightedAuthorizationModelEdge, bool) {
	var recursiveEdge *authzGraph.WeightedAuthorizationModelEdge
	edges, ok := m.GetEdgesFromNode(node)
	if !ok {
		return nil, false
	}
	relation := tuple.GetRelation(userType)
	if relation != "" {
		return nil, false
	}

	allEdgesCanApply := true
	for _, edge := range edges {
		if edge.GetRecursiveRelation() != recursiveRelation {
			if w, ok := edge.GetWeight(userType); ok && w > 1 {
				allEdgesCanApply = false
				continue
			}
		} else if edge.GetEdgeType() == authzGraph.DirectEdge || edge.GetEdgeType() == authzGraph.TTUEdge {
			recursiveEdge = edge
		} else {
			edgeResult, canApply := m.canApplyRecursiveOptimization(edge.GetTo(), recursiveRelation, userType)
			if !canApply {
				allEdgesCanApply = false
			}
			if edgeResult != nil {
				recursiveEdge = edgeResult
			}
		}
	}
	return recursiveEdge, allEdgesCanApply
}

func (m *AuthorizationModelGraph) CanApplyRecursion(node *authzGraph.WeightedAuthorizationModelNode, userType string, newstrategy bool) (*authzGraph.WeightedAuthorizationModelEdge, bool) {
	userRelation := tuple.GetRelation(userType)
	// if it is not first time we don't need to resolve any recursive relation because we are already iterating over it
	if userRelation == "" && node.GetRecursiveRelation() == node.GetUniqueLabel() && !node.IsPartOfTupleCycle() {
		edge, ok := m.canApplyRecursiveOptimization(node, node.GetRecursiveRelation(), userType)
		return edge, ok && newstrategy
	}

	return nil, false
}

func WildcardRelationReference(objectType string) *openfgav1.RelationReference {
	return &openfgav1.RelationReference{
		Type: objectType,
		RelationOrWildcard: &openfgav1.RelationReference_Wildcard{
			Wildcard: &openfgav1.Wildcard{},
		},
	}
}
