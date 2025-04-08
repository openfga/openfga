package graph

import (
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/multi"
)

type WeightedAuthorizationModelGraphBuilder struct {
	graph.DirectedMultigraphBuilder
	drawingDirection DrawingDirection
}

func NewWeightedAuthorizationModelGraphBuilder() *WeightedAuthorizationModelGraphBuilder {
	return &WeightedAuthorizationModelGraphBuilder{multi.NewDirectedGraph(), DrawingDirectionCheck}
}

func (wgb *WeightedAuthorizationModelGraphBuilder) Build(model *openfgav1.AuthorizationModel) (*WeightedAuthorizationModelGraph, error) {
	g, err := NewAuthorizationModelGraph(model)
	if err != nil {
		return nil, err
	}
	g, err = g.Reversed() // we want edges to have the direction of Check when doing the weight assignments later
	if err != nil {
		return nil, err
	}

	wb := NewWeightedAuthorizationModelGraph()
	// Add all nodes
	iterNodes := g.Nodes()
	for iterNodes.Next() {
		nextNode := iterNodes.Node()
		node, ok := nextNode.(*AuthorizationModelNode)
		if !ok {
			return nil, fmt.Errorf("%w: could not cast to WeightedAuthorizationModelNode", ErrBuildingGraph)
		}

		wb.AddNode(node.uniqueLabel, node.label, node.nodeType)
	}

	// Add all the edges
	iterEdges := g.Edges()
	for iterEdges.Next() {
		nextEdge, ok := iterEdges.Edge().(multi.Edge)
		if !ok {
			return nil, fmt.Errorf("%w: could not cast %v to multi.Edge", ErrBuildingGraph, iterEdges.Edge())
		}

		// NOTE: because we use a multigraph, one edge can include multiple lines, so we need to add each line individually.
		iterLines := nextEdge.Lines
		for iterLines.Next() {
			nextLine := iterLines.Line()
			castedEdge, ok := nextLine.(*AuthorizationModelEdge)
			if !ok {
				return nil, fmt.Errorf("%w: could not cast %v to AuthorizationModelEdge", ErrBuildingGraph, nextLine)
			}
			castedFromNode, ok := castedEdge.From().(*AuthorizationModelNode)
			if !ok {
				return nil, fmt.Errorf("%w: could not cast %v to AuthorizationModelNode", ErrBuildingGraph, castedEdge.From())
			}
			castedToNode, ok := castedEdge.To().(*AuthorizationModelNode)
			if !ok {
				return nil, fmt.Errorf("%w: could not cast %v to AuthorizationModelNode", ErrBuildingGraph, castedEdge.To())
			}
			wb.AddEdge(castedFromNode.uniqueLabel, castedToNode.uniqueLabel, castedEdge.edgeType, castedEdge.tuplesetRelation, castedEdge.conditions)
		}
	}

	err = wb.AssignWeights()
	if err != nil {
		return nil, err
	}

	return wb, nil
}
