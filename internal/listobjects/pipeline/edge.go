package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/openfga/openfga/internal/listobjects/pipeline/internal/worker"
)

// edgeInterpreter dispatches to the appropriate handler based on edge type.
type edgeInterpreter struct {
	store ObjectStore
	graph *Graph
}

func (e *edgeInterpreter) identity(objects []string) Receiver[Item] {
	return worker.MapReceiver(worker.NewSliceReceiver(objects), func(s string) Item {
		return Item{Value: s}
	})
}

func (e *edgeInterpreter) direct(ctx context.Context, edge *Edge, objects []string) Receiver[Item] {
	nodeType, nodeRelation, _ := strings.Cut(edge.GetRelationDefinition(), "#")

	_, userRelation, exists := strings.Cut(edge.GetTo().GetLabel(), "#")

	if exists {
		mutated := make([]string, len(objects))
		for i, obj := range objects {
			obj += "#" + userRelation
			mutated[i] = obj
		}
		objects = mutated
	}

	var results Receiver[Item]

	if len(objects) > 0 {
		input := ObjectQuery{
			ObjectType: nodeType,
			Relation:   nodeRelation,
			Users:      objects,
			Conditions: edge.GetConditions(),
		}
		results = e.store.Read(ctx, input)
	} else {
		results = emptyReceiver
	}

	return results
}

func (e *edgeInterpreter) ttu(ctx context.Context, edge *Edge, objects []string) Receiver[Item] {
	tuplesetType, tuplesetRelation, ok := strings.Cut(edge.GetTuplesetRelation(), "#")
	if !ok {
		return worker.NewValueReceiver(Item{Err: errors.New("invalid tupleset relation")})
	}

	tuplesetNode, ok := e.graph.GetNodeByID(edge.GetTuplesetRelation())
	if !ok {
		return worker.NewValueReceiver(Item{Err: errors.New("tupleset node not in graph")})
	}

	edges, ok := e.graph.GetEdgesFromNode(tuplesetNode)
	if !ok {
		return worker.NewValueReceiver(Item{Err: errors.New("no edges found for tupleset node")})
	}

	targetType, _, _ := strings.Cut(edge.GetTo().GetLabel(), "#")

	var targetEdge *Edge

	for _, e := range edges {
		if e.GetTo().GetLabel() == targetType {
			targetEdge = e
			break
		}
	}

	if targetEdge == nil {
		return worker.NewValueReceiver(Item{Err: errors.New("ttu target type is not an edge of tupleset")})
	}

	var results Receiver[Item]

	if len(objects) > 0 {
		input := ObjectQuery{
			ObjectType: tuplesetType,
			Relation:   tuplesetRelation,
			Users:      objects,
			Conditions: targetEdge.GetConditions(),
		}
		results = e.store.Read(ctx, input)
	} else {
		results = emptyReceiver
	}

	return results
}

func (e *edgeInterpreter) Interpret(
	ctx context.Context,
	edge *Edge,
	items []string,
) Receiver[Item] {
	if len(items) == 0 {
		return emptyReceiver
	}
	if edge == nil {
		return e.identity(items)
	}
	switch edge.GetEdgeType() {
	case edgeTypeDirect:
		return e.direct(ctx, edge, items)
	case edgeTypeTTU:
		return e.ttu(ctx, edge, items)
	case edgeTypeComputed, edgeTypeRewrite, edgeTypeDirectLogical, edgeTypeTTULogical:
		return e.identity(items)
	default:
		return worker.NewValueReceiver(Item{Err: fmt.Errorf(
			"no handler for edge type: %v",
			edge.GetEdgeType(),
		)})
	}
}
