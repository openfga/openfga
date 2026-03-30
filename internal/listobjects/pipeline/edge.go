package pipeline

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"

	"github.com/openfga/openfga/internal/listobjects/pipeline/internal/worker"
	"github.com/openfga/openfga/internal/seq"
)

// directEdgeHandler queries objects via the edge's relation definition.
type directEdgeHandler struct {
	reader ObjectReader
}

// Handle queries storage for objects related to the given items through
// the edge's relation definition.
func (h *directEdgeHandler) Handle(
	ctx context.Context,
	edge *Edge,
	objects []string,
) iter.Seq[worker.Item] {
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

	var results iter.Seq[worker.Item]

	if len(objects) > 0 {
		input := ObjectQuery{
			ObjectType: nodeType,
			Relation:   nodeRelation,
			Users:      objects,
			Conditions: edge.GetConditions(),
		}
		results = h.reader.Read(ctx, input)
	} else {
		results = emptySequence
	}

	return results
}

// ttuEdgeHandler queries objects via the edge's tupleset relation.
type ttuEdgeHandler struct {
	reader ObjectReader
	graph  *Graph
}

// Handle resolves the tupleset relation for the edge, then queries storage
// for objects related to the given items through the tupleset's target type.
func (h *ttuEdgeHandler) Handle(
	ctx context.Context,
	edge *Edge,
	objects []string,
) iter.Seq[worker.Item] {
	tuplesetType, tuplesetRelation, ok := strings.Cut(edge.GetTuplesetRelation(), "#")
	if !ok {
		return seq.Sequence(worker.Item{Err: errors.New("invalid tupleset relation")})
	}

	tuplesetNode, ok := h.graph.GetNodeByID(edge.GetTuplesetRelation())
	if !ok {
		return seq.Sequence(worker.Item{Err: errors.New("tupleset node not in graph")})
	}

	edges, ok := h.graph.GetEdgesFromNode(tuplesetNode)
	if !ok {
		return seq.Sequence(worker.Item{Err: errors.New("no edges found for tupleset node")})
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
		return seq.Sequence(worker.Item{Err: errors.New("ttu target type is not an edge of tupleset")})
	}

	var results iter.Seq[worker.Item]

	if len(objects) > 0 {
		input := ObjectQuery{
			ObjectType: tuplesetType,
			Relation:   tuplesetRelation,
			Users:      objects,
			Conditions: targetEdge.GetConditions(),
		}
		results = h.reader.Read(ctx, input)
	} else {
		results = emptySequence
	}

	return results
}

// identityEdgeHandler passes items through without querying storage.
type identityEdgeHandler struct{}

// Handle returns each input string as a successful Item without querying storage.
func (h *identityEdgeHandler) Handle(
	_ context.Context,
	_ *Edge,
	items []string,
) iter.Seq[worker.Item] {
	return func(yield func(worker.Item) bool) {
		for _, s := range items {
			if !yield(worker.Item{Value: s}) {
				return
			}
		}
	}
}

// edgeInterpreter dispatches to the appropriate handler based on edge type.
type edgeInterpreter struct {
	direct   *directEdgeHandler
	ttu      *ttuEdgeHandler
	identity *identityEdgeHandler
}

// Interpret dispatches to the handler matching edge's type. A nil edge
// is treated as an identity pass-through.
func (e *edgeInterpreter) Interpret(
	ctx context.Context,
	edge *Edge,
	items []string,
) iter.Seq[worker.Item] {
	if len(items) == 0 {
		return emptySequence
	}
	if edge == nil {
		return e.identity.Handle(ctx, edge, items)
	}
	switch edge.GetEdgeType() {
	case edgeTypeDirect:
		return e.direct.Handle(ctx, edge, items)
	case edgeTypeTTU:
		return e.ttu.Handle(ctx, edge, items)
	case edgeTypeComputed, edgeTypeRewrite, edgeTypeDirectLogical, edgeTypeTTULogical:
		return e.identity.Handle(ctx, edge, items)
	default:
		return seq.Sequence(worker.Item{Err: fmt.Errorf(
			"no handler for edge type: %v",
			edge.GetEdgeType(),
		)})
	}
}
