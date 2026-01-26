package pipeline

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"

	"github.com/openfga/openfga/internal/seq"
)

type interpreter interface {
	Interpret(ctx context.Context, edge *Edge, items []string) iter.Seq[Item]
}

// directEdgeHandler queries objects via the edge's relation definition.
type directEdgeHandler struct {
	reader ObjectReader
}

func (h *directEdgeHandler) Handle(
	ctx context.Context,
	edge *Edge,
	objects []string,
) iter.Seq[Item] {
	nodeType, nodeRelation, _ := strings.Cut(edge.GetRelationDefinition(), "#")

	_, userRelation, exists := strings.Cut(edge.GetTo().GetLabel(), "#")

	userFilter := make([]string, len(objects))

	for i, obj := range objects {
		objectRelation := obj
		if exists {
			objectRelation += "#" + userRelation
		}
		userFilter[i] = objectRelation
	}

	var results iter.Seq[Item]

	if len(userFilter) > 0 {
		input := ObjectQuery{
			ObjectType: nodeType,
			Relation:   nodeRelation,
			Users:      userFilter,
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

func (h *ttuEdgeHandler) Handle(
	ctx context.Context,
	edge *Edge,
	objects []string,
) iter.Seq[Item] {
	tuplesetType, tuplesetRelation, ok := strings.Cut(edge.GetTuplesetRelation(), "#")
	if !ok {
		return seq.Sequence(Item{Err: errors.New("invalid tupleset relation")})
	}

	tuplesetNode, ok := h.graph.GetNodeByID(edge.GetTuplesetRelation())
	if !ok {
		return seq.Sequence(Item{Err: errors.New("tupleset node not in graph")})
	}

	edges, ok := h.graph.GetEdgesFromNode(tuplesetNode)
	if !ok {
		return seq.Sequence(Item{Err: errors.New("no edges found for tupleset node")})
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
		return seq.Sequence(Item{Err: errors.New("ttu target type is not an edge of tupleset")})
	}

	var results iter.Seq[Item]

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

func (h *identityEdgeHandler) Handle(
	_ context.Context,
	_ *Edge,
	items []string,
) iter.Seq[Item] {
	return func(yield func(Item) bool) {
		for _, s := range items {
			if !yield(Item{Value: s}) {
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

func (e *edgeInterpreter) Interpret(
	ctx context.Context,
	edge *Edge,
	items []string,
) iter.Seq[Item] {
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
		return seq.Sequence(Item{Err: fmt.Errorf(
			"no handler for edge type: %v",
			edge.GetEdgeType(),
		)})
	}
}
