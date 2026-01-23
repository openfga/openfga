package pipeline

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/seq"
)

// interpreter is an interface that exposes a method for interpreting input for an edge into output.
type interpreter interface {
	Interpret(ctx context.Context, edge *Edge, items []string) iter.Seq[Item]
}

// directEdgeHandler is a struct that handles edges having a direct type. These edges will
// always require a query for objects using the edge's relation definition -- the downstream
// type and relation.
type directEdgeHandler struct {
	queryEngine *queryEngine
}

// Handle is a function that queries for objects using the relation definition of edge.
func (h *directEdgeHandler) Handle(
	ctx context.Context,
	edge *Edge,
	items []string,
) iter.Seq[Item] {
	nodeType, nodeRelation, _ := strings.Cut(edge.GetRelationDefinition(), "#")

	_, userRelation, _ := strings.Cut(edge.GetTo().GetLabel(), "#")

	userFilter := make([]*openfgav1.ObjectRelation, len(items))

	for i, item := range items {
		userFilter[i] = &openfgav1.ObjectRelation{
			Object:   item,
			Relation: userRelation,
		}
	}

	var results iter.Seq[Item]

	if len(userFilter) > 0 {
		input := queryInput{
			objectType:     nodeType,
			objectRelation: nodeRelation,
			userFilter:     userFilter,
			conditions:     edge.GetConditions(),
		}
		results = h.queryEngine.Execute(ctx, input)
	} else {
		results = emptySequence
	}

	return results
}

// ttuEdgeHandler is a struct that handles edges having a TTU type. These edges will always
// require a query for objects using the edge's tulpeset relation.
type ttuEdgeHandler struct {
	queryEngine *queryEngine
	graph       *Graph
}

// Handle is a function that queries for objects given the tupleset relation of edge.
func (h *ttuEdgeHandler) Handle(
	ctx context.Context,
	edge *Edge,
	items []string,
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

	userFilter := make([]*openfgav1.ObjectRelation, len(items))

	for i, item := range items {
		userFilter[i] = &openfgav1.ObjectRelation{
			Object:   item,
			Relation: "",
		}
	}

	var results iter.Seq[Item]

	if len(userFilter) > 0 {
		input := queryInput{
			objectType:     tuplesetType,
			objectRelation: tuplesetRelation,
			userFilter:     userFilter,
			conditions:     targetEdge.GetConditions(),
		}
		results = h.queryEngine.Execute(ctx, input)
	} else {
		results = emptySequence
	}

	return results
}

// identityEdgeHandler is a struct that handles items for edge types that do not
// require additional processing.
type identityEdgeHandler struct{}

// Handle is a function that returns a sequence of the provided items as Item values.
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

// edgeInterpreter is a struct that holds all edge handlers, and acts as a proxy
// interpreter for each handler.
type edgeInterpreter struct {
	// direct contains an edge handler that specifically handles direct edge types.
	direct *directEdgeHandler

	// ttu contains an edge handler that specifically handles ttu edge types.
	ttu *ttuEdgeHandler

	// identity contains an edge handler that handles nil edge values, computed,
	// rewrite, direct logical, and ttu logical edge types.
	identity *identityEdgeHandler
}

// Interpret is a function that selects the appropriate edge handler for a given edge
// and passes its items to the chosen handler. When the value of edge has a type that
// is not recognized, an iter.Seq containing an Item with an error is returned.
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
