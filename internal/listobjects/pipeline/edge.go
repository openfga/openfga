package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/listobjects/pipeline/internal/worker"
	"github.com/openfga/openfga/pkg/tuple"
)

// edgeInterpreter dispatches to the appropriate handler based on edge type.
type edgeInterpreter struct {
	store ObjectStore
	graph *Graph
}

var _ worker.Interpreter = (*edgeInterpreter)(nil)

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

func (e *edgeInterpreter) Exists(ctx context.Context, edge *Edge, object, user string) (bool, error) {
	if t := edge.GetEdgeType(); t != graph.DirectEdge {
		return false, fmt.Errorf(
			"%w: expected type '%d'; got '%d'",
			worker.ErrUnexpectedEdge,
			graph.DirectEdge,
			t,
		)
	}

	targetNode := edge.GetTo()

	switch targetNode.GetNodeType() {
	case graph.SpecificType, graph.SpecificTypeWildcard, graph.SpecificTypeAndRelation:
	default:
		return false, fmt.Errorf(
			"%w: expected specific type",
			worker.ErrUnexpectedNode,
		)
	}
	targetLabel := targetNode.GetLabel()
	targetType, targetRelation := tuple.SplitObjectRelation(targetLabel)
	targetType = tuple.GetType(targetType)

	userType, userID, userRelation := tuple.ToUserParts(user)

	if userType != targetType {
		return false, fmt.Errorf(
			"%w: expected user type '%s'; got '%s'",
			worker.ErrUnexpectedUserType,
			targetType,
			userType,
		)
	}

	if userRelation != "" && userRelation != targetRelation {
		return false, fmt.Errorf(
			"%w: expected user relation '%s'; got '%s'",
			worker.ErrUnexpectedRelation,
			targetRelation,
			userRelation,
		)
	}

	targetID := userID

	if t := targetNode.GetNodeType(); t == graph.SpecificTypeWildcard {
		// since the target node is a wildcard, the query must
		// target a wildcard user.
		targetID = "*"
	}

	targetUser := tuple.BuildObject(targetType, targetID)

	if targetRelation != "" {
		targetUser = tuple.ToObjectRelationString(targetUser, targetRelation)
	}

	sourceLabel := edge.GetRelationDefinition()
	sourceType, sourceRelation := tuple.SplitObjectRelation(sourceLabel)

	if sourceRelation == "" {
		return false, fmt.Errorf("%w: expected non-empty source relation", worker.ErrUnexpectedRelation)
	}

	objectType, objectID := tuple.SplitObject(object)

	if sourceType != objectType {
		return false, fmt.Errorf(
			"%w: expected type '%s'; got type '%s'",
			worker.ErrUnexpectedObjectType,
			sourceType,
			objectType,
		)
	}

	sourceObject := tuple.BuildObject(sourceType, objectID)

	input := ObjectGet{
		Object:     sourceObject,
		Relation:   sourceRelation,
		User:       targetUser,
		Conditions: edge.GetConditions(),
	}

	item := e.store.Get(ctx, input)
	if item == nil {
		return false, nil
	}
	if item.Err != nil {
		return false, item.Err
	}
	return true, nil
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
