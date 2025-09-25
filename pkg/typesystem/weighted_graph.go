package typesystem

import (
	"fmt"
	"math"

	"github.com/oklog/ulid/v2"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/pkg/tuple"
)

const directEdgesKey = "direct_edges"

type weightedGraphItem interface {
	GetWeight(destinationType string) (int, bool)
}

// hasPathTo returns a boolean indicating if a path exists from a node or edge to a terminal type. E.g
// can we reach "user" from "document".
func hasPathTo(nodeOrEdge weightedGraphItem, destinationType string) bool {
	_, ok := nodeOrEdge.GetWeight(destinationType)
	return ok
}

type IntersectionEdges struct {
	LowestEdges  []*graph.WeightedAuthorizationModelEdge   // lowest edges to apply list objects
	SiblingEdges [][]*graph.WeightedAuthorizationModelEdge // the rest of the edges to apply intersection
}

type ExclusionEdges struct {
	BaseEdges     []*graph.WeightedAuthorizationModelEdge // base edges to apply list objects
	ExcludedEdges []*graph.WeightedAuthorizationModelEdge // excluded edges to apply exclusion
}

// GetEdgesForIntersection returns the lowest weighted edge and
// its siblings edges for intersection based via the weighted graph.
// If the direct edges have equal weight as its sibling edges, it will choose
// the direct edges as preference.
// If any of the children are not connected, it will return empty IntersectionEdges.
func GetEdgesForIntersection(edges []*graph.WeightedAuthorizationModelEdge, sourceType string) (IntersectionEdges, error) {
	if len(edges) < 2 {
		// Intersection by definition must have at least 2 children
		return IntersectionEdges{}, fmt.Errorf("invalid edges for source type %s", sourceType)
	}

	// Group edges by type
	groupedEdges := GetGroupedEdges(edges, sourceType)

	// Find the group with the lowest maximum weight
	lowestWeight := math.MaxInt32
	lowestKey := ""
	siblingEdges := make([][]*graph.WeightedAuthorizationModelEdge, 0, len(edges))

	for key, group := range groupedEdges {
		maxWeight := 0
		for _, edge := range group {
			weight, _ := edge.GetWeight(sourceType)
			// get the max weight of the grouping
			if weight > maxWeight {
				maxWeight = weight
			}
		}
		// if the max weight of the grouping is lower than the lowest weight found so far
		// take the lowest weight or in the case of direct edges have more priority if there are others with the same weight
		if maxWeight < lowestWeight || (key == directEdgesKey && maxWeight == lowestWeight) {
			if lowestKey != "" {
				// verify the intersection edges
				siblingEdges = append(siblingEdges, groupedEdges[lowestKey])
			}
			lowestWeight = maxWeight
			lowestKey = key
		} else {
			siblingEdges = append(siblingEdges, group)
		}
	}

	return IntersectionEdges{
		LowestEdges:  groupedEdges[lowestKey],
		SiblingEdges: siblingEdges,
	}, nil
}

// GetGroupedEdges groups edges for direct edge or TTU edges by their type and parent relation.
// Other edge types are treated individually.
func GetGroupedEdges(edges []*graph.WeightedAuthorizationModelEdge, sourceType string) map[string][]*graph.WeightedAuthorizationModelEdge {
	// Group edges by type
	groupedEdges := make(map[string][]*graph.WeightedAuthorizationModelEdge, len(edges))

	for _, edge := range edges {
		_, ok := edge.GetWeight(sourceType)
		if !ok {
			// Skip edges that don't have a path to the source type
			continue
		}
		var key string
		switch edge.GetEdgeType() {
		case graph.DirectEdge:
			key = directEdgesKey
		case graph.TTUEdge:
			objtype, parent := tuple.SplitObjectRelation(edge.GetTuplesetRelation())
			_, relation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
			key = objtype + "#" + parent + "#" + relation
		default:
			// Other edge types are treated individually
			key = "other_" + ulid.Make().String()
		}

		groupedEdges[key] = append(groupedEdges[key], edge)
	}
	return groupedEdges
}

// GetEdgesForExclusion returns the base edges (i.e., edge A in "A but not B") and
// excluded edge (edge B in "A but not B") based on weighted graph for exclusion.
func GetEdgesForExclusion(
	edges []*graph.WeightedAuthorizationModelEdge,
	sourceType string,
) (ExclusionEdges, error) {
	if len(edges) < 2 {
		return ExclusionEdges{}, fmt.Errorf("invalid exclusion edges for source type %s", sourceType)
	}

	// Group edges by type
	groupedEdges := make(map[string][]*graph.WeightedAuthorizationModelEdge, 2)
	baseKey := ""
	exclusionKey := ""

	for _, edge := range edges {
		var key string
		_, ok := edge.GetWeight(sourceType)
		if !ok {
			// Skip edges that don't have a path to the source type
			continue
		}

		switch edge.GetEdgeType() {
		case graph.DirectEdge:
			key = directEdgesKey
		case graph.TTUEdge:
			objtype, parent := tuple.SplitObjectRelation(edge.GetTuplesetRelation())
			_, relation := tuple.SplitObjectRelation(edge.GetTo().GetUniqueLabel())
			key = objtype + "#" + parent + "#" + relation
		default:
			// Other edge types are treated individually
			key = "other_" + ulid.Make().String()
		}
		if len(baseKey) == 0 {
			baseKey = key
		} else if baseKey != key {
			exclusionKey = key
		}
		groupedEdges[key] = append(groupedEdges[key], edge)
	}

	if len(groupedEdges) > 2 || len(baseKey) == 0 {
		return ExclusionEdges{}, fmt.Errorf("invalid exclusion edges for source type %s", sourceType)
	}

	// for the case of exclusion, we can have that the exlusion part does not have any edge that lead to
	// the user type in question, so no need to run check over it, as it will never negate it the results of the base
	if exclusionKey == "" {
		return ExclusionEdges{
			BaseEdges:     groupedEdges[baseKey],
			ExcludedEdges: nil,
		}, nil
	}

	return ExclusionEdges{
		BaseEdges:     groupedEdges[baseKey],
		ExcludedEdges: groupedEdges[exclusionKey],
	}, nil
}

// ConstructUserset returns the openfgav1.Userset to run CheckRewrite against list objects candidate when
// model has intersection / exclusion.
func (t *TypeSystem) ConstructUserset(currentEdge *graph.WeightedAuthorizationModelEdge, sourceUserType string) (*openfgav1.Userset, error) {
	currentNode := currentEdge.GetTo()
	edgeType := currentEdge.GetEdgeType()
	uniqueLabel := currentNode.GetUniqueLabel()

	switch currentNode.GetNodeType() {
	case graph.SpecificType, graph.SpecificTypeWildcard:
		return This(), nil
	case graph.SpecificTypeAndRelation:
		switch edgeType {
		case graph.DirectEdge:
			// userset use case
			return This(), nil
		case graph.RewriteEdge, graph.ComputedEdge:
			_, relation := tuple.SplitObjectRelation(uniqueLabel)
			return &openfgav1.Userset{
				Userset: &openfgav1.Userset_ComputedUserset{
					ComputedUserset: &openfgav1.ObjectRelation{
						Relation: relation,
					},
				},
			}, nil
		case graph.TTUEdge:
			_, parent := tuple.SplitObjectRelation(currentEdge.GetTuplesetRelation())
			_, relation := tuple.SplitObjectRelation(uniqueLabel)
			return &openfgav1.Userset{
				Userset: &openfgav1.Userset_TupleToUserset{
					TupleToUserset: &openfgav1.TupleToUserset{
						Tupleset: &openfgav1.ObjectRelation{
							Relation: parent, // parent
						},
						ComputedUserset: &openfgav1.ObjectRelation{
							Relation: relation,
						},
					},
				},
			}, nil
		default:
			// This should never happen.
			return nil, fmt.Errorf("unknown edge type: %v for node: %s", edgeType, currentNode.GetUniqueLabel())
		}

	case graph.OperatorNode:
		switch currentNode.GetLabel() {
		case graph.ExclusionOperator:
			return t.ConstructExclusionUserset(currentNode, sourceUserType)
		case graph.IntersectionOperator:
			return t.ConstructIntersectionUserset(currentNode, sourceUserType)
		case graph.UnionOperator:
			return t.ConstructUnionUserset(currentNode, sourceUserType)
		default:
			// This should never happen.
			return nil, fmt.Errorf("unknown operator node label %s for node %s", currentNode.GetLabel(), currentNode.GetUniqueLabel())
		}
	default:
		// This should never happen.
		return nil, fmt.Errorf("unknown node type %v for node %s", currentNode.GetNodeType(), currentNode.GetUniqueLabel())
	}
}

func (t *TypeSystem) ConstructExclusionUserset(node *graph.WeightedAuthorizationModelNode, sourceUserType string) (*openfgav1.Userset, error) {
	edges, ok := t.authzWeightedGraph.GetEdgesFromNode(node)
	if !ok || node.GetLabel() != graph.ExclusionOperator {
		// This should never happen.
		return nil, fmt.Errorf("incorrect exclusion node: %s", node.GetUniqueLabel())
	}
	exclusionEdges, err := GetEdgesForExclusion(edges, sourceUserType)
	if err != nil || len(exclusionEdges.BaseEdges) == 0 {
		return nil, fmt.Errorf("error getting the edges for operation: exclusion: %s", err.Error())
	}

	baseUserset, err := t.ConstructUserset(exclusionEdges.BaseEdges[0], sourceUserType)
	if err != nil {
		return nil, fmt.Errorf("failed to construct userset for edge %s: %w", exclusionEdges.BaseEdges[0].GetTo().GetUniqueLabel(), err)
	}

	if len(exclusionEdges.ExcludedEdges) == 0 {
		return baseUserset, nil
	}

	exclusionUserset, err := t.ConstructUserset(exclusionEdges.ExcludedEdges[0], sourceUserType)
	if err != nil {
		return nil, fmt.Errorf("failed to construct userset for edge %s: %w", exclusionEdges.ExcludedEdges[0].GetTo().GetUniqueLabel(), err)
	}

	return &openfgav1.Userset{
		Userset: &openfgav1.Userset_Difference{
			Difference: &openfgav1.Difference{
				Base:     baseUserset,
				Subtract: exclusionUserset,
			}}}, nil
}

func (t *TypeSystem) ConstructIntersectionUserset(node *graph.WeightedAuthorizationModelNode, sourceUserType string) (*openfgav1.Userset, error) {
	edges, ok := t.authzWeightedGraph.GetEdgesFromNode(node)
	if !ok || node.GetLabel() != graph.IntersectionOperator {
		// This should never happen.
		return nil, fmt.Errorf("incorrect intersection node: %s", node.GetUniqueLabel())
	}
	var usersets []*openfgav1.Userset
	groupedEdges := GetGroupedEdges(edges, sourceUserType)

	if len(groupedEdges) < 2 {
		return nil, fmt.Errorf("no valid edges found for intersection")
	}

	for _, edges := range groupedEdges {
		userset, err := t.ConstructUserset(edges[0], sourceUserType)
		if err != nil {
			return nil, fmt.Errorf("failed to construct userset for edge %s: %w", edges[0].GetTo().GetUniqueLabel(), err)
		}
		usersets = append(usersets, userset)
	}

	return &openfgav1.Userset{
		Userset: &openfgav1.Userset_Intersection{
			Intersection: &openfgav1.Usersets{
				Child: usersets,
			}}}, nil
}

func (t *TypeSystem) ConstructUnionUserset(node *graph.WeightedAuthorizationModelNode, sourceUserType string) (*openfgav1.Userset, error) {
	edges, ok := t.authzWeightedGraph.GetEdgesFromNode(node)
	if !ok || node.GetLabel() != graph.UnionOperator {
		// This should never happen.
		return nil, fmt.Errorf("incorrect union node: %s", node.GetUniqueLabel())
	}
	var usersets []*openfgav1.Userset
	groupedEdges := GetGroupedEdges(edges, sourceUserType)

	if len(groupedEdges) < 2 {
		return nil, fmt.Errorf("no valid edges found for union")
	}

	for _, edges := range groupedEdges {
		userset, err := t.ConstructUserset(edges[0], sourceUserType)
		if err != nil {
			return nil, fmt.Errorf("failed to construct userset for edge %s: %w", edges[0].GetTo().GetUniqueLabel(), err)
		}
		usersets = append(usersets, userset)
	}

	return &openfgav1.Userset{
		Userset: &openfgav1.Userset_Union{
			Union: &openfgav1.Usersets{
				Child: usersets,
			}}}, nil
}
