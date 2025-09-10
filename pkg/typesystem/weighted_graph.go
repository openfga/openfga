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
// hasPathTo reports whether the given graph item has a weight defined for the
// specified destination type (i.e., whether a path to destinationType exists).
// It returns true if GetWeight(destinationType) reports a weight is present.
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
// GetEdgesForIntersection computes which edge group should be treated as the "lowest" group
// for an intersection and returns that group together with the remaining sibling groups.
//
// It requires at least two input edges and groups edges using GetGroupedEdges (which omits
// edges that have no path to the provided sourceType). For each group, the function considers
// the group's maximum weight and selects the group with the smallest maximum weight as
// LowestEdges. If a tie occurs, the group with key "direct_edges" is preferred. The remaining
// groups are returned as SiblingEdges.
//
// If fewer than two edges are provided, an error is returned. If grouping removes all connected
// edges (no group selected), the returned IntersectionEdges will contain nil/empty slices.
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
	intersectionEdges := make([][]*graph.WeightedAuthorizationModelEdge, 0, len(edges))

	for key, group := range groupedEdges {
		maxWeight := 0
		for _, edge := range group {
			weight, _ := edge.GetWeight(sourceType)
			// get the max weight of the grouping
			if weight > maxWeight {
				maxWeight = weight
			}
		}
		// if the max weight of the grouping is bigger than 0 and it is lower than the lowest weight found so far
		// take the lowest weight or in the case of direct edges have more priority if there are others with the same weight
		if maxWeight < lowestWeight || (key == directEdgesKey && maxWeight == lowestWeight) {
			if lowestKey != "" {
				// verify the intersection edges
				intersectionEdges = append(intersectionEdges, groupedEdges[lowestKey])
			}
			lowestWeight = maxWeight
			lowestKey = key
		} else {
			intersectionEdges = append(intersectionEdges, group)
		}
	}

	return IntersectionEdges{
		LowestEdges:  groupedEdges[lowestKey],
		SiblingEdges: intersectionEdges,
	}, nil
}

// whether an edge has a path/weight to that type.
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
// GetEdgesForExclusion groups the provided weighted edges into a base group and an optional exclusion group for an exclusion ("A but not B") operation.
//
// It filters out edges that have no path (weight) to the given sourceType, then derives a grouping key for each edge:
// - Direct edges are grouped under the constant key `direct_edges`.
// - TTU edges use a key formed as `objectType#parent#relation` derived from the tupleset relation and the target relation.
// - All other edge types are treated as distinct and assigned a unique `other_<id>` key.
// The function returns an ExclusionEdges with BaseEdges set to the first encountered group and ExcludedEdges set to a different group if one exists.
// If no exclusion group is present (i.e., all relevant edges belong to the base group), ExcludedEdges will be nil.
//
// Errors:
// - returned if fewer than two input edges are provided for the operation.
// - returned if grouping yields no base group or more than two distinct groups.
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
