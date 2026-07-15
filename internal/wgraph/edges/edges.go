package edges

import (
	"github.com/openfga/language/pkg/go/graph"
)

// Predicate is a type that accepts an edge and retuns a boolean value.
type Predicate func(*graph.WeightedAuthorizationModelEdge) bool

// WeightOne returns a predicate that returns true when
// the given edge has a weight of one to the terminal type.
func WeightOne(terminal string) Predicate {
	return func(edge *graph.WeightedAuthorizationModelEdge) bool {
		weight, _ := edge.GetWeight(terminal)
		return weight == 1
	}
}

// SplitBy splits source into two slices. The first slice
// contains all edges that return true for fn. The second
// slice contains all edges that return false for fn.
func SplitBy(
	fn Predicate, source ...*graph.WeightedAuthorizationModelEdge,
) ([]*graph.WeightedAuthorizationModelEdge, []*graph.WeightedAuthorizationModelEdge) {
	storage := make([]*graph.WeightedAuthorizationModelEdge, len(source))

	front := 0
	back := len(source) - 1

	for _, edge := range source {
		if fn(edge) {
			storage[front] = edge
			front++
			continue
		}
		storage[back] = edge
		back--
	}

	// Slice out the exact bounds filled
	matches := storage[:front]
	differs := storage[back+1:]

	return matches, differs
}
