package edges

import "github.com/openfga/language/pkg/go/graph"

func SplitWeightOne(terminal string, edges ...*graph.WeightedAuthorizationModelEdge) ([]*graph.WeightedAuthorizationModelEdge, []*graph.WeightedAuthorizationModelEdge) {
	// left tracks first non-weight-1
	// right looks for the first weight 1 to swap with left

	var left int

	for right := range len(edges) {
		rightWeight, _ := edges[right].GetWeight(terminal)

		if rightWeight == 1 {
			edges[left], edges[right] = edges[right], edges[left]
			left++
		}
	}

	return edges[:left], edges[left:]
}
