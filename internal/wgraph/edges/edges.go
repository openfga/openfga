package edges

import "github.com/openfga/language/pkg/go/graph"

func SplitWeightOne(terminal string, edges ...*graph.WeightedAuthorizationModelEdge) ([]*graph.WeightedAuthorizationModelEdge, []*graph.WeightedAuthorizationModelEdge) {
	dst := make([]*graph.WeightedAuthorizationModelEdge, len(edges))
	copy(dst, edges)

	// left tracks first non-weight-1
	// right looks for the first weight 1 to swap with left

	var left int

	for right := range len(dst) {
		rightWeight, _ := dst[right].GetWeight(terminal)

		if rightWeight == 1 {
			dst[left], dst[right] = dst[right], dst[left]
			left++
		}
	}

	return dst[:left], dst[left:]
}
