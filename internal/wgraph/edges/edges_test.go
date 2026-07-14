package edges_test

import (
	"testing"

	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/internal/wgraph/edges"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestSplitWeightOne(t *testing.T) {
	cases := []struct {
		name          string
		model         string
		source        string
		target        string
		weightOne     int
		weightTwoPlus int
	}{
		{
			name: "mixed weight one and weight two",
			model: `
				model
					schema 1.1
		
				type user
		
				type folder
					relations
						define viewer: [user]
		
				type document
					relations
						define parent: [folder]
						define owner: [user] or editor
						define editor: [user] but not blocked
						define blocked: [user]
						define viewer: [user, user:*, folder#viewer] or owner or viewer from parent
			`,
			source:        "document#viewer",
			target:        "user",
			weightOne:     4,
			weightTwoPlus: 2,
		},
		{
			name: "multiple direct weight one",
			model: `
				model
					schema 1.1
		
				type user
		
				type document
					relations
						define r0: [user]
						define r1: [user]
						define r2: [user]
						define r3: [user]
						define r4: [user]
						define r5: [user]
						define r6: [user]
						define r7: [user]
						define r8: [user]
						define r9: [user]
						define viewer: r0 or r1 or r2 or r3 or r4 or r5 or r6 or r7 or r8 or r9
			`,
			source:        "document#viewer",
			target:        "user",
			weightOne:     10,
			weightTwoPlus: 0,
		},
		{
			name: "single direct weight one",
			model: `
				model
					schema 1.1
		
				type user
		
				type document
					relations
						define viewer: [user]
			`,
			source:        "document#viewer",
			target:        "user",
			weightOne:     1,
			weightTwoPlus: 0,
		},
		{
			name: "single weight two",
			model: `
				model
					schema 1.1
		
				type user
		
				type folder
					relations
						define viewer: [user]
		
				type document
					relations
						define parent: [folder]
						define viewer: viewer from parent
			`,
			source:        "document#viewer",
			target:        "user",
			weightOne:     0,
			weightTwoPlus: 1,
		},
		{
			name: "no path to target",
			model: `
				model
					schema 1.1

				type user

				type user2

				type user3

				type user4

				type directory
					relations
						define viewer: [user2]

				type folder
					relations
						define viewer: [user]

				type document
					relations
						define parent: [folder, directory]
						define editor: [user]
						define viewer: [user2, user3, user4] or editor or viewer from parent
			`,
			source:        "document#viewer",
			target:        "user",
			weightOne:     1,
			weightTwoPlus: 1,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(c.model)
			wgraph, err := modelgraph.New(model)
			require.NoError(t, err)

			node, ok := wgraph.GetNodeByID(c.source)
			require.True(t, ok)

			e, err := wgraph.FlattenNode(node, c.target, false, false)
			require.NoError(t, err)

			w1, w2 := edges.SplitWeightOne(c.target, e...)

			require.Len(t, w1, c.weightOne, "weight one did not have the correct number of elements")
			for _, edge := range w1 {
				weight, ok := edge.GetWeight(c.target)
				require.True(t, ok)
				require.Equal(t, 1, weight)
			}

			require.Len(t, w2, c.weightTwoPlus, "weight two plus did not have the correct number of elements")
			for _, edge := range w2 {
				weight, ok := edge.GetWeight(c.target)
				require.True(t, ok)
				require.Greater(t, weight, 1)
			}
		})
	}
}
