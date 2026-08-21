package pipeline_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/language/pkg/go/graph"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/internal/listobjects/pipeline"
)

func TestFlattenWildcardEdges(t *testing.T) {
	var tests = []struct {
		name    string
		model   string
		nodeFn  func(*graph.WeightedAuthorizationModelGraph) *graph.WeightedAuthorizationModelNode
		target  string
		edgesFn func(*graph.WeightedAuthorizationModelGraph) []*graph.WeightedAuthorizationModelEdge
	}{
		{
			name: "nested wildcard with union should return set of all edges to wildcard",
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define blocked: [user:*]
						define rel_a: [user:*] or blocked
						define rel_b: [user:*] or rel_a
						define rel_c: [user:*] or rel_b
						define viewer: [user] but not rel_c
			`,
			nodeFn: func(g *graph.WeightedAuthorizationModelGraph) *graph.WeightedAuthorizationModelNode {
				edges, _ := g.GetEdgesFromNodeID("document#viewer")
				return edges[0].GetTo()
			},
			target: "user",
			edgesFn: func(g *graph.WeightedAuthorizationModelGraph) []*graph.WeightedAuthorizationModelEdge {
				var out []*graph.WeightedAuthorizationModelEdge

				wildcard, _ := g.GetNodeByID("user:*")

				adjacencies := g.GetEdges()

				for _, edges := range adjacencies {
					for _, edge := range edges {
						if edge.GetTo() != wildcard {
							continue
						}
						out = append(out, edge)
					}
				}
				return out
			},
		},
		{
			name: "nested wildcard with intersection should return empty set",
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define blocked: [user:*]
						define rel_a: [user:*] and blocked
						define rel_b: [user:*] or rel_a
						define rel_c: [user:*] or rel_b
						define viewer: [user] but not rel_c
			`,
			nodeFn: func(g *graph.WeightedAuthorizationModelGraph) *graph.WeightedAuthorizationModelNode {
				edges, _ := g.GetEdgesFromNodeID("document#viewer")
				return edges[0].GetTo()
			},
			target: "user",
			edgesFn: func(_ *graph.WeightedAuthorizationModelGraph) []*graph.WeightedAuthorizationModelEdge {
				return nil
			},
		},
		{
			name: "nested wildcard with exclusion should return empty set",
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define blocked: [user:*]
						define rel_a: [user:*] but not blocked
						define rel_b: [user:*] or rel_a
						define rel_c: [user:*] or rel_b
						define viewer: [user] but not rel_c
			`,
			nodeFn: func(g *graph.WeightedAuthorizationModelGraph) *graph.WeightedAuthorizationModelNode {
				edges, _ := g.GetEdgesFromNodeID("document#viewer")
				return edges[0].GetTo()
			},
			target: "user",
			edgesFn: func(_ *graph.WeightedAuthorizationModelGraph) []*graph.WeightedAuthorizationModelEdge {
				return nil
			},
		},
		{
			name: "nested wildcard with user should return empty set",
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define blocked: [user, user:*]
						define rel_a: [user:*] or blocked
						define rel_b: [user:*] or rel_a
						define rel_c: [user:*] or rel_b
						define viewer: [user] but not rel_c
			`,
			nodeFn: func(g *graph.WeightedAuthorizationModelGraph) *graph.WeightedAuthorizationModelNode {
				edges, _ := g.GetEdgesFromNodeID("document#viewer")
				return edges[0].GetTo()
			},
			target: "user",
			edgesFn: func(_ *graph.WeightedAuthorizationModelGraph) []*graph.WeightedAuthorizationModelEdge {
				return nil
			},
		},
		{
			name: "nested wildcard with userset should return empty set",
			model: `
				model
					schema 1.1

				type user

				type group
					relations
						define member: [user:*]

				type document
					relations
						define blocked: [user:*, group#member]
						define rel_a: [user:*] or blocked
						define rel_b: [user:*] or rel_a
						define rel_c: [user:*] or rel_b
						define viewer: [user] but not rel_c
			`,
			nodeFn: func(g *graph.WeightedAuthorizationModelGraph) *graph.WeightedAuthorizationModelNode {
				edges, _ := g.GetEdgesFromNodeID("document#viewer")
				return edges[0].GetTo()
			},
			target: "user",
			edgesFn: func(_ *graph.WeightedAuthorizationModelGraph) []*graph.WeightedAuthorizationModelEdge {
				return nil
			},
		},
		{
			name: "nested wildcard with ttu should return empty set",
			model: `
				model
					schema 1.1

				type user

				type group
					relations
						define member: [user:*]

				type document
					relations
						define parent: [group]
						define blocked: [user:*] or member from parent
						define rel_a: [user:*] or blocked
						define rel_b: [user:*] or rel_a
						define rel_c: [user:*] or rel_b
						define viewer: [user] but not rel_c
			`,
			nodeFn: func(g *graph.WeightedAuthorizationModelGraph) *graph.WeightedAuthorizationModelNode {
				edges, _ := g.GetEdgesFromNodeID("document#viewer")
				return edges[0].GetTo()
			},
			target: "user",
			edgesFn: func(_ *graph.WeightedAuthorizationModelGraph) []*graph.WeightedAuthorizationModelEdge {
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model := parser.MustTransformDSLToProto(tt.model)
			builder := graph.NewWeightedAuthorizationModelGraphBuilder()
			g, err := builder.Build(model)
			require.NoError(t, err)

			node := tt.nodeFn(g)
			require.Equal(t, graph.OperatorNode, node.GetNodeType())
			require.Equal(t, graph.ExclusionOperator, node.GetLabel())

			edges, _ := g.GetEdgesFromNode(node)
			require.Len(t, edges, 2)

			subtract := edges[1]

			result := pipeline.FlattenWildcardEdges(g, subtract, tt.target)
			require.ElementsMatch(t, tt.edgesFn(g), result)
		})
	}
}
