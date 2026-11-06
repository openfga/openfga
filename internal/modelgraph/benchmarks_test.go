package modelgraph_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/pkg/testutils"
)

// flattenCase describes a single model whose target node exercises FlattenNode.
// Every model is chosen so that the target node has union/computed/rewrite/TTU
// edges that must be expanded (flattened) into their terminal edges - a model
// whose target already consists solely of direct edges would not benchmark the
// flattening work at all.
type flattenCase struct {
	name string
	// model is the FGA DSL for the authorization model.
	model string
	// nodeID is the object#relation node handed to FlattenNode.
	nodeID string
	// userType is the terminal user type resolution is aimed at.
	userType string
	// hasWildcardRequest toggles the wildcard-only edge filtering path.
	hasWildcardRequest bool
	// skipRecursiveRelation, when set, is the recursive relation FlattenNode
	// must exclude (mirrors ResolveRecursive/bottomUp callers).
	skipRecursiveRelation string
}

// buildDeepComputedChainModel produces a pathological model consisting of a
// single, very long chain of computed relations:
//
//	define r0: r1
//	define r1: r2
//	...
//	define r{depth-1}: r{depth}
//	define r{depth}: [user]
//
// Flattening document#r0 forces FlattenNode to walk every computed edge down to
// the single terminal direct edge, giving worst-case traversal depth.
func buildDeepComputedChainModel(depth int) string {
	var b strings.Builder
	b.WriteString("model\n  schema 1.1\ntype user\ntype document\n  relations\n")
	for i := range depth {
		fmt.Fprintf(&b, "    define r%d: r%d\n", i, i+1)
	}
	fmt.Fprintf(&b, "    define r%d: [user]\n", depth)
	return b.String()
}

// buildWideUnionModel produces a pathological model with a single relation whose
// definition is a union of `width` sibling relations, each resolving directly to
// user:
//
//	define target: rel0 or rel1 or ... or rel{width-1}
//	define rel0: [user]
//	...
//
// Flattening document#target forces FlattenNode to expand the union operator and
// then every one of its computed branches, giving worst-case fan-out.
func buildWideUnionModel(width int) string {
	var b strings.Builder
	b.WriteString("model\n  schema 1.1\ntype user\ntype document\n  relations\n")

	b.WriteString("    define target: ")
	for i := range width {
		if i > 0 {
			b.WriteString(" or ")
		}
		fmt.Fprintf(&b, "rel%d", i)
	}
	b.WriteByte('\n')

	for i := range width {
		fmt.Fprintf(&b, "    define rel%d: [user]\n", i)
	}
	return b.String()
}

func flattenCases() []flattenCase {
	return []flattenCase{
		{
			// Simple computed chain: member -> reader/public via nested computed
			// relations feeding a union.
			name: "computed_union",
			model: `
				model
				  schema 1.1
				type user
				type group
				  relations
				    define viewer: member
				    define member: reader or public
				    define public: [user:*]
				    define reader: [user]`,
			nodeID:   "group#viewer",
			userType: "user",
		},
		{
			// Union at the top level with a mix of direct and computed operands.
			name: "top_level_union",
			model: `
				model
				  schema 1.1
				type user
				type group
				  relations
				    define member: [user] or admin or editor
				    define admin: [user]
				    define editor: [user]`,
			nodeID:   "group#member",
			userType: "user",
		},
		{
			// Tuple-to-userset: viewer resolved through a parent document.
			name: "ttu_logical",
			model: `
				model
				  schema 1.1
				type user
				type document
				  relations
				    define parent: [document]
				    define viewer: [user] or viewer from parent`,
			nodeID:   "document#viewer",
			userType: "user",
		},
		{
			// Nested TTU + computed: owner reached through a chain of parents and
			// computed relations.
			name: "nested_ttu_computed",
			model: `
				model
				  schema 1.1
				type user
				type folder
				  relations
				    define parent: [folder]
				    define owner: [user] or manager
				    define manager: [user] or owner from parent`,
			nodeID:   "folder#owner",
			userType: "user",
		},
		{
			// Union whose operands are usersets referencing another type's
			// relation (indirection through group#member).
			name: "userset_union",
			model: `
				model
				  schema 1.1
				type user
				type group
				  relations
				    define member: [user]
				type document
				  relations
				    define viewer: [group#member] or editor
				    define editor: [group#member]`,
			nodeID:   "document#viewer",
			userType: "user",
		},
		{
			// Intersection nested inside a union - the intersection operator node
			// is NOT flattened, so its edge is kept as a single result edge.
			name: "union_with_intersection",
			model: `
				model
				  schema 1.1
				type user
				type document
				  relations
				    define viewer: [user] or (editor and reviewer)
				    define editor: [user]
				    define reviewer: [user]`,
			nodeID:   "document#viewer",
			userType: "user",
		},
		{
			// Exclusion nested inside a union - the difference operator node is
			// NOT flattened, kept as a single result edge.
			name: "union_with_exclusion",
			model: `
				model
				  schema 1.1
				type user
				type document
				  relations
				    define viewer: [user] or (editor but not banned)
				    define editor: [user]
				    define banned: [user]`,
			nodeID:   "document#viewer",
			userType: "user",
		},
		{
			// Wildcard request against a union of wildcard-bearing operands -
			// exercises the hasWildcardRequest filtering branch.
			name: "wildcard_union",
			model: `
				model
				  schema 1.1
				type user
				type group
				  relations
				    define member: [user:*] or admin
				    define admin: [user:*]`,
			nodeID:             "group#member",
			userType:           "user",
			hasWildcardRequest: true,
		},
		{
			// Recursive userset (group#member references group#member). The
			// recursive relation is skipped, mirroring bottom-up callers.
			name: "recursive_userset_skipped",
			model: `
				model
				  schema 1.1
				type user
				type group
				  relations
				    define member: [user, group#member] or admin
				    define admin: [user]`,
			nodeID:                "group#member",
			userType:              "user",
			skipRecursiveRelation: "group#member",
		},
		{
			// Recursive TTU (member from parent) skipped, while an unrelated
			// computed branch (admin) is still flattened.
			name: "recursive_ttu_skipped",
			model: `
				model
				  schema 1.1
				type user
				type group
				  relations
				    define parent: [group]
				    define member: [user] or member from parent or admin
				    define admin: [user]`,
			nodeID:                "group#member",
			userType:              "user",
			skipRecursiveRelation: "group#member",
		},
		{
			// Only the targeted recursive relation is skipped; an unrelated,
			// independently-recursive relation reached along a non-recursive
			// branch must still be flattened (regression for issue #3195).
			name: "recursive_selective_skip",
			model: `
				model
				  schema 1.1
				type user
				type group
				  relations
				    define admin: direct_admin or admin from parent_group
				    define child_group: [group]
				    define descendant_principal: member or descendant_principal from child_group
				    define direct_admin: [user]
				    define direct_member: [user]
				    define member: direct_member or admin
				    define parent_group: [group]`,
			nodeID:                "group#descendant_principal",
			userType:              "user",
			skipRecursiveRelation: "group#descendant_principal",
		},
		{
			// Broad, realistic model combining computed, union, TTU and userset
			// indirection across multiple types.
			name: "mixed_broad",
			model: `
				model
				  schema 1.1
				type user
				type org
				  relations
				    define member: [user]
				type folder
				  relations
				    define parent: [folder]
				    define owner: [user, org#member] or owner from parent
				type document
				  relations
				    define parent: [folder]
				    define viewer: [user, org#member] or editor or owner from parent
				    define editor: [user] or owner from parent`,
			nodeID:   "document#viewer",
			userType: "user",
		},
		{
			// PATHOLOGICAL: a 100-deep chain of computed relations. Worst-case
			// traversal depth for the flattening walk.
			name:     "pathological_deep_computed_chain",
			model:    buildDeepComputedChainModel(100),
			nodeID:   "document#r0",
			userType: "user",
		},
		{
			// PATHOLOGICAL: a single union of 200 sibling relations. Worst-case
			// fan-out for the flattening walk.
			name:     "pathological_wide_union",
			model:    buildWideUnionModel(200),
			nodeID:   "document#target",
			userType: "user",
		},
	}
}

func BenchmarkFlattenNode(b *testing.B) {
	for _, bc := range flattenCases() {
		// Build the graph and resolve the target node once, outside the timed
		// loop, so the benchmark measures FlattenNode in isolation.
		model := testutils.MustTransformDSLToProtoWithID(bc.model)
		graph, err := modelgraph.New(model)
		require.NoError(b, err)

		node, ok := graph.GetNodeByID(bc.nodeID)
		require.True(b, ok, "node %q not found", bc.nodeID)

		// Sanity-check the case actually flattens (and doesn't error) before we
		// benchmark it, so a broken model surfaces as a failure not a fast loop.
		_, err = graph.FlattenNode(node, bc.userType, bc.hasWildcardRequest, bc.skipRecursiveRelation)
		require.NoError(b, err)

		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, _ = graph.FlattenNode(node, bc.userType, bc.hasWildcardRequest, bc.skipRecursiveRelation)
			}
		})
	}
}
