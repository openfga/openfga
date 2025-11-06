package graph

import (
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
)

var (
	RelationshipEdgeTransformer = cmp.Transformer("Sort", func(in []*RelationshipEdge) []*RelationshipEdge {
		out := append([]*RelationshipEdge(nil), in...) // Copy input to avoid mutating it

		// Sort by Type and then by edge and then by tupleset relation
		sort.SliceStable(out, func(i, j int) bool {
			if out[i].Type != out[j].Type {
				return out[i].Type < out[j].Type
			}

			if typesystem.GetRelationReferenceAsString(out[i].TargetReference) != typesystem.GetRelationReferenceAsString(out[j].TargetReference) {
				return typesystem.GetRelationReferenceAsString(out[i].TargetReference) < typesystem.GetRelationReferenceAsString(out[j].TargetReference)
			}

			if out[i].TuplesetRelation != out[j].TuplesetRelation {
				return out[i].TuplesetRelation < out[j].TuplesetRelation
			}

			return true
		})

		return out
	})
)

func TestRelationshipEdge_String(t *testing.T) {
	for _, tc := range []struct {
		name             string
		expected         string
		relationshipEdge RelationshipEdge
	}{
		{
			name:     "TupleToUsersetEdge",
			expected: "userset type:\"document\" relation:\"viewer\", type ttu, tupleset parent",
			relationshipEdge: RelationshipEdge{
				Type:             TupleToUsersetEdge,
				TargetReference:  typesystem.DirectRelationReference("document", "viewer"),
				TuplesetRelation: "parent",
				TargetReferenceInvolvesIntersectionOrExclusion: false,
			},
		},
		{
			name:     "ComputedUsersetEdge",
			expected: "userset type:\"document\" relation:\"viewer\", type computed_userset",
			relationshipEdge: RelationshipEdge{
				Type:            ComputedUsersetEdge,
				TargetReference: typesystem.DirectRelationReference("document", "viewer"),
				TargetReferenceInvolvesIntersectionOrExclusion: false,
			},
		},
		{
			name:     "DirectEdge",
			expected: "userset type:\"document\" relation:\"viewer\", type direct",
			relationshipEdge: RelationshipEdge{
				Type:            DirectEdge,
				TargetReference: typesystem.DirectRelationReference("document", "viewer"),
				TargetReferenceInvolvesIntersectionOrExclusion: false,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.relationshipEdge.String())
		})
	}
}

func TestRelationshipEdgeType_String(t *testing.T) {
	require.Equal(t, "direct", DirectEdge.String())
	require.Equal(t, "computed_userset", ComputedUsersetEdge.String())
	require.Equal(t, "ttu", TupleToUsersetEdge.String())
	require.Equal(t, "undefined", RelationshipEdgeType(4).String())
}

func TestPrunedRelationshipEdges(t *testing.T) {
	tests := []struct {
		name     string
		model    string
		target   *openfgav1.RelationReference
		source   *openfgav1.RelationReference
		expected []*RelationshipEdge
	}{
		{
			name: "basic_intersection",
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define allowed: [user]
						define viewer: [user] and allowed`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: true,
				},
			},
		},
		{
			name: "basic_intersection_through_ttu_1",
			model: `
				model
					schema 1.1

				type user

				type folder
					relations
						define allowed: [user]
						define viewer: [user] and allowed

				type document
					relations
						define parent: [folder]
						define viewer: viewer from parent`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("folder", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: true,
				},
			},
		},
		{
			name: "basic_intersection_through_ttu_2",
			model: `
				model
					schema 1.1

				type user

				type organization
					relations
						define allowed: [user]
						define viewer: [user] and allowed

				type folder
					relations
						define parent: [organization]
						define viewer: viewer from parent

				type document
					relations
						define parent: [folder]
						define viewer: viewer from parent`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("folder", "viewer"),
			expected: []*RelationshipEdge{
				{
					Type:             TupleToUsersetEdge,
					TargetReference:  typesystem.DirectRelationReference("document", "viewer"),
					TuplesetRelation: "parent",
					TargetReferenceInvolvesIntersectionOrExclusion: true,
				},
			},
		},
		{
			name: "basic_exclusion_through_ttu_1",
			model: `
				model
					schema 1.1

				type user

				type folder
					relations
						define writer: [user]
						define editor: [user]
						define viewer: writer but not editor

				type document
					relations
						define parent: [folder]
						define viewer: viewer from parent`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("folder", "writer"),
					TargetReferenceInvolvesIntersectionOrExclusion: true,
				},
			},
		},
		{
			name: "basic_exclusion_through_ttu_2",
			model: `
				model
					schema 1.1

				type user

				type folder
					relations
						define writer: [user]
						define editor: [user]
						define viewer: writer but not editor

				type document
					relations
						define parent: [folder]
						define viewer: viewer from parent`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("folder", "viewer"),
			expected: []*RelationshipEdge{
				{
					Type:             TupleToUsersetEdge,
					TargetReference:  typesystem.DirectRelationReference("document", "viewer"),
					TuplesetRelation: "parent",
					TargetReferenceInvolvesIntersectionOrExclusion: true,
				},
			},
		},
		{
			name: "ttu_with_indirect",
			model: `
				model
					schema 1.1

				type user
				type repo
					relations
						define admin: [user] or repo_admin from owner
						define owner: [organization]
				type organization
					relations
						define member: [user] or owner
						define owner: [user]
						define repo_admin: [user, organization#member]`,
			target: typesystem.DirectRelationReference("repo", "admin"),
			source: typesystem.DirectRelationReference("organization", "member"),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("organization", "repo_admin"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(test.model)
			typesys, err := typesystem.New(model)
			require.NoError(t, err)

			g := New(typesys)

			edges, err := g.GetPrunedRelationshipEdges(test.target, test.source)
			require.NoError(t, err)

			cmpOpts := []cmp.Option{
				cmpopts.IgnoreUnexported(openfgav1.RelationReference{}),
				RelationshipEdgeTransformer,
			}
			if diff := cmp.Diff(test.expected, edges, cmpOpts...); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRelationshipEdges(t *testing.T) {
	tests := []struct {
		name      string
		model     string
		authModel *openfgav1.AuthorizationModel // for models that have "self" or "this" at the end of the relation definition
		target    *openfgav1.RelationReference
		source    *openfgav1.RelationReference
		expected  []*RelationshipEdge
	}{
		{
			name: "direct_edge_through_ComputedUserset_with_multiple_type_restrictions",
			model: `
				model
					schema 1.1

				type user

				type group
					relations
						define member: [user, group#member]

				type document
					relations
						define editor: [user, group#member]
						define viewer: editor`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "editor"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("group", "member"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "direct_edge_through_ComputedUserset",
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define editor: [user]
						define viewer: editor`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "editor"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "direct_edge_through_TupleToUserset_with_multiple_type_restrictions",
			model: `
				model
					schema 1.1

				type user

				type group
					relations
						define member: [user]

				type folder
					relations
						define viewer: [user, group#member]

				type document
					relations
						define parent: [folder]
						define viewer: [user] or viewer from parent`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("folder", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("group", "member"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "direct_edge_with_union_involving_self_and_computed_userset",
			model: `
				model
					schema 1.1

				type user

				type group
					relations
						define member: [user, group#member]

				type document
					relations
						define editor: [user, group#member]
						define viewer: [user] or editor`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "editor"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("group", "member"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "circular_reference",
			model: `
				model
					schema 1.1

				type user

				type team
					relations
						define member: [group#member]

				type group
					relations
						define member: [user, team#member]`,
			target: typesystem.DirectRelationReference("team", "member"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("group", "member"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "cyclical_parent/child_definition",
			model: `
				model
					schema 1.1

				type user

				type folder
					relations
						define parent: [folder]
						define viewer: [user] or viewer from parent`,
			target: typesystem.DirectRelationReference("folder", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("folder", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "no_graph_relationship_connectivity",
			model: `
				model
					schema 1.1

				type user

				type team
					relations
						define member: [team#member]`,
			target:   typesystem.DirectRelationReference("team", "member"),
			source:   typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{},
		},
		{
			name: "test1",
			model: `
				model
					schema 1.1

				type user

				type group
					relations
						define member: [user]

				type folder
					relations
						define viewer: [user, group#member]

				type document
					relations
						define parent: [folder]
						define viewer: viewer from parent`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("folder", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("group", "member"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "test2",
			model: `
				model
					schema 1.1

				type user

				type group
					relations
						define member: [user]

				type folder
					relations
						define viewer: [user, group#member]

				type document
					relations
						define parent: [folder]
						define viewer: viewer from parent`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("group", "member"),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("folder", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "test3",
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
						define viewer: viewer from parent`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("folder", "viewer"),
			expected: []*RelationshipEdge{
				{
					Type:             TupleToUsersetEdge,
					TargetReference:  typesystem.DirectRelationReference("document", "viewer"),
					TuplesetRelation: "parent",
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "undefined_relation_on_one_type_involved_in_a_ttu",
			model: `
				model
					schema 1.1

				type user
				type organization

				type folder
					relations
						define viewer: [user]

				type document
					relations
						define parent: [folder, organization]
						define viewer: viewer from parent`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("folder", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "nested_group_membership_returns_only_top-level_edge",
			model: `
				model
					schema 1.1

				type user

				type group
					relations
						define member: [user, group#member]`,
			target: typesystem.DirectRelationReference("group", "member"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("group", "member"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "edges_for_non-assignable_relation",
			model: `
				model
					schema 1.1

				type organization
					relations
						define viewer: [organization]
						define can_view: viewer

				type document
					relations
						define parent: [organization]
						define view: can_view from parent`,
			target: typesystem.DirectRelationReference("document", "view"),
			source: typesystem.DirectRelationReference("organization", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("organization", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "user_is_a_subset_of_user_*",
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define viewer: [user:*]`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "user_*_is_not_a_subset_of_user",
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define viewer: [user]`,
			target:   typesystem.DirectRelationReference("document", "viewer"),
			source:   typesystem.WildcardRelationReference("user"),
			expected: []*RelationshipEdge{},
		},
		{
			name: "user_*_is_related_to_user_*",
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define viewer: [user:*]`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.WildcardRelationReference("user"),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "edges_involving_wildcard_in_types",
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define editor: [user:*]
						define viewer: editor`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "editor"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "edges_involving_wildcard_in_source",
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define editor: [user]
						define viewer: editor`,
			target:   typesystem.DirectRelationReference("document", "viewer"),
			source:   typesystem.WildcardRelationReference("user"),
			expected: []*RelationshipEdge{},
		},
		{
			name: "edges_involving_wildcards_1",
			model: `
				model
					schema 1.1

				type user
				type employee
				type group

				type document
					relations
						define relation1: [user:*] or relation2 or relation3 or relation4
						define relation2: [group:*]
						define relation3: [employee:*]
						define relation4: [user]`,
			target: typesystem.DirectRelationReference("document", "relation1"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "relation1"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "relation4"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "edges_involving_wildcards_2",
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define relation1: [user] or relation2
						define relation2: [user:*]`,
			target: typesystem.DirectRelationReference("document", "relation1"),
			source: typesystem.WildcardRelationReference("user"),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "relation2"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "indirect_typed_wildcard",
			model: `
				model
					schema 1.1

				type user

				type group
					relations
						define member: [user:*]

				type document
					relations
						define viewer: [group#member]`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("group", "member"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "indirect_relationship_multiple_levels_deep",
			model: `
				model
					schema 1.1

				type user

				type team
					relations
						define member: [user]

				type group
					relations
						define member: [user, team#member]

				type document
					relations
						define viewer: [user:*, group#member]`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("group", "member"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("team", "member"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "indirect_relationship_multiple_levels_deep_no_connectivity",
			model: `
				model
					schema 1.1

				type user
				type employee

				type team
					relations
						define member: [employee]

				type group
					relations
						define member: [team#member]

				type document
					relations
						define viewer: [group#member]`,
			target:   typesystem.DirectRelationReference("document", "viewer"),
			source:   typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{},
		},
		{
			name: "edge_through_ttu_on_non-assignable_relation",
			model: `
				model
					schema 1.1

				type organization
					relations
						define viewer: [organization]
						define can_view: viewer

				type document
					relations
						define parent: [organization]
						define view: can_view from parent`,
			target: typesystem.DirectRelationReference("document", "view"),
			source: typesystem.DirectRelationReference("organization", "can_view"),
			expected: []*RelationshipEdge{
				{
					Type:             TupleToUsersetEdge,
					TargetReference:  typesystem.DirectRelationReference("document", "view"),
					TuplesetRelation: "parent",
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "indirect_relation_through_ttu_on_non-assignable_relation",
			model: `
				model
					schema 1.1

				type organization
					relations
						define viewer: [organization]
						define can_view: viewer

				type document
					relations
						define parent: [organization]
						define view: can_view from parent`,
			target: typesystem.DirectRelationReference("document", "view"),
			source: typesystem.DirectRelationReference("organization", "viewer"),
			expected: []*RelationshipEdge{
				{
					Type:            ComputedUsersetEdge,
					TargetReference: typesystem.DirectRelationReference("organization", "can_view"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "ttu_on_non-assignable_relation",
			model: `
				model
					schema 1.1

				type organization
					relations
						define viewer: [organization]
						define can_view: viewer

				type document
					relations
						define parent: [organization]
						define view: can_view from parent`,
			target: typesystem.DirectRelationReference("document", "view"),
			source: typesystem.DirectRelationReference("organization", "can_view"),
			expected: []*RelationshipEdge{
				{
					Type:             TupleToUsersetEdge,
					TargetReference:  typesystem.DirectRelationReference("document", "view"),
					TuplesetRelation: "parent",
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "multiple_indirect_non-assignable_relations_through_ttu",
			model: `
				model
					schema 1.1

				type organization
					relations
						define viewer: [organization]
						define view: viewer

				type folder
					relations
						define parent: [organization]
						define view: view from parent

				type other

				type document
					relations
						define parent: [folder, other]
						define view: view from parent`,
			target: typesystem.DirectRelationReference("document", "view"),
			source: typesystem.DirectRelationReference("organization", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("organization", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "multiple_directly_assignable_relationships_through_unions",
			model: `
				model
					schema 1.1

				type user

				type team
					relations
						define admin: [user]
						define member: [user, team#member] or admin

				type trial
					relations
						define editor: [user, team#member] or owner
						define owner: [user]
						define viewer: [user, team#member] or editor`,
			target: typesystem.DirectRelationReference("trial", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("trial", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("trial", "editor"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("trial", "owner"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("team", "member"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("team", "admin"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "multiple_assignable_and_non-assignable_computed_usersets",
			model: `
				model
					schema 1.1

				type user

				type team
					relations
						define admin: [user]
						define member: [user, team#member] or admin

				type trial
					relations
						define editor: [user, team#member] or owner
						define owner: [user]
						define viewer: [user, team#member] or editor`,
			target: typesystem.DirectRelationReference("trial", "viewer"),
			source: typesystem.DirectRelationReference("team", "admin"),
			expected: []*RelationshipEdge{
				{
					Type:            ComputedUsersetEdge,
					TargetReference: typesystem.DirectRelationReference("team", "member"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "indirect_relationship_through_assignable_computed_userset",
			model: `
				model
					schema 1.1

				type user

				type team
					relations
						define admin: [user]
						define member: [team#member] or admin`,
			target: typesystem.DirectRelationReference("team", "member"),
			source: typesystem.DirectRelationReference("team", "admin"),
			expected: []*RelationshipEdge{
				{
					Type:            ComputedUsersetEdge,
					TargetReference: typesystem.DirectRelationReference("team", "member"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "indirect_relationship_through_non-assignable_computed_userset",
			model: `
				model
					schema 1.1

				type user

				type group
					relations
						define manager: [user]
						define member: manager

				type document
					relations
						define viewer: [group#member]`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("group", "manager"),
			expected: []*RelationshipEdge{
				{
					Type:            ComputedUsersetEdge,
					TargetReference: typesystem.DirectRelationReference("group", "member"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "indirect_relationship_through_non-assignable_ttu_1",
			model: `
				model
					schema 1.1

				type user

				type org
					relations
						define dept: [group]
						define dept_member: member from dept

				type group
					relations
						define member: [user]

				type resource
					relations
						define writer: [org#dept_member]`,
			target: typesystem.DirectRelationReference("resource", "writer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("group", "member"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "indirect_relationship_through_non-assignable_ttu_2",
			model: `
				model
					schema 1.1

				type user

				type org
					relations
						define dept: [group]
						define dept_member: member from dept

				type group
					relations
						define member: [user]

				type resource
					relations
						define writer: [org#dept_member]`,
			target: typesystem.DirectRelationReference("resource", "writer"),
			source: typesystem.DirectRelationReference("group", "member"),
			expected: []*RelationshipEdge{
				{
					Type:             TupleToUsersetEdge,
					TargetReference:  typesystem.DirectRelationReference("org", "dept_member"),
					TuplesetRelation: "dept",
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "indirect_relationship_through_non-assignable_ttu_3",
			model: `
				model
					schema 1.1

				type user

				type org
					relations
						define dept: [group]
						define dept_member: member from dept

				type group
					relations
						define member: [user]

				type resource
					relations
						define writer: [org#dept_member]`,
			target: typesystem.DirectRelationReference("resource", "writer"),
			source: typesystem.DirectRelationReference("org", "dept_member"),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("resource", "writer"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "unrelated_source_and_target_relationship_involving_ttu",
			model: `
				model
					schema 1.1

				type user

				type folder
					relations
						define viewer: [user]

				type document
					relations
						define can_read: viewer from parent
						define parent: [document,folder]
						define viewer: [user]`,
			target:   typesystem.DirectRelationReference("document", "can_read"),
			source:   typesystem.DirectRelationReference("document", ""),
			expected: []*RelationshipEdge{},
		},
		{
			name: "simple_computeduserset_indirect_ref",
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define parent: [document]
						define viewer: [user] or viewer from parent
						define can_view: viewer`,
			target: typesystem.DirectRelationReference("document", "can_view"),
			source: typesystem.DirectRelationReference("document", "viewer"),
			expected: []*RelationshipEdge{
				{
					Type:            ComputedUsersetEdge,
					TargetReference: typesystem.DirectRelationReference("document", "can_view"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
				{
					Type:             TupleToUsersetEdge,
					TargetReference:  typesystem.DirectRelationReference("document", "viewer"),
					TuplesetRelation: "parent",
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "follow_computed_relation_of_ttu_to_computed_userset",
			model: `
				model
					schema 1.1

				type user
				type folder
					relations
						define owner: [user]
						define viewer: [user] or owner
				type document
					relations
						define can_read: viewer from parent
						define parent: [document, folder]
						define viewer: [user]`,
			target: typesystem.DirectRelationReference("document", "can_read"),
			source: typesystem.DirectRelationReference("folder", "owner"),
			expected: []*RelationshipEdge{
				{
					Type:            ComputedUsersetEdge,
					TargetReference: typesystem.DirectRelationReference("folder", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "computed_target_of_ttu_related_to_same_type",
			model: `
				model
					schema 1.1

				type folder
					relations
						define viewer: [folder]

				type document
					relations
						define parent: [folder]
						define viewer: viewer from parent`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("folder", "viewer"),
			expected: []*RelationshipEdge{
				{
					Type:             TupleToUsersetEdge,
					TargetReference:  typesystem.DirectRelationReference("document", "viewer"),
					TuplesetRelation: "parent",
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "basic_relation_with_intersection_1",
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define allowed: [user]
						define viewer: [user] and allowed`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: true,
				},
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "allowed"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "basic_relation_with_intersection_2",
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define allowed: [user]
						define editor: [user]
						define viewer: editor and allowed`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "editor"),
					TargetReferenceInvolvesIntersectionOrExclusion: true,
				},
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "allowed"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "basic_relation_with_intersection_3",
			authModel: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"allowed": typesystem.This(),
							"editor":  typesystem.This(),
							"viewer": typesystem.Intersection(
								typesystem.ComputedUserset("allowed"),
								typesystem.This(),
							),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"allowed": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "user"},
									},
								},
								"editor": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "user"},
									},
								},
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "user"},
									},
								},
							},
						},
					},
				},
			},
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "allowed"),
					TargetReferenceInvolvesIntersectionOrExclusion: true,
				},
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "basic_relation_with_exclusion_1",
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define restricted: [user]
						define viewer: [user] but not restricted`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: true,
				},
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "restricted"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "basic_relation_with_exclusion_2",
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define restricted: [user]
						define editor: [user]
						define viewer: editor but not restricted`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "editor"),
					TargetReferenceInvolvesIntersectionOrExclusion: true,
				},
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "restricted"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "basic_relation_with_exclusion_3",
			authModel: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"allowed": typesystem.This(),
							"viewer": typesystem.Difference(
								typesystem.ComputedUserset("allowed"),
								typesystem.This()),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"allowed": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "user"},
									},
								},
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "user"},
									},
								},
							},
						},
					},
				},
			},
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "allowed"),
					TargetReferenceInvolvesIntersectionOrExclusion: true,
				},
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("document", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "ttu_through_direct_rewrite_1",
			model: `
				model
					schema 1.1

				type folder
					relations
						define viewer: [folder]

				type document
					relations
						define parent: [folder]
						define viewer: viewer from parent`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("folder", "viewer"),
			expected: []*RelationshipEdge{
				{
					Type:             TupleToUsersetEdge,
					TargetReference:  typesystem.DirectRelationReference("document", "viewer"),
					TuplesetRelation: "parent",
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
		{
			name: "ttu_through_direct_rewrite_2",
			model: `
				model
					schema 1.1

				type folder
					relations
						define viewer: [folder]

				type document
					relations
						define parent: [folder]
						define viewer: viewer from parent`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("folder", ""),
			expected: []*RelationshipEdge{
				{
					Type:            DirectEdge,
					TargetReference: typesystem.DirectRelationReference("folder", "viewer"),
					TargetReferenceInvolvesIntersectionOrExclusion: false,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var typesys *typesystem.TypeSystem
			var err error
			if test.model == "" {
				typesys, err = typesystem.New(test.authModel)
			} else {
				model := testutils.MustTransformDSLToProtoWithID(test.model)
				typesys, err = typesystem.New(model)
			}
			require.NoError(t, err)

			g := New(typesys)

			edges, err := g.GetRelationshipEdges(test.target, test.source)
			require.NoError(t, err)

			cmpOpts := []cmp.Option{
				cmpopts.IgnoreUnexported(openfgav1.RelationReference{}),
				RelationshipEdgeTransformer,
			}
			if diff := cmp.Diff(test.expected, edges, cmpOpts...); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResolutionDepthContext(t *testing.T) {
	ctx := ContextWithResolutionDepth(context.Background(), 2)

	depth, ok := ResolutionDepthFromContext(ctx)
	require.True(t, ok)
	require.Equal(t, uint32(2), depth)

	depth, ok = ResolutionDepthFromContext(context.Background())
	require.False(t, ok)
	require.Equal(t, uint32(0), depth)
}
