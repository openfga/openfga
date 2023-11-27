package graph

import (
	"context"
	"sort"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
)

var (
	RelationshipEdgeTransformer = cmp.Transformer("Sort", func(in []*RelationshipEdge) []*RelationshipEdge {
		out := append([]*RelationshipEdge(nil), in...) // Copy input to avoid mutating it

		// Sort by Type and then by edge and then by tupleset relation
		sort.SliceStable(out, func(i, j int) bool {
			if out[i].Type > out[j].Type {
				return false
			}

			if typesystem.GetRelationReferenceAsString(out[i].TargetReference) > typesystem.GetRelationReferenceAsString(out[j].TargetReference) {
				return false
			}

			if out[i].TuplesetRelation > out[j].TuplesetRelation {
				return false
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
			type user

			type document
			  relations
			    define allowed: [user] as self
			    define viewer: [user] as self and allowed
			`,
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
			type user

			type folder
			  relations
			    define allowed: [user] as self
			    define viewer: [user] as self and allowed

			type document
			  relations
			    define parent: [folder] as self
			    define viewer as viewer from parent
			`,
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
			type user

			type organization
			  relations
			    define allowed: [user] as self
			    define viewer: [user] as self and allowed

			type folder
			  relations
			    define parent: [organization] as self
			    define viewer as viewer from parent

			type document
			  relations
			    define parent: [folder] as self
			    define viewer as viewer from parent
			`,
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
			type user

			type folder
			  relations
			    define writer: [user] as self
			    define editor: [user] as self
			    define viewer as writer but not editor

			type document
			  relations
			    define parent: [folder] as self
			    define viewer as viewer from parent
			`,
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
			type user

			type folder
			  relations
			    define writer: [user] as self
			    define editor: [user] as self
			    define viewer as writer but not editor

			type document
			  relations
			    define parent: [folder] as self
			    define viewer as viewer from parent
			`,
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
			  type user
	
			  type repo
				relations
				  define admin: [user] as self or repo_admin from owner
				  define owner: [organization] as self
	
			  type organization
				relations
				  define member: [user] as self or owner
				  define owner: [user] as self
				  define repo_admin: [user, organization#member] as self
			`,
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
			typedefs := parser.MustParse(test.model)
			typesys := typesystem.New(&openfgav1.AuthorizationModel{
				SchemaVersion:   typesystem.SchemaVersion1_1,
				TypeDefinitions: typedefs,
			})

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
		name     string
		model    string
		target   *openfgav1.RelationReference
		source   *openfgav1.RelationReference
		expected []*RelationshipEdge
	}{
		{
			name: "direct_edge_through_ComputedUserset_with_multiple_type_restrictions",
			model: `
			type user

			type group
			  relations
			    define member: [user, group#member] as self

			type document
			  relations
			    define editor: [user, group#member] as self
			    define viewer as editor
			`,
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
			type user

			type document
			  relations
			    define editor: [user] as self
			    define viewer as editor
			`,
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
			type user

			type group
			  relations
			    define member: [user] as self

			type folder
			  relations
			    define viewer: [user, group#member] as self

			type document
			  relations
			    define parent: [folder] as self
			    define viewer: [user] as self or viewer from parent
			`,
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
			type user

			type group
			  relations
			    define member: [user, group#member] as self

			type document
			  relations
			    define editor: [user, group#member] as self
			    define viewer: [user] as self or editor
			`,
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
			type user

			type team
			  relations
			    define member: [group#member] as self

			type group
			  relations
			    define member: [user, team#member] as self
			`,
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
			type user

			type folder
			  relations
			    define parent: [folder] as self
			    define viewer: [user] as self or viewer from parent
			`,
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
			type user

			type team
			  relations
			    define member: [team#member] as self
			`,
			target:   typesystem.DirectRelationReference("team", "member"),
			source:   typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{},
		},
		{
			name: "test1",
			model: `
			type user

			type group
			  relations
			    define member: [user] as self

			type folder
			  relations
			    define viewer: [user, group#member] as self

			type document
			  relations
			    define parent: [folder] as self
			    define viewer as viewer from parent
			`,
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
			type user

			type group
			  relations
			    define member: [user] as self

			type folder
			  relations
			    define viewer: [user, group#member] as self

			type document
			  relations
			    define parent: [folder] as self
			    define viewer as viewer from parent
			`,
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
			type user

			type folder
			  relations
			    define viewer: [user] as self

			type document
			  relations
			    define parent: [folder] as self
			    define viewer as viewer from parent
			`,
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
			type user
			type organization

			type folder
			  relations
			    define viewer: [user] as self

			type document
			  relations
			    define parent: [folder, organization] as self
			    define viewer as viewer from parent
			`,
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
			type user

			type group
			  relations
			    define member: [user, group#member] as self
			`,
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
			type organization
			  relations
			    define viewer: [organization] as self
			    define can_view as viewer

			type document
			  relations
			    define parent: [organization] as self
			    define view as can_view from parent
			`,
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
			type user

			type document
			  relations
			    define viewer: [user:*] as self
			`,
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
			type user

			type document
			  relations
			    define viewer: [user] as self
			`,
			target:   typesystem.DirectRelationReference("document", "viewer"),
			source:   typesystem.WildcardRelationReference("user"),
			expected: []*RelationshipEdge{},
		},
		{
			name: "user_*_is_related_to_user_*",
			model: `
			type user

			type document
			  relations
			    define viewer: [user:*] as self
			`,
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
			type user

			type document
			  relations
			    define editor: [user:*] as self
			    define viewer as editor
			`,
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
			type user

			type document
			  relations
			    define editor: [user] as self
			    define viewer as editor
			`,
			target:   typesystem.DirectRelationReference("document", "viewer"),
			source:   typesystem.WildcardRelationReference("user"),
			expected: []*RelationshipEdge{},
		},
		{
			name: "edges_involving_wildcards_1",
			model: `
			type user
			type employee
			type group

			type document
			  relations
			    define relation1: [user:*] as self or relation2 or relation3 or relation4
			    define relation2: [group:*] as self
			    define relation3: [employee:*] as self
			    define relation4: [user] as self
			`,
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
			type user

			type document
			  relations
			    define relation1: [user] as self or relation2
			    define relation2: [user:*] as self
			`,
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
			type user

			type group
			  relations
			    define member: [user:*] as self

			type document
			  relations
			    define viewer: [group#member] as self
			`,
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
			type user

			type team
			  relations
			    define member: [user] as self

			type group
			  relations
			    define member: [user, team#member] as self

			type document
			  relations
			    define viewer: [user:*, group#member] as self
			`,
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
			type user
			type employee

			type team
			  relations
			    define member: [employee] as self

			type group
			  relations
			    define member: [team#member] as self

			type document
			  relations
			    define viewer: [group#member] as self
			`,
			target:   typesystem.DirectRelationReference("document", "viewer"),
			source:   typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipEdge{},
		},
		{
			name: "edge_through_ttu_on_non-assignable_relation",
			model: `
			type organization
			  relations
			    define viewer: [organization] as self
			    define can_view as viewer

			type document
			  relations
			    define parent: [organization] as self
			    define view as can_view from parent
			`,
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
			type organization
			  relations
			    define viewer: [organization] as self
			    define can_view as viewer

			type document
			  relations
			    define parent: [organization] as self
			    define view as can_view from parent
			`,
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
			type organization
			  relations
			    define viewer: [organization] as self
			    define can_view as viewer

			type document
			  relations
			    define parent: [organization] as self
			    define view as can_view from parent
			`,
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
			type organization
			  relations
			    define viewer: [organization] as self
			    define view as viewer

			type folder
			  relations
			    define parent: [organization] as self
			    define view as view from parent

			type other

			type document
			  relations
			    define parent: [folder, other] as self
			    define view as view from parent
			`,
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
			type user

			type team
			  relations
			    define admin: [user] as self
			    define member: [user, team#member] as self or admin

			type trial
			  relations
			    define editor: [user, team#member] as self or owner
			    define owner: [user] as self
			    define viewer: [user, team#member] as self or editor
			`,
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
			type user

			type team
			  relations
			    define admin: [user] as self
			    define member: [user, team#member] as self or admin

			type trial
			  relations
			    define editor: [user, team#member] as self or owner
			    define owner: [user] as self
			    define viewer: [user, team#member] as self or editor
			`,
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
			type user

			type team
			  relations
			    define admin: [user] as self
			    define member: [team#member] as self or admin
			`,
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
			type user

			type group
			  relations
			    define manager: [user] as self
			    define member as manager

			type document
			  relations
			    define viewer: [group#member] as self
			`,
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
			type user

			type org
			  relations
			    define dept: [group] as self
			    define dept_member as member from dept

			type group
			  relations
			    define member: [user] as self

			type resource
			  relations
			    define writer: [org#dept_member] as self
			`,
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
			type user

			type org
			  relations
			    define dept: [group] as self
			    define dept_member as member from dept

			type group
			  relations
			    define member: [user] as self

			type resource
			  relations
			    define writer: [org#dept_member] as self
			`,
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
			type user

			type org
			  relations
			    define dept: [group] as self
			    define dept_member as member from dept

			type group
			  relations
			    define member: [user] as self

			type resource
			  relations
			    define writer: [org#dept_member] as self
			`,
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
			type user

			type folder
				relations
					define viewer: [user] as self

			type document
				relations
					define can_read as viewer from parent
					define parent: [document,folder] as self
					define viewer: [user] as self
			`,
			target:   typesystem.DirectRelationReference("document", "can_read"),
			source:   typesystem.DirectRelationReference("document", ""),
			expected: []*RelationshipEdge{},
		},
		{
			name: "simple_computeduserset_indirect_ref",
			model: `
			type user

			type document
			  relations
			    define parent: [document] as self
			    define viewer: [user] as self or viewer from parent
				define can_view as viewer
			`,
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
			type user
			type folder
			  relations
				define owner: [user] as self
				define viewer: [user] as self or owner
			type document
			  relations
				define can_read as viewer from parent
				define parent: [document, folder] as self
				define viewer: [user] as self
			`,
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
			type folder
			  relations
				define viewer: [folder] as self

			type document
			  relations
				define parent: [folder] as self
				define viewer as viewer from parent
			`,
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
			type user

			type document
			  relations
				define allowed: [user] as self
				define viewer: [user] as self and allowed
			`,
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
			type user

			type document
			  relations
				define allowed: [user] as self
				define editor: [user] as self
				define viewer as editor and allowed
			`,
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
			model: `
			type user

			type document
			  relations
				define allowed: [user] as self
				define editor: [user] as self
				define viewer: [user] as allowed and self
			`,
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
			type user

			type document
			  relations
				define restricted: [user] as self
				define viewer: [user] as self but not restricted
			`,
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
			type user

			type document
			  relations
				define restricted: [user] as self
				define editor: [user] as self
				define viewer as editor but not restricted
			`,
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
			model: `
			type user

			type document
			  relations
				define allowed: [user] as self
				define viewer: [user] as allowed but not self
			`,
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
			type folder
				relations
				define viewer: [folder] as self
		
			type document
				relations
				define parent: [folder] as self
				define viewer as viewer from parent
			`,
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
			type folder
				relations
				define viewer: [folder] as self
		
			type document
				relations
				define parent: [folder] as self
				define viewer as viewer from parent
			`,
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
			typedefs := parser.MustParse(test.model)
			typesys := typesystem.New(&openfgav1.AuthorizationModel{
				SchemaVersion:   typesystem.SchemaVersion1_1,
				TypeDefinitions: typedefs,
			})

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
