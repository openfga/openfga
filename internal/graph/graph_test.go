package graph

import (
	"context"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestConnectedObjectGraph_RelationshipIngresss(t *testing.T) {

	tests := []struct {
		name     string
		model    string
		target   *openfgapb.RelationReference
		source   *openfgapb.RelationReference
		expected []*RelationshipIngress
	}{
		{
			name: "direct_ingress_through_ComputedUserset_with_multiple_type_restrictions",
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
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("document", "editor"),
				},
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("group", "member"),
				},
			},
		},
		{
			name: "direct_ingress_through_ComputedUserset",
			model: `
			type user

			type document
			  relations
			    define editor: [user] as self
				define viewer as editor
			`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("document", "editor"),
				},
			},
		},
		{
			name: "direct_ingress_through_TupleToUserset_with_multiple_type_restrictions",
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
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("document", "viewer"),
				},
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("folder", "viewer"),
				},
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("group", "member"),
				},
			},
		},
		{
			name: "direct_ingress_with_union_involving_self_and_computed_userset",
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
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("document", "viewer"),
				},
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("document", "editor"),
				},
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("group", "member"),
				},
			},
		},
		{
			name: "Circular reference",
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
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("group", "member"),
				},
			},
		},
		{
			name: "Cyclical parent/child definition",
			model: `
			type user

			type folder
			  relations
			    define parent: [folder] as self
				define viewer: [user] as self or viewer from parent
			`,
			target: typesystem.DirectRelationReference("folder", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("folder", "viewer"),
				},
			},
		},
		{
			name: "No graph relationship connectivity",
			model: `
			type user

			type team
			  relations
			    define member: [team#member] as self
			`,
			target:   typesystem.DirectRelationReference("team", "member"),
			source:   typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipIngress{},
		},
		{
			name: "Test1",
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
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("folder", "viewer"),
				},
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("group", "member"),
				},
			},
		},
		{
			name: "Test2",
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
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("folder", "viewer"),
				},
			},
		},
		{
			name: "Test3",
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
			source: typesystem.DirectRelationReference("folder", "viewer"),
			expected: []*RelationshipIngress{
				{
					Type:             TupleToUsersetIngress,
					Ingress:          typesystem.DirectRelationReference("document", "viewer"),
					TuplesetRelation: typesystem.DirectRelationReference("document", "parent"),
				},
			},
		},
		{
			name: "Undefined relation on one type involved in a tuple to userset",
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
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("folder", "viewer"),
				},
			},
		},
		{
			name: "Nested group membership returns only top-level ingress",
			model: `
			type user

			type group
			  relations
			    define member: [user, group#member] as self
			`,
			target: typesystem.DirectRelationReference("group", "member"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("group", "member"),
				},
			},
		},
		{
			name: "Ingresses for non-assignable relation",
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
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("organization", "viewer"),
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
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("document", "viewer"),
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
			expected: []*RelationshipIngress{},
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
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("document", "viewer"),
				},
			},
		},
		{
			name: "ingresses_involving_wildcard_in_types",
			model: `
			type user

			type document
			  relations
			    define editor: [user:*] as self
				define viewer as editor
			`,
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("document", "editor"),
				},
			},
		},
		{
			name: "ingresses_involving_wildcard_in_source",
			model: `
			type user

			type document
			  relations
			    define editor: [user] as self
				define viewer as editor
			`,
			target:   typesystem.DirectRelationReference("document", "viewer"),
			source:   typesystem.WildcardRelationReference("user"),
			expected: []*RelationshipIngress{},
		},
		{
			name: "ingresses_involving_wildcards_1",
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
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("document", "relation1"),
				},
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("document", "relation4"),
				},
			},
		},
		{
			name: "ingresses_involving_wildcards_2",
			model: `
			type user

			type document
			  relations
			    define relation1: [user] as self or relation2
				define relation2: [user:*] as self
			`,
			target: typesystem.DirectRelationReference("document", "relation1"),
			source: typesystem.WildcardRelationReference("user"),
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("document", "relation2"),
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
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("group", "member"),
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
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("document", "viewer"),
				},
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("group", "member"),
				},
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("team", "member"),
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
			target: typesystem.DirectRelationReference("document", "viewer"),
			source: typesystem.DirectRelationReference("user", ""),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			typedefs := parser.MustParse(test.model)
			typesys := typesystem.New(&openfgapb.AuthorizationModel{
				SchemaVersion:   typesystem.SchemaVersion1_1,
				TypeDefinitions: typedefs,
			})

			g := BuildConnectedObjectGraph(typesys)

			ingresses, err := g.RelationshipIngresses(test.target, test.source)
			require.NoError(t, err)

			require.ElementsMatch(t, test.expected, ingresses)
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
