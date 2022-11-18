package graph

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestConnectedObjectGraph_RelationshipIngresss(t *testing.T) {

	tests := []struct {
		name     string
		model    *openfgapb.AuthorizationModel
		target   *openfgapb.RelationReference
		source   *openfgapb.RelationReference
		expected []*RelationshipIngress
	}{
		{
			name: "Direct ingress through ComputedUserset with multiple type restrictions",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"editor": typesystem.This(),
							"viewer": typesystem.ComputedUserset("editor"),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"editor": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
										typesystem.DirectRelationReference("group", "member"),
									},
								},
							},
						},
					},
					{
						Type: "group",
						Relations: map[string]*openfgapb.Userset{
							"member": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
										typesystem.DirectRelationReference("group", "member"),
									},
								},
							},
						},
					},
				},
			},
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
			name: "Direct ingress through ComputedUserset",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"editor": typesystem.This(),
							"viewer": typesystem.ComputedUserset("editor"),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"editor": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
				},
			},
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
			name: "Direct ingress through TupleToUserset with multiple type restrictions",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
						Relations: map[string]*openfgapb.Userset{
							"member": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
										typesystem.DirectRelationReference("group", "member"),
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.Union(
								typesystem.This(),
								typesystem.TupleToUserset("parent", "viewer"),
							),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("folder", ""),
									},
								},
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
				},
			},
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
			name: "Direct ingress with union involving self and computed userset",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"editor": typesystem.This(),
							"viewer": typesystem.Union(
								typesystem.This(),
								typesystem.ComputedUserset("editor")),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"editor": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
										typesystem.DirectRelationReference("group", "member"),
									},
								},
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
					{
						Type: "group",
						Relations: map[string]*openfgapb.Userset{
							"member": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
										typesystem.DirectRelationReference("group", "member"),
									},
								},
							},
						},
					},
				},
			},
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
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "team",
						Relations: map[string]*openfgapb.Userset{
							"member": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("group", "member"),
									},
								},
							},
						},
					},

					{
						Type: "group",
						Relations: map[string]*openfgapb.Userset{
							"member": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
										typesystem.DirectRelationReference("team", "member"),
									},
								},
							},
						},
					},
				},
			},
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
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.Union(
								typesystem.This(),
								typesystem.TupleToUserset("parent", "viewer"),
							),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("folder", ""),
									},
								},
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
				},
			},
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
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "team",
						Relations: map[string]*openfgapb.Userset{
							"member": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("team", "member"),
									},
								},
							},
						},
					},
				},
			},
			target:   typesystem.DirectRelationReference("team", "member"),
			source:   typesystem.DirectRelationReference("user", ""),
			expected: []*RelationshipIngress{},
		},
		{
			name: "Test1",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
						Relations: map[string]*openfgapb.Userset{
							"member": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
										typesystem.DirectRelationReference("group", "member"),
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.TupleToUserset("parent", "viewer"),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("folder", ""),
									},
								},
							},
						},
					},
				},
			},
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
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
						Relations: map[string]*openfgapb.Userset{
							"member": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
										typesystem.DirectRelationReference("group", "member"),
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.TupleToUserset("parent", "viewer"),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("folder", ""),
									},
								},
							},
						},
					},
				},
			},
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
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
						Relations: map[string]*openfgapb.Userset{
							"member": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
										typesystem.DirectRelationReference("group", "member"),
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.TupleToUserset("parent", "viewer"),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("folder", ""),
									},
								},
							},
						},
					},
				},
			},
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
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "organization",
					},
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.TupleToUserset("parent", "viewer"),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("folder", ""),
										typesystem.DirectRelationReference("organization", ""),
									},
								},
							},
						},
					},
				},
			},
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
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
						Relations: map[string]*openfgapb.Userset{
							"member": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
										typesystem.DirectRelationReference("group", "member"),
									},
								},
							},
						},
					},
				},
			},
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
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "organization",
						Relations: map[string]*openfgapb.Userset{
							"viewer":   typesystem.This(),
							"can_view": typesystem.ComputedUserset("viewer"),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("organization", ""),
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"view":   typesystem.TupleToUserset("parent", "can_view"),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("organization", "viewer"),
									},
								},
							},
						},
					},
				},
			},
			target: typesystem.DirectRelationReference("document", "view"),
			source: typesystem.DirectRelationReference("organization", ""),
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("organization", "viewer"),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			typesys := typesystem.New(test.model)

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
