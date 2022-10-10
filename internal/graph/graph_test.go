package graph

import (
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
										typesystem.RelationReference("user", ""),
										typesystem.RelationReference("group", "member"),
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
										typesystem.RelationReference("user", ""),
										typesystem.RelationReference("group", "member"),
									},
								},
							},
						},
					},
				},
			},
			target: typesystem.RelationReference("document", "viewer"),
			source: typesystem.RelationReference("user", ""),
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.RelationReference("document", "editor"),
				},
				{
					Type:    DirectIngress,
					Ingress: typesystem.RelationReference("group", "member"),
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
										typesystem.RelationReference("user", ""),
									},
								},
							},
						},
					},
				},
			},
			target: typesystem.RelationReference("document", "viewer"),
			source: typesystem.RelationReference("user", ""),
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.RelationReference("document", "editor"),
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
										typesystem.RelationReference("user", ""),
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
										typesystem.RelationReference("user", ""),
										typesystem.RelationReference("group", "member"),
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
										typesystem.RelationReference("folder", ""),
									},
								},
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.RelationReference("user", ""),
									},
								},
							},
						},
					},
				},
			},
			target: typesystem.RelationReference("document", "viewer"),
			source: typesystem.RelationReference("user", ""),
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.RelationReference("document", "viewer"),
				},
				{
					Type:    DirectIngress,
					Ingress: typesystem.RelationReference("folder", "viewer"),
				},
				{
					Type:    DirectIngress,
					Ingress: typesystem.RelationReference("group", "member"),
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
										typesystem.RelationReference("user", ""),
										typesystem.RelationReference("group", "member"),
									},
								},
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.RelationReference("user", ""),
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
										typesystem.RelationReference("user", ""),
										typesystem.RelationReference("group", "member"),
									},
								},
							},
						},
					},
				},
			},
			target: typesystem.RelationReference("document", "viewer"),
			source: typesystem.RelationReference("user", ""),
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.RelationReference("document", "viewer"),
				},
				{
					Type:    DirectIngress,
					Ingress: typesystem.RelationReference("document", "editor"),
				},
				{
					Type:    DirectIngress,
					Ingress: typesystem.RelationReference("group", "member"),
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
										typesystem.RelationReference("group", "member"),
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
										typesystem.RelationReference("user", ""),
										typesystem.RelationReference("team", "member"),
									},
								},
							},
						},
					},
				},
			},
			target: typesystem.RelationReference("team", "member"),
			source: typesystem.RelationReference("user", ""),
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.RelationReference("group", "member"),
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
										typesystem.RelationReference("folder", ""),
									},
								},
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.RelationReference("user", ""),
									},
								},
							},
						},
					},
				},
			},
			target: typesystem.RelationReference("folder", "viewer"),
			source: typesystem.RelationReference("user", ""),
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.RelationReference("folder", "viewer"),
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
										typesystem.RelationReference("team", "member"),
									},
								},
							},
						},
					},
				},
			},
			target:   typesystem.RelationReference("team", "member"),
			source:   typesystem.RelationReference("user", ""),
			expected: []*RelationshipIngress{},
		},
		{
			name: "",
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
										typesystem.RelationReference("user", ""),
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
										typesystem.RelationReference("user", ""),
										typesystem.RelationReference("group", "member"),
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
										typesystem.RelationReference("folder", ""),
									},
								},
							},
						},
					},
				},
			},
			target: typesystem.RelationReference("document", "viewer"),
			source: typesystem.RelationReference("user", ""),
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.RelationReference("folder", "viewer"),
				},
				{
					Type:    DirectIngress,
					Ingress: typesystem.RelationReference("group", "member"),
				},
			},
		},
		{
			name: "",
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
										typesystem.RelationReference("user", ""),
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
										typesystem.RelationReference("user", ""),
										typesystem.RelationReference("group", "member"),
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
										typesystem.RelationReference("folder", ""),
									},
								},
							},
						},
					},
				},
			},
			target: typesystem.RelationReference("document", "viewer"),
			source: typesystem.RelationReference("group", "member"),
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.RelationReference("folder", "viewer"),
				},
			},
		},
		{
			name: "",
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
										typesystem.RelationReference("user", ""),
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
										typesystem.RelationReference("user", ""),
										typesystem.RelationReference("group", "member"),
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
										typesystem.RelationReference("folder", ""),
									},
								},
							},
						},
					},
				},
			},
			target: typesystem.RelationReference("document", "viewer"),
			source: typesystem.RelationReference("folder", "viewer"),
			expected: []*RelationshipIngress{
				{
					Type:             TupleToUsersetIngress,
					Ingress:          typesystem.RelationReference("document", "viewer"),
					TuplesetRelation: typesystem.RelationReference("document", "parent"),
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
										typesystem.RelationReference("user", ""),
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
										typesystem.RelationReference("folder", ""),
										typesystem.RelationReference("organization", ""),
									},
								},
							},
						},
					},
				},
			},
			target: typesystem.RelationReference("document", "viewer"),
			source: typesystem.RelationReference("user", ""),
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.RelationReference("folder", "viewer"),
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
										typesystem.RelationReference("user", ""),
										typesystem.RelationReference("group", "member"),
									},
								},
							},
						},
					},
				},
			},
			target: typesystem.RelationReference("group", "member"),
			source: typesystem.RelationReference("user", ""),
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.RelationReference("group", "member"),
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

			//for _, i := range ingresses {
			//	fmt.Println(">>>", i)
			//}

			require.ElementsMatch(t, test.expected, ingresses)
		})
	}
}
