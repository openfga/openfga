package graph

import (
	"testing"

	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestConnectedObjectGraph_RelationshipIngresss(t *testing.T) {

	tests := []struct {
		name              string
		model             *openfgapb.AuthorizationModel
		targetUserRef     *openfgapb.RelationReference
		targetObjectRef   *openfgapb.RelationReference
		expectedIngresses []*RelationshipIngress
		expectedError     error
	}{
		{
			name: "Direct ingress through ComputedUserset with multiple type restrictions",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
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
			targetUserRef:   typesystem.RelationReference("user", ""),
			targetObjectRef: typesystem.RelationReference("document", "viewer"),
			expectedIngresses: []*RelationshipIngress{
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
			targetUserRef:   typesystem.RelationReference("user", ""),
			targetObjectRef: typesystem.RelationReference("document", "viewer"),
			expectedIngresses: []*RelationshipIngress{
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
			targetUserRef:   typesystem.RelationReference("user", ""),
			targetObjectRef: typesystem.RelationReference("document", "viewer"),
			expectedIngresses: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.RelationReference("document", "viewer"),
				},
				{
					Type:    TupleToUsersetIngress,
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
			targetUserRef:   typesystem.RelationReference("user", ""),
			targetObjectRef: typesystem.RelationReference("document", "viewer"),
			expectedIngresses: []*RelationshipIngress{
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
			targetUserRef:   typesystem.RelationReference("user", ""),
			targetObjectRef: typesystem.RelationReference("team", "member"),
			expectedIngresses: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.RelationReference("group", "member"),
				},
			},
		},
		{
			name: "No graph relationship connectivity",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
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

					{
						Type:      "user",
						Relations: map[string]*openfgapb.Userset{},
						Metadata:  &openfgapb.Metadata{},
					},
				},
			},
			targetUserRef:     typesystem.RelationReference("user", ""),
			targetObjectRef:   typesystem.RelationReference("team", "member"),
			expectedIngresses: []*RelationshipIngress{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			typesys := typesystem.NewTypeSystem(typesystem.SchemaVersion1_1, test.model.TypeDefinitions)

			objGraph := BuildConnectedObjectGraph(typesys)

			ingresses, err := objGraph.RelationshipIngresses(test.targetUserRef, test.targetObjectRef)
			require.ErrorIs(t, err, test.expectedError)

			require.Equal(t, test.expectedIngresses, ingresses)
		})
	}
}
