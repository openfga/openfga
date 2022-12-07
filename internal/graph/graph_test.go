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
			name: "direct_ingress_through_ComputedUserset_with_multiple_type_restrictions",
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
			name: "direct_ingress_through_ComputedUserset",
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
			name: "direct_ingress_through_TupleToUserset_with_multiple_type_restrictions",
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
			name: "direct_ingress_with_union_involving_self_and_computed_userset",
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
		{
			name: "user_is_a_subset_of_user_*",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.WildcardRelationReference("user"),
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
			},
		},
		{
			name: "user_*_is_not_a_subset_of_user",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
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
				},
			},
			target:   typesystem.DirectRelationReference("document", "viewer"),
			source:   typesystem.WildcardRelationReference("user"),
			expected: []*RelationshipIngress{},
		},
		{
			name: "user_*_is_related_to_user_*",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.WildcardRelationReference("user"),
									},
								},
							},
						},
					},
				},
			},
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
										typesystem.WildcardRelationReference("user"),
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
			name: "ingresses_involving_wildcard_in_source",
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
			target:   typesystem.DirectRelationReference("document", "viewer"),
			source:   typesystem.WildcardRelationReference("user"),
			expected: []*RelationshipIngress{},
		},
		{
			name: "ingresses_involving_wildcards_1",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "employee",
					},
					{
						Type: "group",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"relation1": typesystem.Union(typesystem.This(), typesystem.ComputedUserset("relation2"), typesystem.ComputedUserset("relation3"), typesystem.ComputedUserset("relation4")),
							"relation2": typesystem.This(),
							"relation3": typesystem.This(),
							"relation4": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"relation1": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.WildcardRelationReference("user"),
									},
								},
								"relation2": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.WildcardRelationReference("group"),
									},
								},
								"relation3": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.WildcardRelationReference("employee"),
									},
								},
								"relation4": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
				},
			},
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
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"relation1": typesystem.Union(typesystem.This(), typesystem.ComputedUserset("relation2")),
							"relation2": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"relation1": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
									},
								},
								"relation2": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.WildcardRelationReference("user"),
									},
								},
							},
						},
					},
				},
			},
			target: typesystem.DirectRelationReference("document", "relation1"),
			source: typesystem.WildcardRelationReference("user"),
			expected: []*RelationshipIngress{
				{
					Type:    DirectIngress,
					Ingress: typesystem.DirectRelationReference("document", "relation2"),
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
