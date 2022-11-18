package validation

import (
	"testing"

	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestValidateTuple(t *testing.T) {

	tests := []struct {
		name        string
		tuple       *openfgapb.TupleKey
		model       *openfgapb.AuthorizationModel
		expectError bool
	}{
		{
			name:        "Malformed Object 1",
			tuple:       tuple.NewTupleKey("group#group1:member", "relation", "user:jon"),
			expectError: true,
		},
		{
			name:        "Malformed Object 2",
			tuple:       tuple.NewTupleKey("repo:sand castle", "relation", "user:jon"),
			expectError: true,
		},
		{
			name:        "Malformed Object 3",
			tuple:       tuple.NewTupleKey("fga", "relation", "user:jon"),
			expectError: true,
		},
		{
			name:        "Malformed Object 4",
			tuple:       tuple.NewTupleKey("github:org-iam#member", "relation", "user:jon"),
			expectError: true,
		},
		{
			name:        "Malformed Object 5",
			tuple:       tuple.NewTupleKey("group:group:group", "relation", "user:jon"),
			expectError: true,
		},
		{
			name:        "Malformed Object 6",
			tuple:       tuple.NewTupleKey(":", "relation", "user:jon"),
			expectError: true,
		},
		{
			name:        "Malformed Relation 1",
			tuple:       tuple.NewTupleKey("document:1", "group#group", "user:jon"),
			expectError: true,
		},
		{
			name:        "Malformed Relation 2",
			tuple:       tuple.NewTupleKey("document:1", "organization:openfga", "user:jon"),
			expectError: true,
		},
		{
			name:        "Malformed Relation 3",
			tuple:       tuple.NewTupleKey("document:1", "my relation", "user:jon"),
			expectError: true,
		},
		{
			name:        "Malformed User 1",
			tuple:       tuple.NewTupleKey("document:1", "relation", "john:albert:doe"),
			expectError: true,
		},
		{
			name:        "Malformed User 2",
			tuple:       tuple.NewTupleKey("document:1", "relation", "john#albert#doe"),
			expectError: true,
		},
		{
			name:        "Malformed User 3",
			tuple:       tuple.NewTupleKey("document:1", "relation", "invalid#test:go"),
			expectError: true,
		},
		{
			name:        "Malformed User 4",
			tuple:       tuple.NewTupleKey("document:1", "relation", "anne@openfga .com"),
			expectError: true,
		},
		{
			name:  "Malformed User 5 (Invalid user for 1.1 model)",
			tuple: tuple.NewTupleKey("document:1", "viewer", "anne"), // user must be 'object' or 'object#relation' in 1.1 models
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"viewer": typesystem.This(),
						},
					},
				},
			},
			expectError: true,
		},
		{
			name:  "Undefined user type (1.1 model)",
			tuple: tuple.NewTupleKey("document:1", "viewer", "employee:anne"),
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
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
										{Type: "user"},
									},
								},
							},
						},
					},
				},
			},
			expectError: true,
		},
		{
			name:  "Undefined user type in userset value (1.1 model)",
			tuple: tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
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
										{Type: "user"},
									},
								},
							},
						},
					},
				},
			},
			expectError: true,
		},
		{
			name:  "Undefined userset relation in userset value (1.1 model)",
			tuple: tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
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
										{Type: "user"},
									},
								},
							},
						},
					},
				},
			},
			expectError: true,
		},
		{
			name:  "Untyped wildcard (1.0 model)",
			tuple: tuple.NewTupleKey("document:1", "viewer", "*"),
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"viewer": typesystem.This(),
						},
					},
				},
			},
			expectError: false,
		},
		{
			name:  "Typed wildcard with undefined object type",
			tuple: tuple.NewTupleKey("document:1", "viewer", "employee:*"),
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
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
										{Type: "user"},
									},
								},
							},
						},
					},
				},
			},
			expectError: true,
		},
		{
			name:  "Untyped wildcard in 1.1 model",
			tuple: tuple.NewTupleKey("document:1", "viewer", "*"),
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
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
										{Type: "user"},
									},
								},
							},
						},
					},
				},
			},
			expectError: true,
		},
		{
			name:  "Typed wildcard with valid object type in 1.1 model",
			tuple: tuple.NewTupleKey("document:1", "viewer", "user:*"),
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
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
										{Type: "user"},
										typesystem.WildcardRelationReference("user"),
									},
								},
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name:  "Incorrect_User_Object_Reference_in_Tupleset_Relation",
			tuple: tuple.NewTupleKey("document:1", "parent", "someuser"),
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.TupleToUserset("parent", "viewer"),
						},
					},
				},
			},
			expectError: true,
		},
		{
			name:  "Wildcard (User) value in Tupleset Relation",
			tuple: tuple.NewTupleKey("document:1", "parent", "*"),
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.TupleToUserset("parent", "viewer"),
						},
					},
				},
			},
			expectError: true,
		},
		{
			name:  "Userset (User) value in Tupleset Relation",
			tuple: tuple.NewTupleKey("document:1", "ancestor", "folder:1#parent"),
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"ancestor": typesystem.This(),
							"viewer":   typesystem.TupleToUserset("ancestor", "viewer"),
						},
					},
				},
			},
			expectError: true,
		},
		{
			name:  "Typed wildcard (User) value in Tupleset Relation (1.1 models)",
			tuple: tuple.NewTupleKey("document:1", "parent", "folder:*"),
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
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
										{Type: "user"},
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
										{Type: "folder"},
									},
								},
							},
						},
					},
				},
			},
			expectError: true,
		},
		{
			name:  "Tupleset relation involving rewrite returns error",
			tuple: tuple.NewTupleKey("document:1", "parent", "folder:1"),
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"viewer": typesystem.This(),
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.ComputedUserset("editor"),
							"editor": typesystem.This(),
							"viewer": typesystem.TupleToUserset("parent", "viewer"),
						},
					},
				},
			},
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			err := ValidateTuple(typesystem.New(test.model), test.tuple)

			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
