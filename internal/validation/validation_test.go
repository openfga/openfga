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
		name          string
		tuple         *openfgapb.TupleKey
		model         *openfgapb.AuthorizationModel
		expectedError error
	}{
		{
			name:  "malformed_object_1",
			tuple: tuple.NewTupleKey("group#group1:member", "relation", "user:jon"),
			expectedError: &tuple.InvalidObjectFormatError{
				TupleKey: tuple.NewTupleKey("group#group1:member", "relation", "user:jon"),
			},
		},
		{
			name:  "malformed_object_2",
			tuple: tuple.NewTupleKey("repo:sand castle", "relation", "user:jon"),
			expectedError: &tuple.InvalidObjectFormatError{
				TupleKey: tuple.NewTupleKey("repo:sand castle", "relation", "user:jon"),
			},
		},
		{
			name:  "malformed_object_3",
			tuple: tuple.NewTupleKey("fga", "relation", "user:jon"),
			expectedError: &tuple.InvalidObjectFormatError{
				TupleKey: tuple.NewTupleKey("fga", "relation", "user:jon"),
			},
		},
		{
			name:  "malformed_object_4",
			tuple: tuple.NewTupleKey("github:org-iam#member", "relation", "user:jon"),
			expectedError: &tuple.InvalidObjectFormatError{
				TupleKey: tuple.NewTupleKey("github:org-iam#member", "relation", "user:jon"),
			},
		},
		{
			name:  "malformed_object_5",
			tuple: tuple.NewTupleKey("group:group:group", "relation", "user:jon"),
			expectedError: &tuple.InvalidObjectFormatError{
				TupleKey: tuple.NewTupleKey("group:group:group", "relation", "user:jon"),
			},
		},
		{
			name:  "malformed_object_6",
			tuple: tuple.NewTupleKey(":", "relation", "user:jon"),
			expectedError: &tuple.InvalidObjectFormatError{
				TupleKey: tuple.NewTupleKey(":", "relation", "user:jon"),
			},
		},
		{
			name:  "malformed_relation_1",
			tuple: tuple.NewTupleKey("document:1", "group#group", "user:jon"),
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{Type: "document"},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Reason:   "invalid relation",
				TupleKey: tuple.NewTupleKey("document:1", "group#group", "user:jon"),
			},
		},
		{
			name:  "malformed_relation_2",
			tuple: tuple.NewTupleKey("document:1", "organization:openfga", "user:jon"),
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{Type: "document"},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Reason:   "invalid relation",
				TupleKey: tuple.NewTupleKey("document:1", "organization:openfga", "user:jon"),
			},
		},
		{
			name:  "malformed_relation_3",
			tuple: tuple.NewTupleKey("document:1", "my relation", "user:jon"),
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{Type: "document"},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Reason:   "invalid relation",
				TupleKey: tuple.NewTupleKey("document:1", "my relation", "user:jon"),
			},
		},
		{
			name:  "malformed_user_1",
			tuple: tuple.NewTupleKey("document:1", "relation", "john:albert:doe"),
			expectedError: &tuple.InvalidTupleError{
				Reason:   "the 'user' field is invalid",
				TupleKey: tuple.NewTupleKey("document:1", "relation", "john:albert:doe"),
			},
		},
		{
			name:  "malformed_user_2",
			tuple: tuple.NewTupleKey("document:1", "relation", "john#albert#doe"),
			expectedError: &tuple.InvalidTupleError{
				Reason:   "the 'user' field is invalid",
				TupleKey: tuple.NewTupleKey("document:1", "relation", "john#albert#doe"),
			},
		},
		{
			name:  "malformed_user_3",
			tuple: tuple.NewTupleKey("document:1", "relation", "invalid#test:go"),
			expectedError: &tuple.InvalidTupleError{
				Reason:   "the 'user' field is invalid",
				TupleKey: tuple.NewTupleKey("document:1", "relation", "invalid#test:go"),
			},
		},
		{
			name:  "malformed_user_4",
			tuple: tuple.NewTupleKey("document:1", "relation", "anne@openfga .com"),
			expectedError: &tuple.InvalidTupleError{
				Reason:   "the 'user' field is invalid",
				TupleKey: tuple.NewTupleKey("document:1", "relation", "anne@openfga .com"),
			},
		},
		{
			name:  "malformed_user_4_(invalid_user_for_1.1_model)",
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
			expectedError: &tuple.InvalidTupleError{
				Reason:   "the 'user' field must be an object (e.g. document:1) or an 'object#relation' or a typed wildcard (e.g. group:*)",
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "anne"),
			},
		},
		{
			name:  "undefined_user_type_(1.1_model)",
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
			expectedError: &tuple.TypeNotFoundError{TypeName: "employee"},
		},
		{
			name:  "undefined_user_type_in_userset_value_(1.1_model)",
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
			expectedError: &tuple.TypeNotFoundError{TypeName: "group"},
		},
		{
			name:  "undefined_userset_relation_in_userset_value_(1.1_model)",
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
			expectedError: &tuple.RelationNotFoundError{
				TypeName: "group",
				Relation: "member",
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			},
		},
		{
			name:  "untyped_wildcard_(1.0_model)",
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
		},
		{
			name:  "typed_wildcard_with_undefined_object_type",
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
			expectedError: &tuple.TypeNotFoundError{TypeName: "employee"},
		},
		{
			name:  "untyped_wildcard_in_1.1_model",
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
			expectedError: &tuple.InvalidTupleError{
				Reason:   "the 'user' field must be an object (e.g. document:1) or an 'object#relation' or a typed wildcard (e.g. group:*)",
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "*"),
			},
		},
		{
			name:  "typed_wildcard_with_valid_object_type_in_1.1_model",
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
		},
		{
			name:  "incorrect_user_object_reference_in_tupleset_relation",
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
			expectedError: &tuple.InvalidTupleError{
				Reason:   "unexpected user 'someuser' with tupleset relation 'document#parent'",
				TupleKey: tuple.NewTupleKey("document:1", "parent", "someuser"),
			},
		},
		{
			name:  "untyped_wildcard_value_in_tupleset_relation",
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
			expectedError: &tuple.InvalidTupleError{
				Reason:   "unexpected wildcard relationship with tupleset relation 'document#parent'",
				TupleKey: tuple.NewTupleKey("document:1", "parent", "*"),
			},
		},
		{
			name:  "userset_user_value_in_tupleset_relation",
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
			expectedError: &tuple.InvalidTupleError{
				Reason:   "unexpected user 'folder:1#parent' with tupleset relation 'document#parent'",
				TupleKey: tuple.NewTupleKey("document:1", "ancestor", "folder:1#parent"),
			},
		},
		{
			name:  "typed_wildcard_value_in_tupleset_relation_(1.1_models)",
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
			expectedError: &tuple.InvalidTupleError{
				Reason:   "unexpected wildcard relationship with tupleset relation 'document#parent'",
				TupleKey: tuple.NewTupleKey("document:1", "parent", "folder:*"),
			},
		},
		{
			name:  "tupleset_relation_involving_rewrite_returns_error",
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
			expectedError: &tuple.InvalidTupleError{
				Reason:   "unexpected rewrite encountered with tupelset relation 'document#parent'",
				TupleKey: tuple.NewTupleKey("document:1", "parent", "folder:1"),
			},
		},
		{
			name:  "typed_wildcard_without_allowed_type_restriction",
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
									},
								},
							},
						},
					},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Reason:   "the typed wildcard 'user:*' is not an allowed type restriction for 'document#viewer'",
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:*"),
			},
		},
		{
			name:  "relation_reference_without_allowed_type_restriction",
			tuple: tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
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
										{Type: "user"},
									},
								},
							},
						},
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
			expectedError: &tuple.InvalidTupleError{
				Reason:   "'group#member' is not an allowed type restriction for 'document#viewer'",
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			},
		},
		{
			name:  "type_without_allowed_type_restriction",
			tuple: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
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
										typesystem.WildcardRelationReference("user"),
									},
								},
							},
						},
					},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Reason:   "type 'user' is not an allowed type restriction for 'document#viewer'",
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			err := ValidateTuple(typesystem.New(test.model), test.tuple)
			require.ErrorIs(t, err, test.expectedError)
		})
	}
}
