package validation

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestValidateTupleForWrite(t *testing.T) {
	tests := []struct {
		name          string
		tuple         *openfgav1.TupleKey
		model         *openfgav1.AuthorizationModel
		expectedError error
	}{
		{
			name:  "malformed_object_1",
			tuple: tuple.NewTupleKey("group#group1:member", "relation", "user:jon"),
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("invalid 'object' field format"),
				TupleKey: tuple.NewTupleKey("group#group1:member", "relation", "user:jon"),
			},
		},
		{
			name:  "malformed_object_2",
			tuple: tuple.NewTupleKey("repo:sand castle", "relation", "user:jon"),
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("invalid 'object' field format"),
				TupleKey: tuple.NewTupleKey("repo:sand castle", "relation", "user:jon"),
			},
		},
		{
			name:  "malformed_object_3",
			tuple: tuple.NewTupleKey("fga", "relation", "user:jon"),
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("invalid 'object' field format"),
				TupleKey: tuple.NewTupleKey("fga", "relation", "user:jon"),
			},
		},
		{
			name:  "malformed_object_4",
			tuple: tuple.NewTupleKey("github:org-iam#member", "relation", "user:jon"),
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("invalid 'object' field format"),
				TupleKey: tuple.NewTupleKey("github:org-iam#member", "relation", "user:jon"),
			},
		},
		{
			name:  "malformed_object_5",
			tuple: tuple.NewTupleKey("group:group:group", "relation", "user:jon"),
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("invalid 'object' field format"),
				TupleKey: tuple.NewTupleKey("group:group:group", "relation", "user:jon"),
			},
		},
		{
			name:  "malformed_object_6",
			tuple: tuple.NewTupleKey(":", "relation", "user:jon"),
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("invalid 'object' field format"),
				TupleKey: tuple.NewTupleKey(":", "relation", "user:jon"),
			},
		},
		{
			name:  "malformed_object_7",
			tuple: tuple.NewTupleKey("unknown:id", "relation", "user:jon"),
			model: &openfgav1.AuthorizationModel{
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{Type: "user"},
					{Type: "document"},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    &tuple.TypeNotFoundError{TypeName: "unknown"},
				TupleKey: tuple.NewTupleKey("unknown:id", "relation", "user:jon"),
			},
		},
		{
			name:  "malformed_relation_1",
			tuple: tuple.NewTupleKey("document:1", "group#group", "user:jon"),
			model: &openfgav1.AuthorizationModel{
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{Type: "document"},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the 'relation' field is malformed"),
				TupleKey: tuple.NewTupleKey("document:1", "group#group", "user:jon"),
			},
		},
		{
			name:  "malformed_relation_2",
			tuple: tuple.NewTupleKey("document:1", "organization:openfga", "user:jon"),
			model: &openfgav1.AuthorizationModel{
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{Type: "document"},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the 'relation' field is malformed"),
				TupleKey: tuple.NewTupleKey("document:1", "organization:openfga", "user:jon"),
			},
		},
		{
			name:  "malformed_relation_3",
			tuple: tuple.NewTupleKey("document:1", "my relation", "user:jon"),
			model: &openfgav1.AuthorizationModel{
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{Type: "document"},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the 'relation' field is malformed"),
				TupleKey: tuple.NewTupleKey("document:1", "my relation", "user:jon"),
			},
		},
		{
			name:  "unknown_relation",
			tuple: tuple.NewTupleKey("document:1", "unknown", "user:jon"),
			model: &openfgav1.AuthorizationModel{
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{Type: "document"},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("relation 'document#unknown' not found"),
				TupleKey: tuple.NewTupleKey("document:1", "unknown", "user:jon"),
			},
		},
		{
			name:  "malformed_user_1",
			tuple: tuple.NewTupleKey("document:1", "relation", "john:albert:doe"),
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the 'user' field is malformed"),
				TupleKey: tuple.NewTupleKey("document:1", "relation", "john:albert:doe"),
			},
		},
		{
			name:  "malformed_user_2",
			tuple: tuple.NewTupleKey("document:1", "relation", "john#albert#doe"),
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the 'user' field is malformed"),
				TupleKey: tuple.NewTupleKey("document:1", "relation", "john#albert#doe"),
			},
		},
		{
			name:  "malformed_user_3",
			tuple: tuple.NewTupleKey("document:1", "relation", "invalid#test:go"),
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the 'user' field is malformed"),
				TupleKey: tuple.NewTupleKey("document:1", "relation", "invalid#test:go"),
			},
		},
		{
			name:  "malformed_user_4",
			tuple: tuple.NewTupleKey("document:1", "relation", "anne@openfga .com"),
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the 'user' field is malformed"),
				TupleKey: tuple.NewTupleKey("document:1", "relation", "anne@openfga .com"),
			},
		},
		{
			name:  "malformed_user_5",
			tuple: tuple.NewTupleKey("document:1", "relation", "user:e:eng#member"),
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the 'user' field is malformed"),
				TupleKey: tuple.NewTupleKey("document:1", "relation", "user:e:eng#member"),
			},
		},
		{
			name:  "malformed_user_6",
			tuple: tuple.NewTupleKey("document:1", "relation", "user:eng#member#member"),
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the 'user' field is malformed"),
				TupleKey: tuple.NewTupleKey("document:1", "relation", "user:eng#member#member"),
			},
		},
		{
			name:  "malformed_user_4_(invalid_user_for_1.1_model)",
			tuple: tuple.NewTupleKey("document:1", "viewer", "anne"), // user must be 'object' or 'object#relation' in 1.1 models
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
					},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the 'user' field must be an object (e.g. document:1) or an 'object#relation' or a typed wildcard (e.g. group:*)"),
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "anne"),
			},
		},
		{
			name:  "undefined_user_type_(1.1_model)",
			tuple: tuple.NewTupleKey("document:1", "viewer", "employee:anne"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
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
			expectedError: &tuple.InvalidTupleError{
				Cause:    &tuple.TypeNotFoundError{TypeName: "employee"},
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "employee:anne"),
			},
		},
		{
			name:  "undefined_user_type_in_userset_value_(1.1_model)",
			tuple: tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
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
			expectedError: &tuple.InvalidTupleError{
				Cause:    &tuple.TypeNotFoundError{TypeName: "group"},
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			},
		},
		{
			name:  "undefined_userset_relation_in_userset_value_(1.1_model)",
			tuple: tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
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
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("relation 'group#member' not found"),
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			},
		},
		{
			name:  "untyped_wildcard_(1.0_model)",
			tuple: tuple.NewTupleKey("document:1", "viewer", "*"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
					},
				},
			},
		},
		{
			name:  "typed_wildcard_with_undefined_object_type",
			tuple: tuple.NewTupleKey("document:1", "viewer", "employee:*"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
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
			expectedError: &tuple.InvalidTupleError{
				Cause:    &tuple.TypeNotFoundError{TypeName: "employee"},
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "employee:*"),
			},
		},
		{
			name:  "untyped_wildcard_in_1.1_model",
			tuple: tuple.NewTupleKey("document:1", "viewer", "*"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
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
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the 'user' field must be an object (e.g. document:1) or an 'object#relation' or a typed wildcard (e.g. group:*)"),
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "*"),
			},
		},
		{
			name:  "typed_wildcard_with_valid_object_type_in_1.1_model",
			tuple: tuple.NewTupleKey("document:1", "viewer", "user:*"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
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
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.TupleToUserset("parent", "viewer"),
						},
					},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("unexpected user 'someuser' with tupleset relation 'document#parent'"),
				TupleKey: tuple.NewTupleKey("document:1", "parent", "someuser"),
			},
		},
		{
			name:  "untyped_wildcard_value_in_tupleset_relation",
			tuple: tuple.NewTupleKey("document:1", "parent", "*"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.TupleToUserset("parent", "viewer"),
						},
					},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("unexpected wildcard relationship with tupleset relation 'document#parent'"),
				TupleKey: tuple.NewTupleKey("document:1", "parent", "*"),
			},
		},
		{
			name:  "userset_user_value_in_tupleset_relation",
			tuple: tuple.NewTupleKey("document:1", "ancestor", "folder:1#parent"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "folder",
						Relations: map[string]*openfgav1.Userset{
							"parent": typesystem.This(),
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"ancestor": typesystem.This(),
							"viewer":   typesystem.TupleToUserset("ancestor", "viewer"),
						},
					},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("unexpected user 'folder:1#parent' with tupleset relation 'document#ancestor'"),
				TupleKey: tuple.NewTupleKey("document:1", "ancestor", "folder:1#parent"),
			},
		},
		{
			name:  "typed_wildcard_value_in_tupleset_relation_(1.1_models)",
			tuple: tuple.NewTupleKey("document:1", "parent", "folder:*"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "folder",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "user"},
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.TupleToUserset("parent", "viewer"),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "folder"},
									},
								},
							},
						},
					},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("unexpected wildcard relationship with tupleset relation 'document#parent'"),
				TupleKey: tuple.NewTupleKey("document:1", "parent", "folder:*"),
			},
		},
		{
			name:  "tupleset_relation_involving_rewrite_returns_error",
			tuple: tuple.NewTupleKey("document:1", "parent", "folder:1"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "folder",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent": typesystem.ComputedUserset("editor"),
							"editor": typesystem.This(),
							"viewer": typesystem.TupleToUserset("parent", "viewer"),
						},
					},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("unexpected rewrite encountered with tupleset relation 'document#parent'"),
				TupleKey: tuple.NewTupleKey("document:1", "parent", "folder:1"),
			},
		},
		{
			name:  "typed_wildcard_without_allowed_type_restriction",
			tuple: tuple.NewTupleKey("document:1", "viewer", "user:*"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
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
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the typed wildcard 'user:*' is not an allowed type restriction for 'document#viewer'"),
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:*"),
			},
		},
		{
			name:  "relation_reference_without_allowed_type_restriction",
			tuple: tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
						Relations: map[string]*openfgav1.Userset{
							"member": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "user"},
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
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
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("'group#member' is not an allowed type restriction for 'document#viewer'"),
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			},
		},
		{
			name:  "type_without_allowed_type_restriction",
			tuple: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										typesystem.WildcardRelationReference("user"),
									},
								},
							},
						},
					},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("type 'user' is not an allowed type restriction for 'document#viewer'"),
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			},
		},
		{
			name:  "typed_wildcard_in_object_value",
			tuple: tuple.NewTupleKey("document:*", "viewer", "user:jon"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										typesystem.DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the 'object' field cannot reference a typed wildcard"),
				TupleKey: tuple.NewTupleKey("document:*", "viewer", "user:jon"),
			},
		},
		{
			name:  "typed_wildcard_in_id_of_userset_value",
			tuple: tuple.NewTupleKey("document:1", "viewer", "document:*#editor"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"editor": typesystem.This(),
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"editor": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										typesystem.DirectRelationReference("user", ""),
									},
								},
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										typesystem.DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the 'user' field is malformed"),
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "document:*#editor"),
			},
		},
		{
			name:  "typed_wildcard_in_relation_of_userset_value",
			tuple: tuple.NewTupleKey("document:1", "viewer", "document:2#*"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"editor": typesystem.This(),
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"editor": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										typesystem.DirectRelationReference("user", ""),
									},
								},
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										typesystem.DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the 'user' field is malformed"),
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "document:2#*"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ts, err := typesystem.New(test.model)
			require.NoError(t, err)
			err = ValidateTupleForWrite(ts, test.tuple)
			if test.expectedError != nil {
				require.ErrorIs(t, err, test.expectedError)
				require.Equal(t, err.Error(), test.expectedError.Error())
			}
		})
	}
}

func TestValidateTupleForRead(t *testing.T) {
	tests := []struct {
		name          string
		tuple         *openfgav1.TupleKey
		model         *openfgav1.AuthorizationModel
		expectedError error
	}{
		{
			name:  "untyped_wildcard_(1.0_model)",
			tuple: tuple.NewTupleKey("document:1", "viewer", "*"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
					},
				},
			},
		},
		{
			name:  "typed_wildcard_with_undefined_object_type",
			tuple: tuple.NewTupleKey("document:1", "viewer", "employee:*"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
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
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the typed wildcard 'employee:*' is not an allowed type restriction for 'document#viewer'"),
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "employee:*"),
			},
		},
		{
			name:  "typed_wildcard_with_valid_object_type_in_1.1_model",
			tuple: tuple.NewTupleKey("document:1", "viewer", "user:*"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
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
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.TupleToUserset("parent", "viewer"),
						},
					},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("unexpected user 'someuser' with tupleset relation 'document#parent'"),
				TupleKey: tuple.NewTupleKey("document:1", "parent", "someuser"),
			},
		},
		{
			name:  "untyped_wildcard_value_in_tupleset_relation",
			tuple: tuple.NewTupleKey("document:1", "parent", "*"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.TupleToUserset("parent", "viewer"),
						},
					},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("unexpected wildcard relationship with tupleset relation 'document#parent'"),
				TupleKey: tuple.NewTupleKey("document:1", "parent", "*"),
			},
		},
		{
			name:  "userset_user_value_in_tupleset_relation",
			tuple: tuple.NewTupleKey("document:1", "ancestor", "folder:1#parent"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "folder",
						Relations: map[string]*openfgav1.Userset{
							"parent": typesystem.This(),
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"ancestor": typesystem.This(),
							"viewer":   typesystem.TupleToUserset("ancestor", "viewer"),
						},
					},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("unexpected user 'folder:1#parent' with tupleset relation 'document#ancestor'"),
				TupleKey: tuple.NewTupleKey("document:1", "ancestor", "folder:1#parent"),
			},
		},
		{
			name:  "typed_wildcard_value_in_tupleset_relation_(1.1_models)",
			tuple: tuple.NewTupleKey("document:1", "parent", "folder:*"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "folder",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "user"},
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.TupleToUserset("parent", "viewer"),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "folder"},
									},
								},
							},
						},
					},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("unexpected wildcard relationship with tupleset relation 'document#parent'"),
				TupleKey: tuple.NewTupleKey("document:1", "parent", "folder:*"),
			},
		},
		{
			name:  "tupleset_relation_involving_rewrite_returns_error",
			tuple: tuple.NewTupleKey("document:1", "parent", "folder:1"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "folder",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent": typesystem.ComputedUserset("editor"),
							"editor": typesystem.This(),
							"viewer": typesystem.TupleToUserset("parent", "viewer"),
						},
					},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("unexpected rewrite encountered with tupleset relation 'document#parent'"),
				TupleKey: tuple.NewTupleKey("document:1", "parent", "folder:1"),
			},
		},
		{
			name:  "typed_wildcard_without_allowed_type_restriction",
			tuple: tuple.NewTupleKey("document:1", "viewer", "user:*"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
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
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("the typed wildcard 'user:*' is not an allowed type restriction for 'document#viewer'"),
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:*"),
			},
		},
		{
			name:  "relation_reference_without_allowed_type_restriction",
			tuple: tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
						Relations: map[string]*openfgav1.Userset{
							"member": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "user"},
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
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
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("'group#member' is not an allowed type restriction for 'document#viewer'"),
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			},
		},
		{
			name:  "type_without_allowed_type_restriction",
			tuple: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										typesystem.WildcardRelationReference("user"),
									},
								},
							},
						},
					},
				},
			},
			expectedError: &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("type 'user' is not an allowed type restriction for 'document#viewer'"),
				TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ts, err := typesystem.New(test.model)
			require.NoError(t, err)
			err = ValidateTupleForRead(ts, test.tuple)
			if test.expectedError != nil {
				require.ErrorIs(t, err, test.expectedError)
				require.Equal(t, err.Error(), test.expectedError.Error())
			}
		})
	}
}

func BenchmarkValidateTupleForWrite(b *testing.B) {
	model := &openfgav1.AuthorizationModel{
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "user",
			},
			{
				Type: "folder",
				Relations: map[string]*openfgav1.Userset{
					"viewer": typesystem.This(),
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"viewer": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{Type: "user"},
							},
						},
					},
				},
			},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"parent": typesystem.This(),
					"viewer": typesystem.TupleToUserset("parent", "viewer"),
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"parent": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{Type: "folder"},
							},
						},
					},
				},
			},
		},
	}

	ts, err := typesystem.New(model)
	require.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := ValidateTupleForWrite(ts, tuple.NewTupleKey("folder:x", "viewer", fmt.Sprintf("user:%v", i)))
		require.NoError(b, err)
	}
}

func BenchmarkValidateTupleForRead(b *testing.B) {
	model := &openfgav1.AuthorizationModel{
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "user",
			},
			{
				Type: "folder",
				Relations: map[string]*openfgav1.Userset{
					"viewer": typesystem.This(),
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"viewer": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{Type: "user"},
							},
						},
					},
				},
			},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"parent": typesystem.This(),
					"viewer": typesystem.TupleToUserset("parent", "viewer"),
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"parent": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{Type: "folder"},
							},
						},
					},
				},
			},
		},
	}

	ts, err := typesystem.New(model)
	require.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := ValidateTupleForRead(ts, tuple.NewTupleKey("folder:x", "viewer", fmt.Sprintf("user:%v", i)))
		require.NoError(b, err)
	}
}
