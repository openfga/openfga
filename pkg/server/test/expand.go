package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestExpandQuery(t *testing.T, datastore storage.OpenFGADatastore) {
	tests := []struct {
		name     string
		model    *openfgav1.AuthorizationModel
		tuples   []*openfgav1.TupleKey
		request  *openfgav1.ExpandRequest
		expected *openfgav1.ExpandResponse
	}{
		{
			name: "1.1_simple_direct",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "repo",
						Relations: map[string]*openfgav1.Userset{
							"admin": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"admin": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
			tuples: []*openfgav1.TupleKey{
				{
					Object:   "repo:openfga/foo",
					Relation: "admin",
					User:     "user:jon",
				},
			},
			request: &openfgav1.ExpandRequest{
				TupleKey: tuple.NewExpandRequestTupleKey(
					"repo:openfga/foo",
					"admin",
				),
			},
			expected: &openfgav1.ExpandResponse{
				Tree: &openfgav1.UsersetTree{
					Root: &openfgav1.UsersetTree_Node{
						Name: "repo:openfga/foo#admin",
						Value: &openfgav1.UsersetTree_Node_Leaf{
							Leaf: &openfgav1.UsersetTree_Leaf{
								Value: &openfgav1.UsersetTree_Leaf_Users{
									Users: &openfgav1.UsersetTree_Users{
										Users: []string{"user:jon"},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "1.1_computed_userset",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "repo",
						Relations: map[string]*openfgav1.Userset{
							"admin":  typesystem.This(),
							"writer": typesystem.ComputedUserset("admin"),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"admin": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
			tuples: []*openfgav1.TupleKey{},
			request: &openfgav1.ExpandRequest{
				TupleKey: tuple.NewExpandRequestTupleKey(
					"repo:openfga/foo",
					"writer",
				),
			},
			expected: &openfgav1.ExpandResponse{
				Tree: &openfgav1.UsersetTree{
					Root: &openfgav1.UsersetTree_Node{
						Name: "repo:openfga/foo#writer",
						Value: &openfgav1.UsersetTree_Node_Leaf{
							Leaf: &openfgav1.UsersetTree_Leaf{
								Value: &openfgav1.UsersetTree_Leaf_Computed{
									Computed: &openfgav1.UsersetTree_Computed{
										Userset: "repo:openfga/foo#admin",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "1.1_tuple_to_userset",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "repo",
						Relations: map[string]*openfgav1.Userset{
							"admin":   typesystem.TupleToUserset("manager", "repo_admin"),
							"manager": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"manager": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "org",
										},
									},
								},
							},
						},
					},
					{
						Type: "org",
						Relations: map[string]*openfgav1.Userset{
							"repo_admin": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"repo_admin": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
			tuples: []*openfgav1.TupleKey{
				{
					Object:   "repo:openfga/foo",
					Relation: "manager",
					User:     "org:openfga",
				},
				{
					Object:   "org:openfga",
					Relation: "repo_admin",
					User:     "user:jon",
				},
			},
			request: &openfgav1.ExpandRequest{
				TupleKey: tuple.NewExpandRequestTupleKey(
					"repo:openfga/foo",
					"admin",
				),
			},
			expected: &openfgav1.ExpandResponse{
				Tree: &openfgav1.UsersetTree{
					Root: &openfgav1.UsersetTree_Node{
						Name: "repo:openfga/foo#admin",
						Value: &openfgav1.UsersetTree_Node_Leaf{
							Leaf: &openfgav1.UsersetTree_Leaf{
								Value: &openfgav1.UsersetTree_Leaf_TupleToUserset{
									TupleToUserset: &openfgav1.UsersetTree_TupleToUserset{
										Tupleset: "repo:openfga/foo#manager",
										Computed: []*openfgav1.UsersetTree_Computed{
											{
												Userset: "org:openfga#repo_admin",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "1.1_tuple_to_userset_II",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "repo",
						Relations: map[string]*openfgav1.Userset{
							"admin":   typesystem.TupleToUserset("manager", "repo_admin"),
							"manager": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"manager": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "org",
										},
									},
								},
							},
						},
					},
					{
						Type: "org",
						Relations: map[string]*openfgav1.Userset{
							"repo_admin": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"repo_admin": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
			tuples: []*openfgav1.TupleKey{
				{
					Object:   "repo:openfga/foo",
					Relation: "manager",
					User:     "org:openfga",
				},
				{
					Object:   "org:openfga",
					Relation: "repo_admin",
					User:     "user:jon",
				},
				{
					Object:   "repo:openfga/foo",
					Relation: "manager",
					User:     "amy", // should be skipped since it's not a valid target for a tupleset relation
				},
			},
			request: &openfgav1.ExpandRequest{
				TupleKey: tuple.NewExpandRequestTupleKey(
					"repo:openfga/foo",
					"admin",
				),
			},
			expected: &openfgav1.ExpandResponse{
				Tree: &openfgav1.UsersetTree{
					Root: &openfgav1.UsersetTree_Node{
						Name: "repo:openfga/foo#admin",
						Value: &openfgav1.UsersetTree_Node_Leaf{
							Leaf: &openfgav1.UsersetTree_Leaf{
								Value: &openfgav1.UsersetTree_Leaf_TupleToUserset{
									TupleToUserset: &openfgav1.UsersetTree_TupleToUserset{
										Tupleset: "repo:openfga/foo#manager",
										Computed: []*openfgav1.UsersetTree_Computed{
											{
												Userset: "org:openfga#repo_admin",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "1.1_tuple_to_userset_implicit",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "repo",
						Relations: map[string]*openfgav1.Userset{
							"admin":   typesystem.TupleToUserset("manager", "repo_admin"),
							"manager": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"manager": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "org",
										},
									},
								},
							},
						},
					},
					{
						Type: "org",
						Relations: map[string]*openfgav1.Userset{
							"repo_admin": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"repo_admin": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
			tuples: []*openfgav1.TupleKey{
				{
					Object:   "repo:openfga/foo",
					Relation: "manager",
					User:     "org:openfga",
				},
				{
					Object:   "org:openfga",
					Relation: "repo_admin",
					User:     "user:jon",
				},
			},
			request: &openfgav1.ExpandRequest{
				TupleKey: tuple.NewExpandRequestTupleKey(
					"repo:openfga/foo",
					"admin",
				),
			},
			expected: &openfgav1.ExpandResponse{
				Tree: &openfgav1.UsersetTree{
					Root: &openfgav1.UsersetTree_Node{
						Name: "repo:openfga/foo#admin",
						Value: &openfgav1.UsersetTree_Node_Leaf{
							Leaf: &openfgav1.UsersetTree_Leaf{
								Value: &openfgav1.UsersetTree_Leaf_TupleToUserset{
									TupleToUserset: &openfgav1.UsersetTree_TupleToUserset{
										Tupleset: "repo:openfga/foo#manager",
										Computed: []*openfgav1.UsersetTree_Computed{
											{
												Userset: "org:openfga#repo_admin",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "1.1_simple_union",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "repo",
						Relations: map[string]*openfgav1.Userset{
							"admin": typesystem.This(),
							"writer": typesystem.Union(
								typesystem.This(),
								typesystem.ComputedUserset("admin")),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"admin": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
								"writer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
			tuples: []*openfgav1.TupleKey{
				{
					Object:   "repo:openfga/foo",
					Relation: "writer",
					User:     "user:jon",
				},
			},
			request: &openfgav1.ExpandRequest{
				TupleKey: tuple.NewExpandRequestTupleKey(
					"repo:openfga/foo",
					"writer",
				),
			},
			expected: &openfgav1.ExpandResponse{
				Tree: &openfgav1.UsersetTree{
					Root: &openfgav1.UsersetTree_Node{
						Name: "repo:openfga/foo#writer",
						Value: &openfgav1.UsersetTree_Node_Union{
							Union: &openfgav1.UsersetTree_Nodes{
								Nodes: []*openfgav1.UsersetTree_Node{
									{
										Name: "repo:openfga/foo#writer",
										Value: &openfgav1.UsersetTree_Node_Leaf{
											Leaf: &openfgav1.UsersetTree_Leaf{
												Value: &openfgav1.UsersetTree_Leaf_Users{
													Users: &openfgav1.UsersetTree_Users{
														Users: []string{"user:jon"},
													},
												},
											},
										},
									},
									{
										Name: "repo:openfga/foo#writer",
										Value: &openfgav1.UsersetTree_Node_Leaf{
											Leaf: &openfgav1.UsersetTree_Leaf{
												Value: &openfgav1.UsersetTree_Leaf_Computed{
													Computed: &openfgav1.UsersetTree_Computed{
														Userset: "repo:openfga/foo#admin",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "1.1_simple_difference",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "repo",
						Relations: map[string]*openfgav1.Userset{
							"admin":  typesystem.This(),
							"banned": typesystem.This(),
							"active_admin": typesystem.Difference(
								typesystem.ComputedUserset("admin"),
								typesystem.ComputedUserset("banned")),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"admin": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
								"banned": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
			tuples: []*openfgav1.TupleKey{},
			request: &openfgav1.ExpandRequest{
				TupleKey: tuple.NewExpandRequestTupleKey(
					"repo:openfga/foo",
					"active_admin",
				),
			},
			expected: &openfgav1.ExpandResponse{
				Tree: &openfgav1.UsersetTree{
					Root: &openfgav1.UsersetTree_Node{
						Name: "repo:openfga/foo#active_admin",
						Value: &openfgav1.UsersetTree_Node_Difference{
							Difference: &openfgav1.UsersetTree_Difference{
								Base: &openfgav1.UsersetTree_Node{
									Name: "repo:openfga/foo#active_admin",
									Value: &openfgav1.UsersetTree_Node_Leaf{
										Leaf: &openfgav1.UsersetTree_Leaf{
											Value: &openfgav1.UsersetTree_Leaf_Computed{
												Computed: &openfgav1.UsersetTree_Computed{
													Userset: "repo:openfga/foo#admin",
												},
											},
										},
									},
								},
								Subtract: &openfgav1.UsersetTree_Node{
									Name: "repo:openfga/foo#active_admin",
									Value: &openfgav1.UsersetTree_Node_Leaf{
										Leaf: &openfgav1.UsersetTree_Leaf{
											Value: &openfgav1.UsersetTree_Leaf_Computed{
												Computed: &openfgav1.UsersetTree_Computed{
													Userset: "repo:openfga/foo#banned",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "1.1_intersection",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "repo",
						Relations: map[string]*openfgav1.Userset{
							"admin": typesystem.This(),
							// Writers must be both directly in 'writers', and in 'admins'
							"writer": typesystem.Intersection(
								typesystem.This(),
								typesystem.ComputedUserset("admin")),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"admin": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
								"writer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
			tuples: []*openfgav1.TupleKey{},
			request: &openfgav1.ExpandRequest{
				TupleKey: tuple.NewExpandRequestTupleKey(
					"repo:openfga/foo",
					"writer",
				),
			},
			expected: &openfgav1.ExpandResponse{
				Tree: &openfgav1.UsersetTree{
					Root: &openfgav1.UsersetTree_Node{
						Name: "repo:openfga/foo#writer",
						Value: &openfgav1.UsersetTree_Node_Intersection{
							Intersection: &openfgav1.UsersetTree_Nodes{
								Nodes: []*openfgav1.UsersetTree_Node{
									{
										Name: "repo:openfga/foo#writer",
										Value: &openfgav1.UsersetTree_Node_Leaf{
											Leaf: &openfgav1.UsersetTree_Leaf{
												Value: &openfgav1.UsersetTree_Leaf_Users{
													Users: &openfgav1.UsersetTree_Users{
														Users: []string{},
													},
												},
											},
										},
									},
									{
										Name: "repo:openfga/foo#writer",
										Value: &openfgav1.UsersetTree_Node_Leaf{
											Leaf: &openfgav1.UsersetTree_Leaf{
												Value: &openfgav1.UsersetTree_Leaf_Computed{
													Computed: &openfgav1.UsersetTree_Computed{
														Userset: "repo:openfga/foo#admin",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "1.1_complex_tree",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "repo",
						Relations: map[string]*openfgav1.Userset{
							"admin":         typesystem.This(),
							"owner":         typesystem.This(),
							"banned_writer": typesystem.This(),
							// Users can write if they are direct members of writers, or repo_writers
							// in the org, unless they are also in banned_writers
							"writer": typesystem.Difference(
								typesystem.Union(
									typesystem.This(),
									typesystem.TupleToUserset("owner", "repo_writer")),
								typesystem.ComputedUserset("banned_writer")),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"admin": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
								"owner": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "org",
										},
									},
								},
								"banned_writer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
								"writer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
					{
						Type: "org",
						Relations: map[string]*openfgav1.Userset{
							"repo_writer": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"repo_writer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
			tuples: []*openfgav1.TupleKey{
				{
					Object:   "repo:openfga/foo",
					Relation: "owner",
					User:     "org:openfga",
				},
				{
					Object:   "repo:openfga/foo",
					Relation: "writer",
					User:     "user:jon",
				},
			},
			request: &openfgav1.ExpandRequest{
				TupleKey: tuple.NewExpandRequestTupleKey(
					"repo:openfga/foo",
					"writer",
				),
			},
			expected: &openfgav1.ExpandResponse{
				Tree: &openfgav1.UsersetTree{
					Root: &openfgav1.UsersetTree_Node{
						Name: "repo:openfga/foo#writer",
						Value: &openfgav1.UsersetTree_Node_Difference{
							Difference: &openfgav1.UsersetTree_Difference{
								Base: &openfgav1.UsersetTree_Node{
									Name: "repo:openfga/foo#writer",
									Value: &openfgav1.UsersetTree_Node_Union{
										Union: &openfgav1.UsersetTree_Nodes{
											Nodes: []*openfgav1.UsersetTree_Node{
												{
													Name: "repo:openfga/foo#writer",
													Value: &openfgav1.UsersetTree_Node_Leaf{
														Leaf: &openfgav1.UsersetTree_Leaf{
															Value: &openfgav1.UsersetTree_Leaf_Users{
																Users: &openfgav1.UsersetTree_Users{
																	Users: []string{"user:jon"},
																},
															},
														},
													},
												},
												{
													Name: "repo:openfga/foo#writer",
													Value: &openfgav1.UsersetTree_Node_Leaf{
														Leaf: &openfgav1.UsersetTree_Leaf{
															Value: &openfgav1.UsersetTree_Leaf_TupleToUserset{
																TupleToUserset: &openfgav1.UsersetTree_TupleToUserset{
																	Tupleset: "repo:openfga/foo#owner",
																	Computed: []*openfgav1.UsersetTree_Computed{
																		{Userset: "org:openfga#repo_writer"},
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
								Subtract: &openfgav1.UsersetTree_Node{
									Name: "repo:openfga/foo#writer",
									Value: &openfgav1.UsersetTree_Node_Leaf{
										Leaf: &openfgav1.UsersetTree_Leaf{
											Value: &openfgav1.UsersetTree_Leaf_Computed{
												Computed: &openfgav1.UsersetTree_Computed{
													Userset: "repo:openfga/foo#banned_writer",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "1.1_Tuple_involving_userset_that_is_not_involved_in_TTU_rewrite",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent": typesystem.This(),
							"editor": typesystem.This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										typesystem.DirectRelationReference("document", "editor"),
									},
								},
								"editor": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "document:2#editor"),
			},
			request: &openfgav1.ExpandRequest{
				TupleKey: tuple.NewExpandRequestTupleKey("document:1", "parent"),
			},
			expected: &openfgav1.ExpandResponse{
				Tree: &openfgav1.UsersetTree{
					Root: &openfgav1.UsersetTree_Node{
						Name: "document:1#parent",
						Value: &openfgav1.UsersetTree_Node_Leaf{
							Leaf: &openfgav1.UsersetTree_Leaf{
								Value: &openfgav1.UsersetTree_Leaf_Users{
									Users: &openfgav1.UsersetTree_Users{
										Users: []string{"document:2#editor"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	ctx := context.Background()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// arrange
			store := ulid.Make().String()
			err := datastore.WriteAuthorizationModel(ctx, store, test.model)
			require.NoError(t, err)

			err = datastore.Write(
				ctx,
				store,
				[]*openfgav1.TupleKeyWithoutCondition{},
				test.tuples,
			)
			require.NoError(t, err)

			require.NoError(t, err)
			test.request.StoreId = store
			test.request.AuthorizationModelId = test.model.GetId()

			// act
			query := commands.NewExpandQuery(datastore)
			got, err := query.Execute(ctx, test.request)
			require.NoError(t, err)

			// assert
			if diff := cmp.Diff(test.expected, got, protocmp.Transform()); diff != "" {
				t.Errorf("mismatch (-want, +got):\n%s", diff)
			}
		})
	}
}

func TestExpandQueryErrors(t *testing.T, datastore storage.OpenFGADatastore) {
	tests := []struct {
		name          string
		model         *openfgav1.AuthorizationModel
		tuples        []*openfgav1.TupleKey
		request       *openfgav1.ExpandRequest
		allowSchema10 bool
		expected      error
	}{
		{
			name: "missing_object_in_request",
			request: &openfgav1.ExpandRequest{
				TupleKey: &openfgav1.ExpandRequestTupleKey{
					Relation: "bar",
				},
			},
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{Type: "repo"},
				},
			},
			allowSchema10: true,
			expected:      serverErrors.InvalidExpandInput,
		},
		{
			name: "missing_object_id_and_type_in_request",
			request: &openfgav1.ExpandRequest{
				TupleKey: &openfgav1.ExpandRequestTupleKey{
					Object:   ":",
					Relation: "bar",
				},
			},
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{Type: "repo"},
				},
			},
			allowSchema10: true,
			expected: serverErrors.ValidationError(
				fmt.Errorf("invalid 'object' field format"),
			),
		},
		{
			name: "missing_object_id_in_request",
			request: &openfgav1.ExpandRequest{
				TupleKey: &openfgav1.ExpandRequestTupleKey{
					Object:   "github:",
					Relation: "bar",
				},
			},
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{Type: "repo"},
				},
			},
			allowSchema10: true,
			expected: serverErrors.ValidationError(
				fmt.Errorf("invalid 'object' field format"),
			),
		},
		{
			name: "missing_relation_in_request",
			request: &openfgav1.ExpandRequest{
				TupleKey: &openfgav1.ExpandRequestTupleKey{
					Object: "bar",
				},
			},
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{Type: "repo"},
				},
			},
			allowSchema10: true,
			expected:      serverErrors.InvalidExpandInput,
		},
		{
			name: "1.1_object_type_not_found_in_model",
			request: &openfgav1.ExpandRequest{
				TupleKey: &openfgav1.ExpandRequestTupleKey{
					Object:   "foo:bar",
					Relation: "baz",
				},
			},
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{Type: "repo"},
				},
			},
			allowSchema10: true,
			expected: serverErrors.ValidationError(
				&tuple.TypeNotFoundError{TypeName: "foo"},
			),
		},
		{
			name: "1.1_relation_not_found_in_model",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{Type: "repo"},
				},
			},
			request: &openfgav1.ExpandRequest{
				TupleKey: &openfgav1.ExpandRequestTupleKey{
					Object:   "repo:bar",
					Relation: "baz",
				},
			},
			allowSchema10: true,
			expected: serverErrors.ValidationError(
				&tuple.RelationNotFoundError{
					TypeName: "repo",
					Relation: "baz",
				},
			),
		},
	}

	ctx := context.Background()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// arrange
			store := ulid.Make().String()
			err := datastore.WriteAuthorizationModel(ctx, store, test.model)
			require.NoError(t, err)

			err = datastore.Write(
				ctx,
				store,
				[]*openfgav1.TupleKeyWithoutCondition{},
				test.tuples,
			)
			require.NoError(t, err)

			require.NoError(t, err)
			test.request.StoreId = store
			test.request.AuthorizationModelId = test.model.GetId()

			// act
			query := commands.NewExpandQuery(datastore)
			resp, err := query.Execute(ctx, test.request)

			// assert
			require.Nil(t, resp)
			require.ErrorIs(t, err, test.expected)
		})
	}
}
