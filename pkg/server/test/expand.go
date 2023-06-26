package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestExpandQuery(t *testing.T, datastore storage.OpenFGADatastore) {
	tests := []struct {
		name     string
		model    *openfgapb.AuthorizationModel
		tuples   []*openfgapb.TupleKey
		request  *openfgapb.ExpandRequest
		expected *openfgapb.ExpandResponse
	}{
		{
			name: "1.1_simple_direct",
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"admin": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
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
			tuples: []*openfgapb.TupleKey{
				{
					Object:   "repo:openfga/foo",
					Relation: "admin",
					User:     "user:jon",
				},
			},
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:openfga/foo",
					Relation: "admin",
				},
			},
			expected: &openfgapb.ExpandResponse{
				Tree: &openfgapb.UsersetTree{
					Root: &openfgapb.UsersetTree_Node{
						Name: "repo:openfga/foo#admin",
						Value: &openfgapb.UsersetTree_Node_Leaf{
							Leaf: &openfgapb.UsersetTree_Leaf{
								Value: &openfgapb.UsersetTree_Leaf_Users{
									Users: &openfgapb.UsersetTree_Users{
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
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin":  typesystem.This(),
							"writer": typesystem.ComputedUserset("admin"),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"admin": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
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
			tuples: []*openfgapb.TupleKey{},
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:openfga/foo",
					Relation: "writer",
				},
			},
			expected: &openfgapb.ExpandResponse{
				Tree: &openfgapb.UsersetTree{
					Root: &openfgapb.UsersetTree_Node{
						Name: "repo:openfga/foo#writer",
						Value: &openfgapb.UsersetTree_Node_Leaf{
							Leaf: &openfgapb.UsersetTree_Leaf{
								Value: &openfgapb.UsersetTree_Leaf_Computed{
									Computed: &openfgapb.UsersetTree_Computed{
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
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin":   typesystem.TupleToUserset("manager", "repo_admin"),
							"manager": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"manager": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
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
						Relations: map[string]*openfgapb.Userset{
							"repo_admin": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"repo_admin": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
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
			tuples: []*openfgapb.TupleKey{
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
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:openfga/foo",
					Relation: "admin",
				},
			},
			expected: &openfgapb.ExpandResponse{
				Tree: &openfgapb.UsersetTree{
					Root: &openfgapb.UsersetTree_Node{
						Name: "repo:openfga/foo#admin",
						Value: &openfgapb.UsersetTree_Node_Leaf{
							Leaf: &openfgapb.UsersetTree_Leaf{
								Value: &openfgapb.UsersetTree_Leaf_TupleToUserset{
									TupleToUserset: &openfgapb.UsersetTree_TupleToUserset{
										Tupleset: "repo:openfga/foo#manager",
										Computed: []*openfgapb.UsersetTree_Computed{
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
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin":   typesystem.TupleToUserset("manager", "repo_admin"),
							"manager": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"manager": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
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
						Relations: map[string]*openfgapb.Userset{
							"repo_admin": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"repo_admin": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
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
			tuples: []*openfgapb.TupleKey{
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
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:openfga/foo",
					Relation: "admin",
				},
			},
			expected: &openfgapb.ExpandResponse{
				Tree: &openfgapb.UsersetTree{
					Root: &openfgapb.UsersetTree_Node{
						Name: "repo:openfga/foo#admin",
						Value: &openfgapb.UsersetTree_Node_Leaf{
							Leaf: &openfgapb.UsersetTree_Leaf{
								Value: &openfgapb.UsersetTree_Leaf_TupleToUserset{
									TupleToUserset: &openfgapb.UsersetTree_TupleToUserset{
										Tupleset: "repo:openfga/foo#manager",
										Computed: []*openfgapb.UsersetTree_Computed{
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
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin":   typesystem.TupleToUserset("manager", "repo_admin"),
							"manager": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"manager": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
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
						Relations: map[string]*openfgapb.Userset{
							"repo_admin": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"repo_admin": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
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
			tuples: []*openfgapb.TupleKey{
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
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:openfga/foo",
					Relation: "admin",
				},
			},
			expected: &openfgapb.ExpandResponse{
				Tree: &openfgapb.UsersetTree{
					Root: &openfgapb.UsersetTree_Node{
						Name: "repo:openfga/foo#admin",
						Value: &openfgapb.UsersetTree_Node_Leaf{
							Leaf: &openfgapb.UsersetTree_Leaf{
								Value: &openfgapb.UsersetTree_Leaf_TupleToUserset{
									TupleToUserset: &openfgapb.UsersetTree_TupleToUserset{
										Tupleset: "repo:openfga/foo#manager",
										Computed: []*openfgapb.UsersetTree_Computed{
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
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin": typesystem.This(),
							"writer": typesystem.Union(
								typesystem.This(),
								typesystem.ComputedUserset("admin")),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"admin": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type: "user",
										},
									},
								},
								"writer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
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
			tuples: []*openfgapb.TupleKey{
				{
					Object:   "repo:openfga/foo",
					Relation: "writer",
					User:     "user:jon",
				},
			},
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:openfga/foo",
					Relation: "writer",
				},
			},
			expected: &openfgapb.ExpandResponse{
				Tree: &openfgapb.UsersetTree{
					Root: &openfgapb.UsersetTree_Node{
						Name: "repo:openfga/foo#writer",
						Value: &openfgapb.UsersetTree_Node_Union{
							Union: &openfgapb.UsersetTree_Nodes{
								Nodes: []*openfgapb.UsersetTree_Node{
									{
										Name: "repo:openfga/foo#writer",
										Value: &openfgapb.UsersetTree_Node_Leaf{
											Leaf: &openfgapb.UsersetTree_Leaf{
												Value: &openfgapb.UsersetTree_Leaf_Users{
													Users: &openfgapb.UsersetTree_Users{
														Users: []string{"user:jon"},
													},
												},
											},
										},
									},
									{
										Name: "repo:openfga/foo#writer",
										Value: &openfgapb.UsersetTree_Node_Leaf{
											Leaf: &openfgapb.UsersetTree_Leaf{
												Value: &openfgapb.UsersetTree_Leaf_Computed{
													Computed: &openfgapb.UsersetTree_Computed{
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
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin":  typesystem.This(),
							"banned": typesystem.This(),
							"active_admin": typesystem.Difference(
								typesystem.ComputedUserset("admin"),
								typesystem.ComputedUserset("banned")),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"admin": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type: "user",
										},
									},
								},
								"banned": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
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
			tuples: []*openfgapb.TupleKey{},
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:openfga/foo",
					Relation: "active_admin",
				},
			},
			expected: &openfgapb.ExpandResponse{
				Tree: &openfgapb.UsersetTree{
					Root: &openfgapb.UsersetTree_Node{
						Name: "repo:openfga/foo#active_admin",
						Value: &openfgapb.UsersetTree_Node_Difference{
							Difference: &openfgapb.UsersetTree_Difference{
								Base: &openfgapb.UsersetTree_Node{
									Name: "repo:openfga/foo#active_admin",
									Value: &openfgapb.UsersetTree_Node_Leaf{
										Leaf: &openfgapb.UsersetTree_Leaf{
											Value: &openfgapb.UsersetTree_Leaf_Computed{
												Computed: &openfgapb.UsersetTree_Computed{
													Userset: "repo:openfga/foo#admin",
												},
											},
										},
									},
								},
								Subtract: &openfgapb.UsersetTree_Node{
									Name: "repo:openfga/foo#active_admin",
									Value: &openfgapb.UsersetTree_Node_Leaf{
										Leaf: &openfgapb.UsersetTree_Leaf{
											Value: &openfgapb.UsersetTree_Leaf_Computed{
												Computed: &openfgapb.UsersetTree_Computed{
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
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin": typesystem.This(),
							// Writers must be both directly in 'writers', and in 'admins'
							"writer": typesystem.Intersection(
								typesystem.This(),
								typesystem.ComputedUserset("admin")),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"admin": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type: "user",
										},
									},
								},
								"writer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
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
			tuples: []*openfgapb.TupleKey{},
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:openfga/foo",
					Relation: "writer",
				},
			},
			expected: &openfgapb.ExpandResponse{
				Tree: &openfgapb.UsersetTree{
					Root: &openfgapb.UsersetTree_Node{
						Name: "repo:openfga/foo#writer",
						Value: &openfgapb.UsersetTree_Node_Intersection{
							Intersection: &openfgapb.UsersetTree_Nodes{
								Nodes: []*openfgapb.UsersetTree_Node{
									{
										Name: "repo:openfga/foo#writer",
										Value: &openfgapb.UsersetTree_Node_Leaf{
											Leaf: &openfgapb.UsersetTree_Leaf{
												Value: &openfgapb.UsersetTree_Leaf_Users{
													Users: &openfgapb.UsersetTree_Users{
														Users: []string{},
													},
												},
											},
										},
									},
									{
										Name: "repo:openfga/foo#writer",
										Value: &openfgapb.UsersetTree_Node_Leaf{
											Leaf: &openfgapb.UsersetTree_Leaf{
												Value: &openfgapb.UsersetTree_Leaf_Computed{
													Computed: &openfgapb.UsersetTree_Computed{
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
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
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
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"admin": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type: "user",
										},
									},
								},
								"owner": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type: "org",
										},
									},
								},
								"banned_writer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type: "user",
										},
									},
								},
								"writer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
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
						Relations: map[string]*openfgapb.Userset{
							"repo_writer": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"repo_writer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
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
			tuples: []*openfgapb.TupleKey{
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
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:openfga/foo",
					Relation: "writer",
				},
			},
			expected: &openfgapb.ExpandResponse{
				Tree: &openfgapb.UsersetTree{
					Root: &openfgapb.UsersetTree_Node{
						Name: "repo:openfga/foo#writer",
						Value: &openfgapb.UsersetTree_Node_Difference{
							Difference: &openfgapb.UsersetTree_Difference{
								Base: &openfgapb.UsersetTree_Node{
									Name: "repo:openfga/foo#writer",
									Value: &openfgapb.UsersetTree_Node_Union{
										Union: &openfgapb.UsersetTree_Nodes{
											Nodes: []*openfgapb.UsersetTree_Node{
												{
													Name: "repo:openfga/foo#writer",
													Value: &openfgapb.UsersetTree_Node_Leaf{
														Leaf: &openfgapb.UsersetTree_Leaf{
															Value: &openfgapb.UsersetTree_Leaf_Users{
																Users: &openfgapb.UsersetTree_Users{
																	Users: []string{"user:jon"},
																},
															},
														},
													},
												},
												{
													Name: "repo:openfga/foo#writer",
													Value: &openfgapb.UsersetTree_Node_Leaf{
														Leaf: &openfgapb.UsersetTree_Leaf{
															Value: &openfgapb.UsersetTree_Leaf_TupleToUserset{
																TupleToUserset: &openfgapb.UsersetTree_TupleToUserset{
																	Tupleset: "repo:openfga/foo#owner",
																	Computed: []*openfgapb.UsersetTree_Computed{
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
								Subtract: &openfgapb.UsersetTree_Node{
									Name: "repo:openfga/foo#writer",
									Value: &openfgapb.UsersetTree_Node_Leaf{
										Leaf: &openfgapb.UsersetTree_Leaf{
											Value: &openfgapb.UsersetTree_Leaf_Computed{
												Computed: &openfgapb.UsersetTree_Computed{
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
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"editor": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("document", "editor"),
									},
								},
								"editor": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "document:2#editor"),
			},
			request: &openfgapb.ExpandRequest{
				TupleKey: tuple.NewTupleKey("document:1", "parent", ""),
			},
			expected: &openfgapb.ExpandResponse{
				Tree: &openfgapb.UsersetTree{
					Root: &openfgapb.UsersetTree_Node{
						Name: "document:1#parent",
						Value: &openfgapb.UsersetTree_Node_Leaf{
							Leaf: &openfgapb.UsersetTree_Leaf{
								Value: &openfgapb.UsersetTree_Leaf_Users{
									Users: &openfgapb.UsersetTree_Users{
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

	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// arrange
			store := ulid.Make().String()
			err := datastore.WriteAuthorizationModel(ctx, store, test.model)
			require.NoError(err)

			err = datastore.Write(ctx, store, []*openfgapb.TupleKey{}, test.tuples)
			require.NoError(err)

			require.NoError(err)
			test.request.StoreId = store
			test.request.AuthorizationModelId = test.model.Id

			// act
			query := commands.NewExpandQuery(datastore, logger)
			got, err := query.Execute(ctx, test.request)
			require.NoError(err)

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
		model         *openfgapb.AuthorizationModel
		tuples        []*openfgapb.TupleKey
		request       *openfgapb.ExpandRequest
		allowSchema10 bool
		expected      error
	}{
		{
			name: "missing_object_in_request",
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Relation: "bar",
				},
			},
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{Type: "repo"},
				},
			},
			allowSchema10: true,
			expected:      serverErrors.InvalidExpandInput,
		},
		{
			name: "missing_object_id_and_type_in_request",
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   ":",
					Relation: "bar",
				},
			},
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
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
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "github:",
					Relation: "bar",
				},
			},
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
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
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object: "bar",
				},
			},
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{Type: "repo"},
				},
			},
			allowSchema10: true,
			expected:      serverErrors.InvalidExpandInput,
		},
		{
			name: "1.1_object_type_not_found_in_model",
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "foo:bar",
					Relation: "baz",
				},
			},
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
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
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{Type: "repo"},
				},
			},
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
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

	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// arrange
			store := ulid.Make().String()
			err := datastore.WriteAuthorizationModel(ctx, store, test.model)
			require.NoError(err)

			err = datastore.Write(ctx, store, []*openfgapb.TupleKey{}, test.tuples)
			require.NoError(err)

			require.NoError(err)
			test.request.StoreId = store
			test.request.AuthorizationModelId = test.model.Id

			// act
			query := commands.NewExpandQuery(datastore, logger)
			resp, err := query.Execute(ctx, test.request)

			// assert
			require.Nil(resp)
			require.ErrorIs(err, test.expected)
		})
	}
}
