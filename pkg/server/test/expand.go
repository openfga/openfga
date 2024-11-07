package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
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
			model: testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type repo
					relations
						define admin: [user]`),
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
			model: testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type repo
					relations
						define admin: [user]
						define writer: admin`),
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
			model: testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type repo
					relations
						define admin: repo_admin from manager
						define manager: [org]
				type org
					relations
						define repo_admin: [user]`),
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
			model: testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type repo
					relations
						define admin: repo_admin from manager
						define manager: [org]
				type org
					relations
						define repo_admin: [user]`),
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
			model: testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type repo
					relations
						define admin: repo_admin from manager
						define manager: [org]
				type org
					relations
						define repo_admin: [user]`),
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
			model: testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type repo
					relations
						define admin: [user]
						define writer: [user] or admin
				type org
					relations
						define repo_admin: [user]`),
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
			model: testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type repo
					relations
						define admin: [user]
						define banned: [user]
						define active_admin: admin but not banned`),
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
			model: testutils.MustTransformDSLToProtoWithID(`
				model
				  schema 1.1
				type user
				type repo
					relations
						define admin: [user]
						define writer: [user] and admin`),
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
			model: testutils.MustTransformDSLToProtoWithID(`
				model
				  schema 1.1
				type user
				type repo
					relations
						define admin: [user]
						define owner: [org]
						define banned_writer: [user]
						define writer: ([user] or repo_writer from owner) but not banned_writer
				type org
					relations
						define repo_writer: [user]`),
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
			model: testutils.MustTransformDSLToProtoWithID(`
				model
				  schema 1.1
				type user
				type document
					relations
						define parent: [document#editor]
						define editor: [user]`),
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
		{
			name: "self_defined_userset_not_returned",
			model: testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type group
				relations
					define viewer: [user]
			`),
			tuples: []*openfgav1.TupleKey{},
			request: &openfgav1.ExpandRequest{
				TupleKey: tuple.NewExpandRequestTupleKey("group:1", "viewer"),
			},
			expected: &openfgav1.ExpandResponse{
				Tree: &openfgav1.UsersetTree{
					Root: &openfgav1.UsersetTree_Node{
						Name: "group:1#viewer",
						Value: &openfgav1.UsersetTree_Node_Leaf{
							Leaf: &openfgav1.UsersetTree_Leaf{
								Value: &openfgav1.UsersetTree_Leaf_Users{
									Users: &openfgav1.UsersetTree_Users{
										// group:1#viewer isn't included because it's implicit
										Users: []string{},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "self_defined_userset_not_returned_even_if_tuple_written",
			model: testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type group
				relations
					define viewer: [user]
			`),
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:1", "viewer", "group:1#viewer"), // invalid, so should be skipped over
			},
			request: &openfgav1.ExpandRequest{
				TupleKey: tuple.NewExpandRequestTupleKey("group:1", "viewer"),
			},
			expected: &openfgav1.ExpandResponse{
				Tree: &openfgav1.UsersetTree{
					Root: &openfgav1.UsersetTree_Node{
						Name: "group:1#viewer",
						Value: &openfgav1.UsersetTree_Node_Leaf{
							Leaf: &openfgav1.UsersetTree_Leaf{
								Value: &openfgav1.UsersetTree_Leaf_Users{
									Users: &openfgav1.UsersetTree_Users{},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "self_defined_userset_returned_if_tuple_written",
			model: testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type group
				relations
					define viewer: [user, group#viewer]
			`),
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:1", "viewer", "group:1#viewer"),
			},
			request: &openfgav1.ExpandRequest{
				TupleKey: tuple.NewExpandRequestTupleKey("group:1", "viewer"),
			},
			expected: &openfgav1.ExpandResponse{
				Tree: &openfgav1.UsersetTree{
					Root: &openfgav1.UsersetTree_Node{
						Name: "group:1#viewer",
						Value: &openfgav1.UsersetTree_Node_Leaf{
							Leaf: &openfgav1.UsersetTree_Leaf{
								Value: &openfgav1.UsersetTree_Leaf_Users{
									Users: &openfgav1.UsersetTree_Users{
										Users: []string{"group:1#viewer"},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "cyclical_tuples",
			model: testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type group
				relations
					define viewer: [user, group#viewer]
			`),
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:2", "viewer", "group:3#viewer"),
				tuple.NewTupleKey("group:1", "viewer", "group:2#viewer"),
				tuple.NewTupleKey("group:3", "viewer", "group:1#viewer"),
			},
			request: &openfgav1.ExpandRequest{
				TupleKey: tuple.NewExpandRequestTupleKey("group:1", "viewer"),
			},
			expected: &openfgav1.ExpandResponse{
				Tree: &openfgav1.UsersetTree{
					Root: &openfgav1.UsersetTree_Node{
						Name: "group:1#viewer",
						Value: &openfgav1.UsersetTree_Node_Leaf{
							Leaf: &openfgav1.UsersetTree_Leaf{
								Value: &openfgav1.UsersetTree_Leaf_Users{
									Users: &openfgav1.UsersetTree_Users{
										Users: []string{"group:2#viewer"},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "nested_groups_I",
			model: testutils.MustTransformDSLToProtoWithID(`
			model
			  schema 1.1
			
			type employee
			  relations
				define can_manage: manager or can_manage from manager
				define manager: [employee]
			
			type report
			  relations
				define approver: can_manage from submitter
				define submitter: [employee]
			`),
			tuples: []*openfgav1.TupleKey{
				// employee:d has no manager
				tuple.NewTupleKey("employee:c", "manager", "employee:d"),
				tuple.NewTupleKey("employee:b", "manager", "employee:c"),
				tuple.NewTupleKey("employee:a", "manager", "employee:b"),
			},
			request: &openfgav1.ExpandRequest{
				TupleKey: tuple.NewExpandRequestTupleKey("employee:d", "manager"),
			},
			expected: &openfgav1.ExpandResponse{
				Tree: &openfgav1.UsersetTree{
					Root: &openfgav1.UsersetTree_Node{
						Name: "employee:d#manager",
						Value: &openfgav1.UsersetTree_Node_Leaf{
							Leaf: &openfgav1.UsersetTree_Leaf{
								Value: &openfgav1.UsersetTree_Leaf_Users{
									Users: &openfgav1.UsersetTree_Users{
										// employee:d has no manager
										Users: []string{},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "nested_groups_II",
			model: testutils.MustTransformDSLToProtoWithID(`
			model
			  schema 1.1
			
			type employee
			  relations
				define can_manage: manager or can_manage from manager
				define manager: [employee]
			
			type report
			  relations
				define approver: can_manage from submitter
				define submitter: [employee]
			`),
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("employee:c", "manager", "employee:d"),
				tuple.NewTupleKey("employee:b", "manager", "employee:c"),
				tuple.NewTupleKey("employee:a", "manager", "employee:b"),
			},
			request: &openfgav1.ExpandRequest{
				TupleKey: tuple.NewExpandRequestTupleKey("employee:c", "manager"),
			},
			expected: &openfgav1.ExpandResponse{
				Tree: &openfgav1.UsersetTree{
					Root: &openfgav1.UsersetTree_Node{
						Name: "employee:c#manager",
						Value: &openfgav1.UsersetTree_Node_Leaf{
							Leaf: &openfgav1.UsersetTree_Leaf{
								Value: &openfgav1.UsersetTree_Leaf_Users{
									Users: &openfgav1.UsersetTree_Users{
										Users: []string{"employee:d"},
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

			ts, err := typesystem.NewAndValidate(ctx, test.model)
			ctx = typesystem.ContextWithTypesystem(ctx, ts)
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

			ts, err := typesystem.NewAndValidate(ctx, test.model)
			ctx = typesystem.ContextWithTypesystem(ctx, ts)
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
