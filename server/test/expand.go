package test

import (
	"context"
	"testing"

	"github.com/go-errors/errors"
	"github.com/google/go-cmp/cmp"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	teststorage "github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/testing/protocmp"
)

func setUp(ctx context.Context, store string, datastore storage.OpenFGADatastore, typeDefinitions *openfgapb.TypeDefinitions, tuples []*openfgapb.TupleKey) (string, error) {
	modelID, err := id.NewString()
	if err != nil {
		return "", err
	}
	if err := datastore.WriteAuthorizationModel(ctx, store, modelID, typeDefinitions); err != nil {
		return "", err
	}
	if err := datastore.Write(ctx, store, []*openfgapb.TupleKey{}, tuples); err != nil {
		return "", err
	}
	return modelID, nil
}

func TestExpandQuery(t *testing.T, dc teststorage.DatastoreConstructor[storage.OpenFGADatastore]) {
	tests := []struct {
		name            string
		typeDefinitions *openfgapb.TypeDefinitions
		tuples          []*openfgapb.TupleKey
		request         *openfgapb.ExpandRequest
		expected        *openfgapb.ExpandResponse
	}{
		{
			name: "simple direct",
			typeDefinitions: &openfgapb.TypeDefinitions{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin": {},
						},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				{
					Object:   "repo:auth0/foo",
					Relation: "admin",
					User:     "github|jon.allie@auth0.com",
				},
			},
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:auth0/foo",
					Relation: "admin",
				},
			},
			expected: &openfgapb.ExpandResponse{
				Tree: &openfgapb.UsersetTree{
					Root: &openfgapb.UsersetTree_Node{
						Name: "repo:auth0/foo#admin",
						Value: &openfgapb.UsersetTree_Node_Leaf{
							Leaf: &openfgapb.UsersetTree_Leaf{
								Value: &openfgapb.UsersetTree_Leaf_Users{
									Users: &openfgapb.UsersetTree_Users{
										Users: []string{"github|jon.allie@auth0.com"},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "computed userset",
			typeDefinitions: &openfgapb.TypeDefinitions{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin": {},
							"writer": {
								Userset: &openfgapb.Userset_ComputedUserset{
									ComputedUserset: &openfgapb.ObjectRelation{
										Relation: "admin",
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
					Object:   "repo:auth0/foo",
					Relation: "writer",
				},
			},
			expected: &openfgapb.ExpandResponse{
				Tree: &openfgapb.UsersetTree{
					Root: &openfgapb.UsersetTree_Node{
						Name: "repo:auth0/foo#writer",
						Value: &openfgapb.UsersetTree_Node_Leaf{
							Leaf: &openfgapb.UsersetTree_Leaf{
								Value: &openfgapb.UsersetTree_Leaf_Computed{
									Computed: &openfgapb.UsersetTree_Computed{
										Userset: "repo:auth0/foo#admin",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "tuple to userset",
			typeDefinitions: &openfgapb.TypeDefinitions{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin": {
								Userset: &openfgapb.Userset_TupleToUserset{
									TupleToUserset: &openfgapb.TupleToUserset{
										Tupleset: &openfgapb.ObjectRelation{
											Relation: "manager",
										},
										ComputedUserset: &openfgapb.ObjectRelation{
											Object:   "$TUPLE_USERSET_OBJECT",
											Relation: "repo_admin",
										},
									},
								},
							},
							"manager": {},
						},
					},
					{
						Type: "org",
						Relations: map[string]*openfgapb.Userset{
							"repo_admin": {},
						},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				{
					Object:   "repo:auth0/foo",
					Relation: "manager",
					User:     "org:auth0",
				},
				{
					Object:   "org:auth0",
					Relation: "repo_admin",
					User:     "github|jon.allie@auth0.com",
				},
			},
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:auth0/foo",
					Relation: "admin",
				},
			},
			expected: &openfgapb.ExpandResponse{
				Tree: &openfgapb.UsersetTree{
					Root: &openfgapb.UsersetTree_Node{
						Name: "repo:auth0/foo#admin",
						Value: &openfgapb.UsersetTree_Node_Leaf{
							Leaf: &openfgapb.UsersetTree_Leaf{
								Value: &openfgapb.UsersetTree_Leaf_TupleToUserset{
									TupleToUserset: &openfgapb.UsersetTree_TupleToUserset{
										Tupleset: "repo:auth0/foo#manager",
										Computed: []*openfgapb.UsersetTree_Computed{
											{
												Userset: "org:auth0#repo_admin",
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
			name: "tuple to userset II",
			typeDefinitions: &openfgapb.TypeDefinitions{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin": {
								Userset: &openfgapb.Userset_TupleToUserset{
									TupleToUserset: &openfgapb.TupleToUserset{
										Tupleset: &openfgapb.ObjectRelation{
											Relation: "manager",
										},
										ComputedUserset: &openfgapb.ObjectRelation{
											Object:   "$TUPLE_USERSET_OBJECT",
											Relation: "repo_admin",
										},
									},
								},
							},
							"manager": {},
						},
					},
					{
						Type: "org",
						Relations: map[string]*openfgapb.Userset{
							"repo_admin": {},
						},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				{
					Object:   "repo:auth0/foo",
					Relation: "manager",
					User:     "org:auth0",
				},
				{
					Object:   "org:auth0",
					Relation: "repo_admin",
					User:     "github|jon.allie@auth0.com",
				},
				{
					Object:   "repo:auth0/foo",
					Relation: "manager",
					User:     "amy",
				},
			},
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:auth0/foo",
					Relation: "admin",
				},
			},
			expected: &openfgapb.ExpandResponse{
				Tree: &openfgapb.UsersetTree{
					Root: &openfgapb.UsersetTree_Node{
						Name: "repo:auth0/foo#admin",
						Value: &openfgapb.UsersetTree_Node_Leaf{
							Leaf: &openfgapb.UsersetTree_Leaf{
								Value: &openfgapb.UsersetTree_Leaf_TupleToUserset{
									TupleToUserset: &openfgapb.UsersetTree_TupleToUserset{
										Tupleset: "repo:auth0/foo#manager",
										Computed: []*openfgapb.UsersetTree_Computed{
											{
												Userset: "org:auth0#repo_admin",
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
			name: "tuple to userset implicit",
			typeDefinitions: &openfgapb.TypeDefinitions{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin": {
								Userset: &openfgapb.Userset_TupleToUserset{
									TupleToUserset: &openfgapb.TupleToUserset{
										Tupleset: &openfgapb.ObjectRelation{
											Relation: "manager",
										},
										ComputedUserset: &openfgapb.ObjectRelation{
											Relation: "repo_admin",
										},
									},
								},
							},
							"manager": {},
						},
					},
					{
						Type: "org",
						Relations: map[string]*openfgapb.Userset{
							"repo_admin": {},
						},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				{
					Object:   "repo:auth0/foo",
					Relation: "manager",
					User:     "org:auth0",
				},
				{
					Object:   "org:auth0",
					Relation: "repo_admin",
					User:     "github|jon.allie@auth0.com",
				},
			},
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:auth0/foo",
					Relation: "admin",
				},
			},
			expected: &openfgapb.ExpandResponse{
				Tree: &openfgapb.UsersetTree{
					Root: &openfgapb.UsersetTree_Node{
						Name: "repo:auth0/foo#admin",
						Value: &openfgapb.UsersetTree_Node_Leaf{
							Leaf: &openfgapb.UsersetTree_Leaf{
								Value: &openfgapb.UsersetTree_Leaf_TupleToUserset{
									TupleToUserset: &openfgapb.UsersetTree_TupleToUserset{
										Tupleset: "repo:auth0/foo#manager",
										Computed: []*openfgapb.UsersetTree_Computed{
											{
												Userset: "org:auth0#repo_admin",
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
			name: "simple union",
			typeDefinitions: &openfgapb.TypeDefinitions{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin": {},
							"writer": {
								Userset: &openfgapb.Userset_Union{
									Union: &openfgapb.Usersets{
										Child: []*openfgapb.Userset{
											{
												Userset: &openfgapb.Userset_This{
													This: &openfgapb.DirectUserset{},
												},
											},
											{
												Userset: &openfgapb.Userset_ComputedUserset{
													ComputedUserset: &openfgapb.ObjectRelation{
														Relation: "admin",
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
			tuples: []*openfgapb.TupleKey{
				{
					Object:   "repo:auth0/foo",
					Relation: "writer",
					User:     "github|jon.allie@auth0.com",
				},
			},
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:auth0/foo",
					Relation: "writer",
				},
			},
			expected: &openfgapb.ExpandResponse{
				Tree: &openfgapb.UsersetTree{
					Root: &openfgapb.UsersetTree_Node{
						Name: "repo:auth0/foo#writer",
						Value: &openfgapb.UsersetTree_Node_Union{
							Union: &openfgapb.UsersetTree_Nodes{
								Nodes: []*openfgapb.UsersetTree_Node{
									{
										Name: "repo:auth0/foo#writer",
										Value: &openfgapb.UsersetTree_Node_Leaf{
											Leaf: &openfgapb.UsersetTree_Leaf{
												Value: &openfgapb.UsersetTree_Leaf_Users{
													Users: &openfgapb.UsersetTree_Users{
														Users: []string{"github|jon.allie@auth0.com"},
													},
												},
											},
										},
									},
									{
										Name: "repo:auth0/foo#writer",
										Value: &openfgapb.UsersetTree_Node_Leaf{
											Leaf: &openfgapb.UsersetTree_Leaf{
												Value: &openfgapb.UsersetTree_Leaf_Computed{
													Computed: &openfgapb.UsersetTree_Computed{
														Userset: "repo:auth0/foo#admin",
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
			name: "simple difference",
			typeDefinitions: &openfgapb.TypeDefinitions{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin":  {},
							"banned": {},
							"active_admin": {
								Userset: &openfgapb.Userset_Difference{
									Difference: &openfgapb.Difference{
										Base: &openfgapb.Userset{
											Userset: &openfgapb.Userset_ComputedUserset{
												ComputedUserset: &openfgapb.ObjectRelation{
													Relation: "admin",
												},
											},
										},
										Subtract: &openfgapb.Userset{
											Userset: &openfgapb.Userset_ComputedUserset{
												ComputedUserset: &openfgapb.ObjectRelation{
													Relation: "banned",
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
			tuples: []*openfgapb.TupleKey{},
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:auth0/foo",
					Relation: "active_admin",
				},
			},
			expected: &openfgapb.ExpandResponse{
				Tree: &openfgapb.UsersetTree{
					Root: &openfgapb.UsersetTree_Node{
						Name: "repo:auth0/foo#active_admin",
						Value: &openfgapb.UsersetTree_Node_Difference{
							Difference: &openfgapb.UsersetTree_Difference{
								Base: &openfgapb.UsersetTree_Node{
									Name: "repo:auth0/foo#active_admin",
									Value: &openfgapb.UsersetTree_Node_Leaf{
										Leaf: &openfgapb.UsersetTree_Leaf{
											Value: &openfgapb.UsersetTree_Leaf_Computed{
												Computed: &openfgapb.UsersetTree_Computed{
													Userset: "repo:auth0/foo#admin",
												},
											},
										},
									},
								},
								Subtract: &openfgapb.UsersetTree_Node{
									Name: "repo:auth0/foo#active_admin",
									Value: &openfgapb.UsersetTree_Node_Leaf{
										Leaf: &openfgapb.UsersetTree_Leaf{
											Value: &openfgapb.UsersetTree_Leaf_Computed{
												Computed: &openfgapb.UsersetTree_Computed{
													Userset: "repo:auth0/foo#banned",
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
			name: "simple intersection",
			typeDefinitions: &openfgapb.TypeDefinitions{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						// Writers must be both directly in 'writers', and in 'admins'
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin": {},
							"writer": {
								Userset: &openfgapb.Userset_Intersection{
									Intersection: &openfgapb.Usersets{
										Child: []*openfgapb.Userset{
											{
												Userset: &openfgapb.Userset_This{
													This: &openfgapb.DirectUserset{},
												},
											},
											{
												Userset: &openfgapb.Userset_ComputedUserset{
													ComputedUserset: &openfgapb.ObjectRelation{
														Relation: "admin",
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
			tuples: []*openfgapb.TupleKey{},
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:auth0/foo",
					Relation: "writer",
				},
			},
			expected: &openfgapb.ExpandResponse{
				Tree: &openfgapb.UsersetTree{
					Root: &openfgapb.UsersetTree_Node{
						Name: "repo:auth0/foo#writer",
						Value: &openfgapb.UsersetTree_Node_Intersection{
							Intersection: &openfgapb.UsersetTree_Nodes{
								Nodes: []*openfgapb.UsersetTree_Node{
									{
										Name: "repo:auth0/foo#writer",
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
										Name: "repo:auth0/foo#writer",
										Value: &openfgapb.UsersetTree_Node_Leaf{
											Leaf: &openfgapb.UsersetTree_Leaf{
												Value: &openfgapb.UsersetTree_Leaf_Computed{
													Computed: &openfgapb.UsersetTree_Computed{
														Userset: "repo:auth0/foo#admin",
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
			name: "complex tree",
			typeDefinitions: &openfgapb.TypeDefinitions{
				// Users can write if they are direct members of writers, or repo_writers
				// in the org, unless they are also in banned_writers
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin":         {},
							"owner":         {},
							"banned_writer": {},
							"writer": {
								Userset: &openfgapb.Userset_Difference{
									Difference: &openfgapb.Difference{
										Base: &openfgapb.Userset{
											Userset: &openfgapb.Userset_Union{
												Union: &openfgapb.Usersets{
													Child: []*openfgapb.Userset{
														{
															Userset: &openfgapb.Userset_This{
																This: &openfgapb.DirectUserset{},
															},
														},
														{
															Userset: &openfgapb.Userset_TupleToUserset{
																TupleToUserset: &openfgapb.TupleToUserset{
																	Tupleset: &openfgapb.ObjectRelation{
																		Relation: "owner",
																	},
																	ComputedUserset: &openfgapb.ObjectRelation{
																		Object:   "$TUPLE_USERSET_OBJECT",
																		Relation: "repo_writer",
																	},
																},
															},
														},
													},
												},
											},
										},
										Subtract: &openfgapb.Userset{
											Userset: &openfgapb.Userset_ComputedUserset{
												ComputedUserset: &openfgapb.ObjectRelation{
													Relation: "banned_writer",
												},
											},
										},
									},
								},
							},
						},
					},
					{
						Type: "org",
						Relations: map[string]*openfgapb.Userset{
							"repo_writer": {},
						},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				{
					Object:   "repo:auth0/foo",
					Relation: "owner",
					User:     "org:auth0",
				},
				{
					Object:   "repo:auth0/foo",
					Relation: "writer",
					User:     "github|jon.allie@auth0.com",
				},
			},
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:auth0/foo",
					Relation: "writer",
				},
			},
			expected: &openfgapb.ExpandResponse{
				Tree: &openfgapb.UsersetTree{
					Root: &openfgapb.UsersetTree_Node{
						Name: "repo:auth0/foo#writer",
						Value: &openfgapb.UsersetTree_Node_Difference{
							Difference: &openfgapb.UsersetTree_Difference{
								Base: &openfgapb.UsersetTree_Node{
									Name: "repo:auth0/foo#writer",
									Value: &openfgapb.UsersetTree_Node_Union{
										Union: &openfgapb.UsersetTree_Nodes{
											Nodes: []*openfgapb.UsersetTree_Node{
												{
													Name: "repo:auth0/foo#writer",
													Value: &openfgapb.UsersetTree_Node_Leaf{
														Leaf: &openfgapb.UsersetTree_Leaf{
															Value: &openfgapb.UsersetTree_Leaf_Users{
																Users: &openfgapb.UsersetTree_Users{
																	Users: []string{"github|jon.allie@auth0.com"},
																},
															},
														},
													},
												},
												{
													Name: "repo:auth0/foo#writer",
													Value: &openfgapb.UsersetTree_Node_Leaf{
														Leaf: &openfgapb.UsersetTree_Leaf{
															Value: &openfgapb.UsersetTree_Leaf_TupleToUserset{
																TupleToUserset: &openfgapb.UsersetTree_TupleToUserset{
																	Tupleset: "repo:auth0/foo#owner",
																	Computed: []*openfgapb.UsersetTree_Computed{
																		{Userset: "org:auth0#repo_writer"},
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
									Name: "repo:auth0/foo#writer",
									Value: &openfgapb.UsersetTree_Node_Leaf{
										Leaf: &openfgapb.UsersetTree_Leaf{
											Value: &openfgapb.UsersetTree_Leaf_Computed{
												Computed: &openfgapb.UsersetTree_Computed{
													Userset: "repo:auth0/foo#banned_writer",
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
	}

	require := require.New(t)
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	datastore, err := dc.New()
	require.NoError(err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := testutils.CreateRandomString(20)
			modelID, err := setUp(ctx, store, datastore, test.typeDefinitions, test.tuples)
			require.NoError(err)

			query := commands.NewExpandQuery(datastore, tracer, logger)
			test.request.StoreId = store
			test.request.AuthorizationModelId = modelID
			got, err := query.Execute(ctx, test.request)
			require.NoError(err)

			if diff := cmp.Diff(test.expected, got, protocmp.Transform()); diff != "" {
				t.Fatalf("%s: Execute() (-want, +got):\n%s", test.name, diff)
			}
		})
	}
}

func TestExpandQueryErrors(t *testing.T, dc teststorage.DatastoreConstructor[storage.OpenFGADatastore]) {
	tests := []struct {
		name            string
		typeDefinitions *openfgapb.TypeDefinitions
		tuples          []*openfgapb.TupleKey
		request         *openfgapb.ExpandRequest
		expected        error
	}{
		{
			name: "missing object",
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Relation: "bar",
				},
			},
			expected: serverErrors.InvalidExpandInput,
		},
		{
			name: "missing object id and type",
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   ":",
					Relation: "bar",
				},
			},
			expected: serverErrors.InvalidObjectFormat(&openfgapb.TupleKey{
				Object:   ":",
				Relation: "bar",
			}),
		},
		{
			name: "missing object id",
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "github:",
					Relation: "bar",
				},
			},
			expected: serverErrors.InvalidObjectFormat(&openfgapb.TupleKey{
				Object:   "github:",
				Relation: "bar",
			}),
		},
		{
			name: "missing relation",
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object: "bar",
				},
			},
			expected: serverErrors.InvalidExpandInput,
		},
		{
			name: "type not found",
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "foo:bar",
					Relation: "baz",
				},
			},
			expected: serverErrors.TypeNotFound("foo"),
		},
		{
			name: "relation not found",
			typeDefinitions: &openfgapb.TypeDefinitions{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
					},
				},
			},
			request: &openfgapb.ExpandRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:bar",
					Relation: "baz",
				},
			},
			expected: serverErrors.RelationNotFound("baz", "repo", &openfgapb.TupleKey{
				Object:   "repo:bar",
				Relation: "baz",
			}),
		},
	}

	require := require.New(t)
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	datastore, err := dc.New()
	require.NoError(err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := testutils.CreateRandomString(20)

			modelID, err := setUp(ctx, store, datastore, test.typeDefinitions, test.tuples)
			require.NoError(err)

			query := commands.NewExpandQuery(datastore, tracer, logger)
			test.request.StoreId = store
			test.request.AuthorizationModelId = modelID

			_, err = query.Execute(ctx, test.request)
			if !errors.Is(err, test.expected) {
				t.Fatalf("'%s': Execute(), err = %v, want %v", test.name, err, test.expected)
			}
		})
	}
}
