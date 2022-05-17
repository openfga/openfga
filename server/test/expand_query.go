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
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/server/queries"
	"github.com/openfga/openfga/storage"
	teststorage "github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
	"go.buf.build/openfga/go/openfga/api/openfga"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/testing/protocmp"
)

func setUp(ctx context.Context, store string, backend storage.TupleBackend, authModelBackend storage.AuthorizationModelBackend, typeDefinitions *openfgav1pb.TypeDefinitions, tuples []*openfga.TupleKey) (string, error) {
	modelID, err := id.NewString()
	if err != nil {
		return "", err
	}
	if err := authModelBackend.WriteAuthorizationModel(ctx, store, modelID, typeDefinitions); err != nil {
		return "", err
	}
	if err := backend.Write(ctx, store, []*openfga.TupleKey{}, tuples); err != nil {
		return "", err
	}
	return modelID, nil
}

func TestExpandQuery(t *testing.T, dbTester teststorage.DatastoreTester) {
	tests := []struct {
		name            string
		typeDefinitions *openfgav1pb.TypeDefinitions
		tuples          []*openfga.TupleKey
		request         *openfgav1pb.ExpandRequest
		expected        *openfgav1pb.ExpandResponse
	}{
		{
			name: "simple direct",
			typeDefinitions: &openfgav1pb.TypeDefinitions{
				TypeDefinitions: []*openfgav1pb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgav1pb.Userset{
							"admin": {},
						},
					},
				},
			},
			tuples: []*openfga.TupleKey{
				{
					Object:   "repo:auth0/foo",
					Relation: "admin",
					User:     "github|jon.allie@auth0.com",
				},
			},
			request: &openfgav1pb.ExpandRequest{
				TupleKey: &openfga.TupleKey{
					Object:   "repo:auth0/foo",
					Relation: "admin",
				},
			},
			expected: &openfgav1pb.ExpandResponse{
				Tree: &openfga.UsersetTree{
					Root: &openfga.UsersetTree_Node{
						Name: "repo:auth0/foo#admin",
						Value: &openfga.UsersetTree_Node_Leaf{
							Leaf: &openfga.UsersetTree_Leaf{
								Value: &openfga.UsersetTree_Leaf_Users{
									Users: &openfga.UsersetTree_Users{
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
			typeDefinitions: &openfgav1pb.TypeDefinitions{
				TypeDefinitions: []*openfgav1pb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgav1pb.Userset{
							"admin": {},
							"writer": {
								Userset: &openfgav1pb.Userset_ComputedUserset{
									ComputedUserset: &openfgav1pb.ObjectRelation{
										Relation: "admin",
									},
								},
							},
						},
					},
				},
			},
			tuples: []*openfga.TupleKey{},
			request: &openfgav1pb.ExpandRequest{
				TupleKey: &openfga.TupleKey{
					Object:   "repo:auth0/foo",
					Relation: "writer",
				},
			},
			expected: &openfgav1pb.ExpandResponse{
				Tree: &openfga.UsersetTree{
					Root: &openfga.UsersetTree_Node{
						Name: "repo:auth0/foo#writer",
						Value: &openfga.UsersetTree_Node_Leaf{
							Leaf: &openfga.UsersetTree_Leaf{
								Value: &openfga.UsersetTree_Leaf_Computed{
									Computed: &openfga.UsersetTree_Computed{
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
			typeDefinitions: &openfgav1pb.TypeDefinitions{
				TypeDefinitions: []*openfgav1pb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgav1pb.Userset{
							"admin": {
								Userset: &openfgav1pb.Userset_TupleToUserset{
									TupleToUserset: &openfgav1pb.TupleToUserset{
										Tupleset: &openfgav1pb.ObjectRelation{
											Relation: "manager",
										},
										ComputedUserset: &openfgav1pb.ObjectRelation{
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
						Relations: map[string]*openfgav1pb.Userset{
							"repo_admin": {},
						},
					},
				},
			},
			tuples: []*openfga.TupleKey{
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
			request: &openfgav1pb.ExpandRequest{
				TupleKey: &openfga.TupleKey{
					Object:   "repo:auth0/foo",
					Relation: "admin",
				},
			},
			expected: &openfgav1pb.ExpandResponse{
				Tree: &openfga.UsersetTree{
					Root: &openfga.UsersetTree_Node{
						Name: "repo:auth0/foo#admin",
						Value: &openfga.UsersetTree_Node_Leaf{
							Leaf: &openfga.UsersetTree_Leaf{
								Value: &openfga.UsersetTree_Leaf_TupleToUserset{
									TupleToUserset: &openfga.UsersetTree_TupleToUserset{
										Tupleset: "repo:auth0/foo#manager",
										Computed: []*openfga.UsersetTree_Computed{
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
			typeDefinitions: &openfgav1pb.TypeDefinitions{
				TypeDefinitions: []*openfgav1pb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgav1pb.Userset{
							"admin": {
								Userset: &openfgav1pb.Userset_TupleToUserset{
									TupleToUserset: &openfgav1pb.TupleToUserset{
										Tupleset: &openfgav1pb.ObjectRelation{
											Relation: "manager",
										},
										ComputedUserset: &openfgav1pb.ObjectRelation{
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
						Relations: map[string]*openfgav1pb.Userset{
							"repo_admin": {},
						},
					},
				},
			},
			tuples: []*openfga.TupleKey{
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
			request: &openfgav1pb.ExpandRequest{
				TupleKey: &openfga.TupleKey{
					Object:   "repo:auth0/foo",
					Relation: "admin",
				},
			},
			expected: &openfgav1pb.ExpandResponse{
				Tree: &openfga.UsersetTree{
					Root: &openfga.UsersetTree_Node{
						Name: "repo:auth0/foo#admin",
						Value: &openfga.UsersetTree_Node_Leaf{
							Leaf: &openfga.UsersetTree_Leaf{
								Value: &openfga.UsersetTree_Leaf_TupleToUserset{
									TupleToUserset: &openfga.UsersetTree_TupleToUserset{
										Tupleset: "repo:auth0/foo#manager",
										Computed: []*openfga.UsersetTree_Computed{
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
			typeDefinitions: &openfgav1pb.TypeDefinitions{
				TypeDefinitions: []*openfgav1pb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgav1pb.Userset{
							"admin": {
								Userset: &openfgav1pb.Userset_TupleToUserset{
									TupleToUserset: &openfgav1pb.TupleToUserset{
										Tupleset: &openfgav1pb.ObjectRelation{
											Relation: "manager",
										},
										ComputedUserset: &openfgav1pb.ObjectRelation{
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
						Relations: map[string]*openfgav1pb.Userset{
							"repo_admin": {},
						},
					},
				},
			},
			tuples: []*openfga.TupleKey{
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
			request: &openfgav1pb.ExpandRequest{
				TupleKey: &openfga.TupleKey{
					Object:   "repo:auth0/foo",
					Relation: "admin",
				},
			},
			expected: &openfgav1pb.ExpandResponse{
				Tree: &openfga.UsersetTree{
					Root: &openfga.UsersetTree_Node{
						Name: "repo:auth0/foo#admin",
						Value: &openfga.UsersetTree_Node_Leaf{
							Leaf: &openfga.UsersetTree_Leaf{
								Value: &openfga.UsersetTree_Leaf_TupleToUserset{
									TupleToUserset: &openfga.UsersetTree_TupleToUserset{
										Tupleset: "repo:auth0/foo#manager",
										Computed: []*openfga.UsersetTree_Computed{
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
			typeDefinitions: &openfgav1pb.TypeDefinitions{
				TypeDefinitions: []*openfgav1pb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgav1pb.Userset{
							"admin": {},
							"writer": {
								Userset: &openfgav1pb.Userset_Union{
									Union: &openfgav1pb.Usersets{
										Child: []*openfgav1pb.Userset{
											{
												Userset: &openfgav1pb.Userset_This{
													This: &openfgav1pb.DirectUserset{},
												},
											},
											{
												Userset: &openfgav1pb.Userset_ComputedUserset{
													ComputedUserset: &openfgav1pb.ObjectRelation{
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
			tuples: []*openfga.TupleKey{
				{
					Object:   "repo:auth0/foo",
					Relation: "writer",
					User:     "github|jon.allie@auth0.com",
				},
			},
			request: &openfgav1pb.ExpandRequest{
				TupleKey: &openfga.TupleKey{
					Object:   "repo:auth0/foo",
					Relation: "writer",
				},
			},
			expected: &openfgav1pb.ExpandResponse{
				Tree: &openfga.UsersetTree{
					Root: &openfga.UsersetTree_Node{
						Name: "repo:auth0/foo#writer",
						Value: &openfga.UsersetTree_Node_Union{
							Union: &openfga.UsersetTree_Nodes{
								Nodes: []*openfga.UsersetTree_Node{
									{
										Name: "repo:auth0/foo#writer",
										Value: &openfga.UsersetTree_Node_Leaf{
											Leaf: &openfga.UsersetTree_Leaf{
												Value: &openfga.UsersetTree_Leaf_Users{
													Users: &openfga.UsersetTree_Users{
														Users: []string{"github|jon.allie@auth0.com"},
													},
												},
											},
										},
									},
									{
										Name: "repo:auth0/foo#writer",
										Value: &openfga.UsersetTree_Node_Leaf{
											Leaf: &openfga.UsersetTree_Leaf{
												Value: &openfga.UsersetTree_Leaf_Computed{
													Computed: &openfga.UsersetTree_Computed{
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
			typeDefinitions: &openfgav1pb.TypeDefinitions{
				TypeDefinitions: []*openfgav1pb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgav1pb.Userset{
							"admin":  {},
							"banned": {},
							"active_admin": {
								Userset: &openfgav1pb.Userset_Difference{
									Difference: &openfgav1pb.Difference{
										Base: &openfgav1pb.Userset{
											Userset: &openfgav1pb.Userset_ComputedUserset{
												ComputedUserset: &openfgav1pb.ObjectRelation{
													Relation: "admin",
												},
											},
										},
										Subtract: &openfgav1pb.Userset{
											Userset: &openfgav1pb.Userset_ComputedUserset{
												ComputedUserset: &openfgav1pb.ObjectRelation{
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
			tuples: []*openfga.TupleKey{},
			request: &openfgav1pb.ExpandRequest{
				TupleKey: &openfga.TupleKey{
					Object:   "repo:auth0/foo",
					Relation: "active_admin",
				},
			},
			expected: &openfgav1pb.ExpandResponse{
				Tree: &openfga.UsersetTree{
					Root: &openfga.UsersetTree_Node{
						Name: "repo:auth0/foo#active_admin",
						Value: &openfga.UsersetTree_Node_Difference{
							Difference: &openfga.UsersetTree_Difference{
								Base: &openfga.UsersetTree_Node{
									Name: "repo:auth0/foo#active_admin",
									Value: &openfga.UsersetTree_Node_Leaf{
										Leaf: &openfga.UsersetTree_Leaf{
											Value: &openfga.UsersetTree_Leaf_Computed{
												Computed: &openfga.UsersetTree_Computed{
													Userset: "repo:auth0/foo#admin",
												},
											},
										},
									},
								},
								Subtract: &openfga.UsersetTree_Node{
									Name: "repo:auth0/foo#active_admin",
									Value: &openfga.UsersetTree_Node_Leaf{
										Leaf: &openfga.UsersetTree_Leaf{
											Value: &openfga.UsersetTree_Leaf_Computed{
												Computed: &openfga.UsersetTree_Computed{
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
			typeDefinitions: &openfgav1pb.TypeDefinitions{
				TypeDefinitions: []*openfgav1pb.TypeDefinition{
					{
						// Writers must be both directly in 'writers', and in 'admins'
						Type: "repo",
						Relations: map[string]*openfgav1pb.Userset{
							"admin": {},
							"writer": {
								Userset: &openfgav1pb.Userset_Intersection{
									Intersection: &openfgav1pb.Usersets{
										Child: []*openfgav1pb.Userset{
											{
												Userset: &openfgav1pb.Userset_This{
													This: &openfgav1pb.DirectUserset{},
												},
											},
											{
												Userset: &openfgav1pb.Userset_ComputedUserset{
													ComputedUserset: &openfgav1pb.ObjectRelation{
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
			tuples: []*openfga.TupleKey{},
			request: &openfgav1pb.ExpandRequest{
				TupleKey: &openfga.TupleKey{
					Object:   "repo:auth0/foo",
					Relation: "writer",
				},
			},
			expected: &openfgav1pb.ExpandResponse{
				Tree: &openfga.UsersetTree{
					Root: &openfga.UsersetTree_Node{
						Name: "repo:auth0/foo#writer",
						Value: &openfga.UsersetTree_Node_Intersection{
							Intersection: &openfga.UsersetTree_Nodes{
								Nodes: []*openfga.UsersetTree_Node{
									{
										Name: "repo:auth0/foo#writer",
										Value: &openfga.UsersetTree_Node_Leaf{
											Leaf: &openfga.UsersetTree_Leaf{
												Value: &openfga.UsersetTree_Leaf_Users{
													Users: &openfga.UsersetTree_Users{
														Users: []string{},
													},
												},
											},
										},
									},
									{
										Name: "repo:auth0/foo#writer",
										Value: &openfga.UsersetTree_Node_Leaf{
											Leaf: &openfga.UsersetTree_Leaf{
												Value: &openfga.UsersetTree_Leaf_Computed{
													Computed: &openfga.UsersetTree_Computed{
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
			typeDefinitions: &openfgav1pb.TypeDefinitions{
				// Users can write if they are direct members of writers, or repo_writers
				// in the org, unless they are also in banned_writers
				TypeDefinitions: []*openfgav1pb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgav1pb.Userset{
							"admin":         {},
							"owner":         {},
							"banned_writer": {},
							"writer": {
								Userset: &openfgav1pb.Userset_Difference{
									Difference: &openfgav1pb.Difference{
										Base: &openfgav1pb.Userset{
											Userset: &openfgav1pb.Userset_Union{
												Union: &openfgav1pb.Usersets{
													Child: []*openfgav1pb.Userset{
														{
															Userset: &openfgav1pb.Userset_This{
																This: &openfgav1pb.DirectUserset{},
															},
														},
														{
															Userset: &openfgav1pb.Userset_TupleToUserset{
																TupleToUserset: &openfgav1pb.TupleToUserset{
																	Tupleset: &openfgav1pb.ObjectRelation{
																		Relation: "owner",
																	},
																	ComputedUserset: &openfgav1pb.ObjectRelation{
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
										Subtract: &openfgav1pb.Userset{
											Userset: &openfgav1pb.Userset_ComputedUserset{
												ComputedUserset: &openfgav1pb.ObjectRelation{
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
						Relations: map[string]*openfgav1pb.Userset{
							"repo_writer": {},
						},
					},
				},
			},
			tuples: []*openfga.TupleKey{
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
			request: &openfgav1pb.ExpandRequest{
				TupleKey: &openfga.TupleKey{
					Object:   "repo:auth0/foo",
					Relation: "writer",
				},
			},
			expected: &openfgav1pb.ExpandResponse{
				Tree: &openfga.UsersetTree{
					Root: &openfga.UsersetTree_Node{
						Name: "repo:auth0/foo#writer",
						Value: &openfga.UsersetTree_Node_Difference{
							Difference: &openfga.UsersetTree_Difference{
								Base: &openfga.UsersetTree_Node{
									Name: "repo:auth0/foo#writer",
									Value: &openfga.UsersetTree_Node_Union{
										Union: &openfga.UsersetTree_Nodes{
											Nodes: []*openfga.UsersetTree_Node{
												{
													Name: "repo:auth0/foo#writer",
													Value: &openfga.UsersetTree_Node_Leaf{
														Leaf: &openfga.UsersetTree_Leaf{
															Value: &openfga.UsersetTree_Leaf_Users{
																Users: &openfga.UsersetTree_Users{
																	Users: []string{"github|jon.allie@auth0.com"},
																},
															},
														},
													},
												},
												{
													Name: "repo:auth0/foo#writer",
													Value: &openfga.UsersetTree_Node_Leaf{
														Leaf: &openfga.UsersetTree_Leaf{
															Value: &openfga.UsersetTree_Leaf_TupleToUserset{
																TupleToUserset: &openfga.UsersetTree_TupleToUserset{
																	Tupleset: "repo:auth0/foo#owner",
																	Computed: []*openfga.UsersetTree_Computed{
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
								Subtract: &openfga.UsersetTree_Node{
									Name: "repo:auth0/foo#writer",
									Value: &openfga.UsersetTree_Node_Leaf{
										Leaf: &openfga.UsersetTree_Leaf{
											Value: &openfga.UsersetTree_Leaf_Computed{
												Computed: &openfga.UsersetTree_Computed{
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

	datastore, err := dbTester.New()
	require.NoError(err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := testutils.CreateRandomString(20)
			modelID, err := setUp(ctx, store, datastore, datastore, test.typeDefinitions, test.tuples)
			if err != nil {
				t.Fatal(err)
			}
			query := queries.NewExpandQuery(datastore, datastore, tracer, logger)
			test.request.StoreId = store
			test.request.AuthorizationModelId = modelID
			got, err := query.Execute(ctx, test.request)
			if err != nil {
				t.Fatalf("%s: Execute() err = %v, want nil", test.name, err)
			}
			if diff := cmp.Diff(test.expected, got, protocmp.Transform()); diff != "" {
				t.Fatalf("%s: Execute() (-want, +got):\n%s", test.name, diff)
			}
		})
	}
}

func TestExpandQueryErrors(t *testing.T, dbTester teststorage.DatastoreTester) {
	tests := []struct {
		name            string
		typeDefinitions *openfgav1pb.TypeDefinitions
		tuples          []*openfga.TupleKey
		request         *openfgav1pb.ExpandRequest
		expected        error
	}{
		{
			name: "missing object",
			request: &openfgav1pb.ExpandRequest{
				TupleKey: &openfga.TupleKey{
					Relation: "bar",
				},
			},
			expected: serverErrors.InvalidExpandInput,
		},
		{
			name: "missing object id and type",
			request: &openfgav1pb.ExpandRequest{
				TupleKey: &openfga.TupleKey{
					Object:   ":",
					Relation: "bar",
				},
			},
			expected: serverErrors.InvalidObjectFormat(&openfga.TupleKey{
				Object:   ":",
				Relation: "bar",
			}),
		},
		{
			name: "missing object id",
			request: &openfgav1pb.ExpandRequest{
				TupleKey: &openfga.TupleKey{
					Object:   "github:",
					Relation: "bar",
				},
			},
			expected: serverErrors.InvalidObjectFormat(&openfga.TupleKey{
				Object:   "github:",
				Relation: "bar",
			}),
		},
		{
			name: "missing relation",
			request: &openfgav1pb.ExpandRequest{
				TupleKey: &openfga.TupleKey{
					Object: "bar",
				},
			},
			expected: serverErrors.InvalidExpandInput,
		},
		{
			name: "type not found",
			request: &openfgav1pb.ExpandRequest{
				TupleKey: &openfga.TupleKey{
					Object:   "foo:bar",
					Relation: "baz",
				},
			},
			expected: serverErrors.TypeNotFound("foo"),
		},
		{
			name: "relation not found",
			typeDefinitions: &openfgav1pb.TypeDefinitions{
				TypeDefinitions: []*openfgav1pb.TypeDefinition{
					{
						Type: "repo",
					},
				},
			},
			request: &openfgav1pb.ExpandRequest{
				TupleKey: &openfga.TupleKey{
					Object:   "repo:bar",
					Relation: "baz",
				},
			},
			expected: serverErrors.RelationNotFound("baz", "repo", &openfga.TupleKey{
				Object:   "repo:bar",
				Relation: "baz",
			}),
		},
	}

	require := require.New(t)
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	datastore, err := dbTester.New()
	require.NoError(err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := testutils.CreateRandomString(20)

			modelID, err := setUp(ctx, store, datastore, datastore, test.typeDefinitions, test.tuples)
			if err != nil {
				t.Fatalf("'%s': setUp() error was %s, want nil", test.name, err)
			}

			query := queries.NewExpandQuery(datastore, datastore, tracer, logger)
			test.request.StoreId = store
			test.request.AuthorizationModelId = modelID

			_, err = query.Execute(ctx, test.request)
			if !errors.Is(err, test.expected) {
				t.Fatalf("'%s': Execute(), err = %v, want %v", test.name, err, test.expected)
			}
		})
	}
}
