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
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/testing/protocmp"
)

func setUp(ctx context.Context, store string, datastore storage.OpenFGADatastore, typeDefinitions []*openfgapb.TypeDefinition, tuples []*openfgapb.TupleKey) (string, error) {
	model := &openfgapb.AuthorizationModel{
		Id:              id.Must(id.New()).String(),
		SchemaVersion:   typesystem.SchemaVersion1_0,
		TypeDefinitions: typeDefinitions,
	}

	if err := datastore.WriteAuthorizationModel(ctx, store, model); err != nil {
		return "", err
	}

	if err := datastore.Write(ctx, store, []*openfgapb.TupleKey{}, tuples); err != nil {
		return "", err
	}

	return model.Id, nil
}

func TestExpandQuery(t *testing.T, datastore storage.OpenFGADatastore) {
	tests := []struct {
		name            string
		typeDefinitions []*openfgapb.TypeDefinition
		tuples          []*openfgapb.TupleKey
		request         *openfgapb.ExpandRequest
		expected        *openfgapb.ExpandResponse
	}{
		{
			name: "simple direct",
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"admin": {},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				{
					Object:   "repo:openfga/foo",
					Relation: "admin",
					User:     "github|jon.allie@openfga",
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
										Users: []string{"github|jon.allie@openfga"},
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
			typeDefinitions: []*openfgapb.TypeDefinition{
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
			name: "tuple to userset",
			typeDefinitions: []*openfgapb.TypeDefinition{
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
			tuples: []*openfgapb.TupleKey{
				{
					Object:   "repo:openfga/foo",
					Relation: "manager",
					User:     "org:openfga",
				},
				{
					Object:   "org:openfga",
					Relation: "repo_admin",
					User:     "github|jon.allie@openfga",
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
			name: "tuple to userset II",
			typeDefinitions: []*openfgapb.TypeDefinition{
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
			tuples: []*openfgapb.TupleKey{
				{
					Object:   "repo:openfga/foo",
					Relation: "manager",
					User:     "org:openfga",
				},
				{
					Object:   "org:openfga",
					Relation: "repo_admin",
					User:     "github|jon.allie@openfga",
				},
				{
					Object:   "repo:openfga/foo",
					Relation: "manager",
					User:     "amy",
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
			name: "tuple to userset implicit",
			typeDefinitions: []*openfgapb.TypeDefinition{
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
			tuples: []*openfgapb.TupleKey{
				{
					Object:   "repo:openfga/foo",
					Relation: "manager",
					User:     "org:openfga",
				},
				{
					Object:   "org:openfga",
					Relation: "repo_admin",
					User:     "github|jon.allie@openfga",
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
			name: "simple union",
			typeDefinitions: []*openfgapb.TypeDefinition{
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
			tuples: []*openfgapb.TupleKey{
				{
					Object:   "repo:openfga/foo",
					Relation: "writer",
					User:     "github|jon.allie@openfga",
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
														Users: []string{"github|jon.allie@openfga"},
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
			name: "simple difference",
			typeDefinitions: []*openfgapb.TypeDefinition{
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
			name: "simple intersection",
			typeDefinitions: []*openfgapb.TypeDefinition{
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
			name: "complex tree",
			// Users can write if they are direct members of writers, or repo_writers
			// in the org, unless they are also in banned_writers
			typeDefinitions: []*openfgapb.TypeDefinition{
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
			tuples: []*openfgapb.TupleKey{
				{
					Object:   "repo:openfga/foo",
					Relation: "owner",
					User:     "org:openfga",
				},
				{
					Object:   "repo:openfga/foo",
					Relation: "writer",
					User:     "github|jon.allie@openfga",
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
																	Users: []string{"github|jon.allie@openfga"},
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
	}

	require := require.New(t)
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

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

func TestExpandQueryErrors(t *testing.T, datastore storage.OpenFGADatastore) {
	tests := []struct {
		name            string
		typeDefinitions []*openfgapb.TypeDefinition
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
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
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
