package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/server/commands"
	"github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestWriteAuthorizationModel(t *testing.T, datastore storage.OpenFGADatastore) {
	type writeAuthorizationModelTestSettings struct {
		name    string
		request *openfgapb.WriteAuthorizationModelRequest
		err     error
	}

	ctx := context.Background()
	logger := logger.NewNoopLogger()

	items := make([]*openfgapb.TypeDefinition, datastore.MaxTypesInTypeDefinition()+1)
	for i := 0; i < datastore.MaxTypesInTypeDefinition(); i++ {
		items[i] = &openfgapb.TypeDefinition{
			Type: fmt.Sprintf("type%v", i),
			Relations: map[string]*openfgapb.Userset{
				"admin": {Userset: &openfgapb.Userset_This{}},
			},
		}
	}

	var tests = []writeAuthorizationModelTestSettings{
		{
			name: "succeeds",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: "somestoreid",
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgapb.Userset{
								"admin": {Userset: &openfgapb.Userset_This{}},
							},
						},
					},
				},
			},
		},
		{
			name: "succeeds part II",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: "somestoreid",
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "group",
							Relations: map[string]*openfgapb.Userset{
								"member": {Userset: &openfgapb.Userset_This{}},
							},
						},
						{
							Type: "document",
							Relations: map[string]*openfgapb.Userset{
								"owner": {Userset: &openfgapb.Userset_This{}},
								"reader": {
									Userset: &openfgapb.Userset_Union{
										Union: &openfgapb.Usersets{
											Child: []*openfgapb.Userset{
												{
													Userset: &openfgapb.Userset_This{},
												},
												{
													Userset: &openfgapb.Userset_ComputedUserset{
														ComputedUserset: &openfgapb.ObjectRelation{Relation: "writer"},
													},
												},
											},
										},
									},
								},
								"writer": {
									Userset: &openfgapb.Userset_Union{
										Union: &openfgapb.Usersets{
											Child: []*openfgapb.Userset{
												{
													Userset: &openfgapb.Userset_This{},
												},
												{
													Userset: &openfgapb.Userset_TupleToUserset{
														TupleToUserset: &openfgapb.TupleToUserset{
															Tupleset:        &openfgapb.ObjectRelation{Relation: "owner"},
															ComputedUserset: &openfgapb.ObjectRelation{Relation: "member"},
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
		},
		{
			name: "fails if too many types",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: "somestoreid",
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: items,
				},
			},
			err: errors.ExceededEntityLimit("type definitions in an authorization model", datastore.MaxTypesInTypeDefinition()),
		},
		{
			name: "empty relations is valid",
			request: &openfgapb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "repo",
						},
					},
				},
			},
		},
		{
			name: "zero length relations is valid",
			request: &openfgapb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type:      "repo",
							Relations: map[string]*openfgapb.Userset{},
						},
					},
				},
			},
		},
		{
			name: "ExecuteWriteFailsIfSameTypeTwice",
			request: &openfgapb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgapb.Userset{
								"admin": {Userset: &openfgapb.Userset_This{}},
							},
						},
						{
							Type: "repo",
							Relations: map[string]*openfgapb.Userset{
								"admin": {Userset: &openfgapb.Userset_This{}},
							},
						},
					},
				},
			},
			err: errors.CannotAllowDuplicateTypesInOneRequest,
		},
		{
			name: "ExecuteWriteFailsIfEmptyRewrites",
			request: &openfgapb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgapb.Userset{
								"owner": {},
							},
						},
					},
				},
			},
			err: errors.EmptyRewrites("repo", "owner"),
		},
		{
			name: "ExecuteWriteFailsIfUnknownRelationInComputedUserset",
			request: &openfgapb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgapb.Userset{
								"writer": {
									Userset: &openfgapb.Userset_ComputedUserset{
										ComputedUserset: &openfgapb.ObjectRelation{
											Object:   "",
											Relation: "owner",
										},
									},
								},
							},
						},
					},
				},
			},
			err: errors.RelationNotFound("owner", "", nil),
		},
		{
			name: "ExecuteWriteFailsIfUnknownRelationInTupleToUserset",
			request: &openfgapb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgapb.Userset{
								"writer": {Userset: &openfgapb.Userset_This{}},
								"viewer": {
									Userset: &openfgapb.Userset_TupleToUserset{
										TupleToUserset: &openfgapb.TupleToUserset{
											Tupleset: &openfgapb.ObjectRelation{
												Object:   "",
												Relation: "writer",
											},
											ComputedUserset: &openfgapb.ObjectRelation{
												Object:   "$TUPLE_USERSET_OBJECT",
												Relation: "owner",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			err: errors.RelationNotFound("owner", "", nil),
		},
		{
			name: "ExecuteWriteFailsIfUnknownRelationInUnion",
			request: &openfgapb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgapb.Userset{
								"writer": {Userset: &openfgapb.Userset_This{}},
								"viewer": {
									Userset: &openfgapb.Userset_Union{
										Union: &openfgapb.Usersets{
											Child: []*openfgapb.Userset{
												{Userset: &openfgapb.Userset_This{}},
												{Userset: &openfgapb.Userset_ComputedUserset{
													ComputedUserset: &openfgapb.ObjectRelation{
														Object:   "",
														Relation: "owner",
													},
												}},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			err: errors.RelationNotFound("owner", "", nil),
		},
		{
			name: "ExecuteWriteFailsIfUnknownRelationInDifferenceBaseArgument",
			request: &openfgapb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgapb.Userset{
								"writer": {Userset: &openfgapb.Userset_This{}},
								"viewer": {
									Userset: &openfgapb.Userset_Difference{
										Difference: &openfgapb.Difference{
											Base: &openfgapb.Userset{Userset: &openfgapb.Userset_ComputedUserset{
												ComputedUserset: &openfgapb.ObjectRelation{
													Object:   "",
													Relation: "writer",
												},
											}},
											Subtract: &openfgapb.Userset{Userset: &openfgapb.Userset_ComputedUserset{
												ComputedUserset: &openfgapb.ObjectRelation{
													Object:   "",
													Relation: "owner",
												},
											}},
										},
									},
								},
							},
						},
					},
				},
			},
			err: errors.RelationNotFound("owner", "", nil),
		},
		{
			name: "ExecuteWriteFailsIfUnknownRelationInDifferenceSubtractArgument",
			request: &openfgapb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgapb.Userset{
								"writer": {Userset: &openfgapb.Userset_This{}},
								"viewer": {
									Userset: &openfgapb.Userset_Difference{
										Difference: &openfgapb.Difference{
											Base: &openfgapb.Userset{Userset: &openfgapb.Userset_ComputedUserset{
												ComputedUserset: &openfgapb.ObjectRelation{
													Object:   "",
													Relation: "owner",
												},
											}},
											Subtract: &openfgapb.Userset{Userset: &openfgapb.Userset_ComputedUserset{
												ComputedUserset: &openfgapb.ObjectRelation{
													Object:   "",
													Relation: "writer",
												},
											}},
										},
									},
								},
							},
						},
					},
				},
			},
			err: errors.RelationNotFound("owner", "", nil),
		},
		{
			name: "ExecuteWriteFailsIfUnknownRelationInTupleToUsersetTupleset",
			request: &openfgapb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgapb.Userset{
								"writer": {Userset: &openfgapb.Userset_This{}},
								"viewer": {
									Userset: &openfgapb.Userset_TupleToUserset{
										TupleToUserset: &openfgapb.TupleToUserset{
											Tupleset: &openfgapb.ObjectRelation{
												Object:   "",
												Relation: "owner",
											},
											ComputedUserset: &openfgapb.ObjectRelation{
												Object:   "$TUPLE_USERSET_OBJECT",
												Relation: "from",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			err: errors.RelationNotFound("owner", "", nil),
		},
		{
			name: "ExecuteWriteFailsIfUnknownRelationInTupleToUsersetComputedUserset",
			request: &openfgapb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgapb.Userset{
								"writer": {Userset: &openfgapb.Userset_This{}},
								"viewer": {
									Userset: &openfgapb.Userset_TupleToUserset{
										TupleToUserset: &openfgapb.TupleToUserset{
											Tupleset: &openfgapb.ObjectRelation{
												Object:   "",
												Relation: "writer",
											},
											ComputedUserset: &openfgapb.ObjectRelation{
												Object:   "$TUPLE_USERSET_OBJECT",
												Relation: "owner",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			err: errors.RelationNotFound("owner", "", nil),
		},
		{
			name: "ExecuteWriteFailsIfTupleToUsersetReferencesUnknownRelation",
			request: &openfgapb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "foo",
							Relations: map[string]*openfgapb.Userset{
								"writer": {
									Userset: &openfgapb.Userset_ComputedUserset{
										ComputedUserset: &openfgapb.ObjectRelation{
											Object:   "",
											Relation: "reader",
										},
									},
								},
								"reader": {
									Userset: &openfgapb.Userset_This{},
								},
							},
						},
						{
							Type: "bar",
							Relations: map[string]*openfgapb.Userset{
								"owner": {
									Userset: &openfgapb.Userset_ComputedUserset{
										ComputedUserset: &openfgapb.ObjectRelation{
											Object:   "",
											Relation: "writer",
										},
									},
								},
							},
						},
					},
				},
			},
			err: errors.RelationNotFound("writer", "", nil),
		},
		{
			name: "ExecuteWriteFailsIfUnknownRelationInIntersection",
			request: &openfgapb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgapb.Userset{
								"writer": {Userset: &openfgapb.Userset_This{}},
								"viewer": {
									Userset: &openfgapb.Userset_Intersection{
										Intersection: &openfgapb.Usersets{
											Child: []*openfgapb.Userset{
												{Userset: &openfgapb.Userset_This{}},
												{Userset: &openfgapb.Userset_ComputedUserset{
													ComputedUserset: &openfgapb.ObjectRelation{
														Object:   "",
														Relation: "owner",
													},
												}},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			err: errors.RelationNotFound("owner", "", nil),
		},
		{
			name: "ExecuteWriteFailsIfDifferenceIncludesSameRelationTwice",
			request: &openfgapb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgapb.Userset{
								"viewer": {
									Userset: &openfgapb.Userset_Difference{
										Difference: &openfgapb.Difference{
											Base: &openfgapb.Userset{Userset: &openfgapb.Userset_This{}},
											Subtract: &openfgapb.Userset{Userset: &openfgapb.Userset_ComputedUserset{
												ComputedUserset: &openfgapb.ObjectRelation{
													Object:   "",
													Relation: "viewer",
												},
											}},
										},
									},
								},
							},
						},
					},
				},
			},
			err: errors.CannotAllowMultipleReferencesToOneRelation,
		},
		{
			name: "ExecuteWriteFailsIfUnionIncludesSameRelationTwice",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: testutils.CreateRandomString(10),
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgapb.Userset{
								"viewer": {
									Userset: &openfgapb.Userset_Union{
										Union: &openfgapb.Usersets{
											Child: []*openfgapb.Userset{
												{Userset: &openfgapb.Userset_ComputedUserset{
													ComputedUserset: &openfgapb.ObjectRelation{
														Relation: "viewer",
													},
												}},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			err: errors.CannotAllowMultipleReferencesToOneRelation,
		},
		{
			name: "ExecuteWriteFailsIfIntersectionIncludesSameRelationTwice",
			request: &openfgapb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgapb.Userset{
								"viewer": {
									Userset: &openfgapb.Userset_Intersection{
										Intersection: &openfgapb.Usersets{Child: []*openfgapb.Userset{
											{Userset: &openfgapb.Userset_ComputedUserset{
												ComputedUserset: &openfgapb.ObjectRelation{
													Relation: "viewer",
												},
											}},
											{Userset: &openfgapb.Userset_This{}},
										}},
									},
								},
							},
						},
					},
				},
			},
			err: errors.CannotAllowMultipleReferencesToOneRelation,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := commands.NewWriteAuthorizationModelCommand(datastore, logger)
			resp, err := cmd.Execute(ctx, test.request)
			require.ErrorIs(t, err, test.err)

			if err == nil {
				require.True(t, id.IsValid(resp.AuthorizationModelId))
			}
		})
	}
}
