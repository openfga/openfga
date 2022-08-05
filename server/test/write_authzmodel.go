package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/server/commands"
	"github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestWriteAuthorizationModel(t *testing.T, datastore storage.OpenFGADatastore) {
	type writeAuthorizationModelTestSettings struct {
		_name    string
		request  *openfgapb.WriteAuthorizationModelRequest
		response *openfgapb.WriteAuthorizationModelResponse
		err      error
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
			_name: "succeeds",
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
			err:      nil,
			response: &openfgapb.WriteAuthorizationModelResponse{},
		},
		{
			_name: "fails if too many types",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: "somestoreid",
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: items,
				},
			},
			err:      errors.ExceededEntityLimit("type definitions in an authorization model", datastore.MaxTypesInTypeDefinition()),
			response: nil,
		},
		{
			_name: "ExecuteWriteFailsIfSameTypeTwice",
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
			_name: "ExecuteWriteFailsIfEmptyRelationDefinition",
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
			err: errors.EmptyRelationDefinition("repo", "owner"),
		},
		{
			_name: "ExecuteWriteFailsIfUnknownRelationInComputedUserset",
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
			err: errors.UnknownRelation("owner"),
		},
		{
			_name: "ExecuteWriteFailsIfUnknownRelationInTupleToUserset",
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
			err: errors.UnknownRelation("owner"),
		},
		{
			_name: "ExecuteWriteFailsIfUnknownRelationInUnion",
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
			err: errors.UnknownRelation("owner"),
		},
		{
			_name: "ExecuteWriteFailsIfUnknownRelationInDifferenceBaseArgument",
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
			err: errors.UnknownRelation("owner"),
		},
		{
			_name: "ExecuteWriteFailsIfUnknownRelationInDifferenceSubtractArgument",
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
			err: errors.UnknownRelation("owner"),
		},
		{
			_name: "ExecuteWriteFailsIfUnknownRelationInTupleToUsersetTupleset",
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
			err: errors.UnknownRelation("owner"),
		},
		{
			_name: "ExecuteWriteFailsIfUnknownRelationInTupleToUsersetComputedUserset",
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
			err: errors.UnknownRelation("owner"),
		},
		{
			_name: "ExecuteWriteFailsIfTupleToUsersetReferencesUnknownRelation",
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
			err: errors.UnknownRelation("writer"),
		},
		{
			_name: "ExecuteWriteFailsIfUnknownRelationInIntersection",
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
			err: errors.UnknownRelation("owner"),
		},
		{
			_name: "ExecuteWriteFailsIfDifferenceIncludesSameRelationTwice",
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
			_name: "ExecuteWriteFailsIfUnionIncludesSameRelationTwice",
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
			_name: "ExecuteWriteFailsIfIntersectionIncludesSameRelationTwice",
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
		t.Run(test._name, func(t *testing.T) {
			cmd := commands.NewWriteAuthorizationModelCommand(datastore, logger)
			actualResponse, actualError := cmd.Execute(ctx, test.request)

			if test.err != nil {
				if actualError == nil {
					t.Fatalf("[%s] Expected error '%s', but got none", test._name, test.err)
				}
				if test.err.Error() != actualError.Error() {
					t.Fatalf("[%s] Expected error '%s', actual '%s'", test._name, test.err, actualError)
				}
			}

			if test.response != nil {
				if actualError != nil {
					t.Fatalf("[%s] Expected no error but got '%s'", test._name, actualError)
				}

				if actualResponse == nil {
					t.Fatalf("Expected non nil response, got nil")
				}
			}
		})
	}
}
