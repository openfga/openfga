package commands

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/server/errors"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel"
)

func TestWriteAuthorizationModel(t *testing.T) {
	type writeAuthorizationModelTestSettings struct {
		_name    string
		request  *openfgav1pb.WriteAuthorizationModelRequest
		response *openfgav1pb.WriteAuthorizationModelResponse
		err      error
	}

	var tests = []writeAuthorizationModelTestSettings{
		{
			_name: "ExecuteWriteFailsIfSameTypeTwice",
			request: &openfgav1pb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1pb.TypeDefinitions{
					TypeDefinitions: []*openfgav1pb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1pb.Userset{
								"admin": {Userset: &openfgav1pb.Userset_This{}},
							},
						},
						{
							Type: "repo",
							Relations: map[string]*openfgav1pb.Userset{
								"admin": {Userset: &openfgav1pb.Userset_This{}},
							},
						},
					},
				},
			},
			err: errors.CannotAllowDuplicateTypesInOneRequest,
		},
		{
			_name: "ExecuteWriteFailsIfEmptyRelationDefinition",
			request: &openfgav1pb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1pb.TypeDefinitions{
					TypeDefinitions: []*openfgav1pb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1pb.Userset{
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
			request: &openfgav1pb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1pb.TypeDefinitions{
					TypeDefinitions: []*openfgav1pb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1pb.Userset{
								"writer": {
									Userset: &openfgav1pb.Userset_ComputedUserset{
										ComputedUserset: &openfgav1pb.ObjectRelation{
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
			request: &openfgav1pb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1pb.TypeDefinitions{
					TypeDefinitions: []*openfgav1pb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1pb.Userset{
								"writer": {Userset: &openfgav1pb.Userset_This{}},
								"viewer": {
									Userset: &openfgav1pb.Userset_TupleToUserset{
										TupleToUserset: &openfgav1pb.TupleToUserset{
											Tupleset: &openfgav1pb.ObjectRelation{
												Object:   "",
												Relation: "writer",
											},
											ComputedUserset: &openfgav1pb.ObjectRelation{
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
			request: &openfgav1pb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1pb.TypeDefinitions{
					TypeDefinitions: []*openfgav1pb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1pb.Userset{
								"writer": {Userset: &openfgav1pb.Userset_This{}},
								"viewer": {
									Userset: &openfgav1pb.Userset_Union{
										Union: &openfgav1pb.Usersets{
											Child: []*openfgav1pb.Userset{
												{Userset: &openfgav1pb.Userset_This{}},
												{Userset: &openfgav1pb.Userset_ComputedUserset{
													ComputedUserset: &openfgav1pb.ObjectRelation{
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
			_name: "ExecuteWriteFailsIfUnknownRelationInIntersection",
			request: &openfgav1pb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1pb.TypeDefinitions{
					TypeDefinitions: []*openfgav1pb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1pb.Userset{
								"writer": {Userset: &openfgav1pb.Userset_This{}},
								"viewer": {
									Userset: &openfgav1pb.Userset_Intersection{
										Intersection: &openfgav1pb.Usersets{
											Child: []*openfgav1pb.Userset{
												{Userset: &openfgav1pb.Userset_This{}},
												{Userset: &openfgav1pb.Userset_ComputedUserset{
													ComputedUserset: &openfgav1pb.ObjectRelation{
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
			request: &openfgav1pb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1pb.TypeDefinitions{
					TypeDefinitions: []*openfgav1pb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1pb.Userset{
								"writer": {Userset: &openfgav1pb.Userset_This{}},
								"viewer": {
									Userset: &openfgav1pb.Userset_Difference{
										Difference: &openfgav1pb.Difference{
											Base: &openfgav1pb.Userset{Userset: &openfgav1pb.Userset_ComputedUserset{
												ComputedUserset: &openfgav1pb.ObjectRelation{
													Object:   "",
													Relation: "writer",
												},
											}},
											Subtract: &openfgav1pb.Userset{Userset: &openfgav1pb.Userset_ComputedUserset{
												ComputedUserset: &openfgav1pb.ObjectRelation{
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
			request: &openfgav1pb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1pb.TypeDefinitions{
					TypeDefinitions: []*openfgav1pb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1pb.Userset{
								"writer": {Userset: &openfgav1pb.Userset_This{}},
								"viewer": {
									Userset: &openfgav1pb.Userset_Difference{
										Difference: &openfgav1pb.Difference{
											Base: &openfgav1pb.Userset{Userset: &openfgav1pb.Userset_ComputedUserset{
												ComputedUserset: &openfgav1pb.ObjectRelation{
													Object:   "",
													Relation: "owner",
												},
											}},
											Subtract: &openfgav1pb.Userset{Userset: &openfgav1pb.Userset_ComputedUserset{
												ComputedUserset: &openfgav1pb.ObjectRelation{
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
			_name: "ExecuteWriteFailsIfEmptyRelationDefinition",
			request: &openfgav1pb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1pb.TypeDefinitions{
					TypeDefinitions: []*openfgav1pb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1pb.Userset{
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
			request: &openfgav1pb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1pb.TypeDefinitions{
					TypeDefinitions: []*openfgav1pb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1pb.Userset{
								"writer": {
									Userset: &openfgav1pb.Userset_ComputedUserset{
										ComputedUserset: &openfgav1pb.ObjectRelation{
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
			_name: "ExecuteWriteFailsIfUnknownRelationInTupleToUsersetTupleset",
			request: &openfgav1pb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1pb.TypeDefinitions{
					TypeDefinitions: []*openfgav1pb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1pb.Userset{
								"writer": {Userset: &openfgav1pb.Userset_This{}},
								"viewer": {
									Userset: &openfgav1pb.Userset_TupleToUserset{
										TupleToUserset: &openfgav1pb.TupleToUserset{
											Tupleset: &openfgav1pb.ObjectRelation{
												Object:   "",
												Relation: "owner",
											},
											ComputedUserset: &openfgav1pb.ObjectRelation{
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
			request: &openfgav1pb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1pb.TypeDefinitions{
					TypeDefinitions: []*openfgav1pb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1pb.Userset{
								"writer": {Userset: &openfgav1pb.Userset_This{}},
								"viewer": {
									Userset: &openfgav1pb.Userset_TupleToUserset{
										TupleToUserset: &openfgav1pb.TupleToUserset{
											Tupleset: &openfgav1pb.ObjectRelation{
												Object:   "",
												Relation: "writer",
											},
											ComputedUserset: &openfgav1pb.ObjectRelation{
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
			request: &openfgav1pb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1pb.TypeDefinitions{
					TypeDefinitions: []*openfgav1pb.TypeDefinition{
						{
							Type: "foo",
							Relations: map[string]*openfgav1pb.Userset{
								"writer": {
									Userset: &openfgav1pb.Userset_ComputedUserset{
										ComputedUserset: &openfgav1pb.ObjectRelation{
											Object:   "",
											Relation: "reader",
										},
									},
								},
								"reader": {
									Userset: &openfgav1pb.Userset_This{},
								},
							},
						},
						{
							Type: "bar",
							Relations: map[string]*openfgav1pb.Userset{
								"owner": {
									Userset: &openfgav1pb.Userset_ComputedUserset{
										ComputedUserset: &openfgav1pb.ObjectRelation{
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
			_name: "ExecuteWriteFailsIfUnknownRelationInUnion",
			request: &openfgav1pb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1pb.TypeDefinitions{
					TypeDefinitions: []*openfgav1pb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1pb.Userset{
								"writer": {Userset: &openfgav1pb.Userset_This{}},
								"viewer": {
									Userset: &openfgav1pb.Userset_Union{
										Union: &openfgav1pb.Usersets{
											Child: []*openfgav1pb.Userset{
												{Userset: &openfgav1pb.Userset_This{}},
												{Userset: &openfgav1pb.Userset_ComputedUserset{
													ComputedUserset: &openfgav1pb.ObjectRelation{
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
			_name: "ExecuteWriteFailsIfUnknownRelationInIntersection",
			request: &openfgav1pb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1pb.TypeDefinitions{
					TypeDefinitions: []*openfgav1pb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1pb.Userset{
								"writer": {Userset: &openfgav1pb.Userset_This{}},
								"viewer": {
									Userset: &openfgav1pb.Userset_Intersection{
										Intersection: &openfgav1pb.Usersets{
											Child: []*openfgav1pb.Userset{
												{Userset: &openfgav1pb.Userset_This{}},
												{Userset: &openfgav1pb.Userset_ComputedUserset{
													ComputedUserset: &openfgav1pb.ObjectRelation{
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
			request: &openfgav1pb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1pb.TypeDefinitions{
					TypeDefinitions: []*openfgav1pb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1pb.Userset{
								"writer": {Userset: &openfgav1pb.Userset_This{}},
								"viewer": {
									Userset: &openfgav1pb.Userset_Difference{
										Difference: &openfgav1pb.Difference{
											Base: &openfgav1pb.Userset{Userset: &openfgav1pb.Userset_ComputedUserset{
												ComputedUserset: &openfgav1pb.ObjectRelation{
													Object:   "",
													Relation: "writer",
												},
											}},
											Subtract: &openfgav1pb.Userset{Userset: &openfgav1pb.Userset_ComputedUserset{
												ComputedUserset: &openfgav1pb.ObjectRelation{
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
			request: &openfgav1pb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1pb.TypeDefinitions{
					TypeDefinitions: []*openfgav1pb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1pb.Userset{
								"writer": {Userset: &openfgav1pb.Userset_This{}},
								"viewer": {
									Userset: &openfgav1pb.Userset_Difference{
										Difference: &openfgav1pb.Difference{
											Base: &openfgav1pb.Userset{Userset: &openfgav1pb.Userset_ComputedUserset{
												ComputedUserset: &openfgav1pb.ObjectRelation{
													Object:   "",
													Relation: "owner",
												},
											}},
											Subtract: &openfgav1pb.Userset{Userset: &openfgav1pb.Userset_ComputedUserset{
												ComputedUserset: &openfgav1pb.ObjectRelation{
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
			_name: "ExecuteWriteFailsIfDifferenceIncludesSameRelationTwice",
			request: &openfgav1pb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1pb.TypeDefinitions{
					TypeDefinitions: []*openfgav1pb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1pb.Userset{
								"viewer": {
									Userset: &openfgav1pb.Userset_Difference{
										Difference: &openfgav1pb.Difference{
											Base: &openfgav1pb.Userset{Userset: &openfgav1pb.Userset_This{}},
											Subtract: &openfgav1pb.Userset{Userset: &openfgav1pb.Userset_ComputedUserset{
												ComputedUserset: &openfgav1pb.ObjectRelation{
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
			request: &openfgav1pb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1pb.TypeDefinitions{
					TypeDefinitions: []*openfgav1pb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1pb.Userset{
								"viewer": {
									Userset: &openfgav1pb.Userset_Union{
										Union: &openfgav1pb.Usersets{
											Child: []*openfgav1pb.Userset{
												{Userset: &openfgav1pb.Userset_ComputedUserset{
													ComputedUserset: &openfgav1pb.ObjectRelation{
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
			request: &openfgav1pb.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1pb.TypeDefinitions{
					TypeDefinitions: []*openfgav1pb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1pb.Userset{
								"viewer": {
									Userset: &openfgav1pb.Userset_Intersection{
										Intersection: &openfgav1pb.Usersets{Child: []*openfgav1pb.Userset{
											{Userset: &openfgav1pb.Userset_ComputedUserset{
												ComputedUserset: &openfgav1pb.ObjectRelation{
													Relation: "viewer",
												},
											}},
											{Userset: &openfgav1pb.Userset_This{}},
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

Tests:
	for _, test := range tests {
		tracer := otel.Tracer("noop")
		storage, err := testutils.BuildAllBackends(tracer)
		if err != nil {
			t.Fatalf("Error building backend: %s", err)
		}
		ctx := context.Background()

		logger := logger.NewNoopLogger()

		writeAuthzModelCommand := NewWriteAuthorizationModelCommand(storage.AuthorizationModelBackend, logger)
		actualResponse, actualError := writeAuthzModelCommand.Execute(ctx, test.request)

		if test.err != nil {
			if actualError == nil {
				t.Errorf("[%s] Expected error '%s', but got none", test._name, test.err)
				continue Tests
			}
			if test.err.Error() != actualError.Error() {
				t.Errorf("[%s] Expected error '%s', actual '%s'", test._name, test.err, actualError)
				continue Tests
			}
		}

		if test.response != nil {
			if actualError != nil {
				t.Errorf("[%s] Expected no error but got '%s'", test._name, actualError)
				continue Tests
			}

			if actualResponse == nil {
				t.Error("Expected non nil response, got nil")
				continue Tests
			}
		}
	}
}
