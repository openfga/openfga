package commands

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/server/errors"
	mockstorage "github.com/openfga/openfga/storage/mocks"
	openfgav1 "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestWriteAuthorizationModel(t *testing.T) {
	type writeAuthorizationModelTest struct {
		_name    string
		request  *openfgav1.WriteAuthorizationModelRequest
		err      error
		response *openfgav1.WriteAuthorizationModelResponse
	}

	logger := logger.NewNoopLogger()
	ctx := context.Background()
	ignoreUnexported := cmpopts.IgnoreUnexported(openfgav1.WriteAuthorizationModelResponse{})
	ignoreFields := cmpopts.IgnoreFields(openfgav1.WriteAuthorizationModelResponse{}, "AuthorizationModelId")

	var tests = []writeAuthorizationModelTest{
		{
			_name: "succeeds",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: writeTestStore,
				TypeDefinitions: &openfgav1.TypeDefinitions{
					TypeDefinitions: []*openfgav1.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1.Userset{
								"admin": {Userset: &openfgav1.Userset_This{}},
							},
						},
					},
				},
			},
			err:      nil,
			response: &openfgav1.WriteAuthorizationModelResponse{},
		},
		{
			_name: "fails if too many types",
			request: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: writeTestStore,
				TypeDefinitions: &openfgav1.TypeDefinitions{
					TypeDefinitions: []*openfgav1.TypeDefinition{
						{
							Type: "typeone",
							Relations: map[string]*openfgav1.Userset{
								"admin": {Userset: &openfgav1.Userset_This{}},
							},
						},
						{
							Type: "typetwo",
							Relations: map[string]*openfgav1.Userset{
								"admin": {Userset: &openfgav1.Userset_This{}},
							},
						},
						{
							Type: "typethree",
							Relations: map[string]*openfgav1.Userset{
								"admin": {Userset: &openfgav1.Userset_This{}},
							},
						},
					},
				},
			},
			err:      serverErrors.ExceededEntityLimit("type definitions in an authorization model", 2),
			response: nil,
		},
		{
			_name: "fails if same type appears twice",
			request: &openfgav1.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1.TypeDefinitions{
					TypeDefinitions: []*openfgav1.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1.Userset{
								"admin": {Userset: &openfgav1.Userset_This{}},
							},
						},
						{
							Type: "repo",
							Relations: map[string]*openfgav1.Userset{
								"admin": {Userset: &openfgav1.Userset_This{}},
							},
						},
					},
				},
			},
			err: serverErrors.CannotAllowDuplicateTypesInOneRequest,
		},
		{
			_name: "fails if empty relation definition",
			request: &openfgav1.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1.TypeDefinitions{
					TypeDefinitions: []*openfgav1.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1.Userset{
								"owner": {},
							},
						},
					},
				},
			},
			err: serverErrors.EmptyRelationDefinition("repo", "owner"),
		},
		{
			_name: "fails if unknown relation in computed userset",
			request: &openfgav1.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1.TypeDefinitions{
					TypeDefinitions: []*openfgav1.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1.Userset{
								"writer": {
									Userset: &openfgav1.Userset_ComputedUserset{
										ComputedUserset: &openfgav1.ObjectRelation{
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
			err: serverErrors.UnknownRelation("owner"),
		},
		{
			_name: "fails if unknown relation in tupleToUserset",
			request: &openfgav1.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1.TypeDefinitions{
					TypeDefinitions: []*openfgav1.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1.Userset{
								"writer": {Userset: &openfgav1.Userset_This{}},
								"viewer": {
									Userset: &openfgav1.Userset_TupleToUserset{
										TupleToUserset: &openfgav1.TupleToUserset{
											Tupleset: &openfgav1.ObjectRelation{
												Object:   "",
												Relation: "writer",
											},
											ComputedUserset: &openfgav1.ObjectRelation{
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
			err: serverErrors.UnknownRelation("owner"),
		},
		{
			_name: "fails if unknown relation in union",
			request: &openfgav1.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1.TypeDefinitions{
					TypeDefinitions: []*openfgav1.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1.Userset{
								"writer": {Userset: &openfgav1.Userset_This{}},
								"viewer": {
									Userset: &openfgav1.Userset_Union{
										Union: &openfgav1.Usersets{
											Child: []*openfgav1.Userset{
												{Userset: &openfgav1.Userset_This{}},
												{Userset: &openfgav1.Userset_ComputedUserset{
													ComputedUserset: &openfgav1.ObjectRelation{
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
			err: serverErrors.UnknownRelation("owner"),
		},
		{
			_name: "fails if unknown relation in intersection",
			request: &openfgav1.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1.TypeDefinitions{
					TypeDefinitions: []*openfgav1.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1.Userset{
								"writer": {Userset: &openfgav1.Userset_This{}},
								"viewer": {
									Userset: &openfgav1.Userset_Intersection{
										Intersection: &openfgav1.Usersets{
											Child: []*openfgav1.Userset{
												{Userset: &openfgav1.Userset_This{}},
												{Userset: &openfgav1.Userset_ComputedUserset{
													ComputedUserset: &openfgav1.ObjectRelation{
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
			err: serverErrors.UnknownRelation("owner"),
		},
		{
			_name: "fails if unknown relation in difference base argument",
			request: &openfgav1.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1.TypeDefinitions{
					TypeDefinitions: []*openfgav1.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1.Userset{
								"writer": {Userset: &openfgav1.Userset_This{}},
								"viewer": {
									Userset: &openfgav1.Userset_Difference{
										Difference: &openfgav1.Difference{
											Base: &openfgav1.Userset{Userset: &openfgav1.Userset_ComputedUserset{
												ComputedUserset: &openfgav1.ObjectRelation{
													Object:   "",
													Relation: "writer",
												},
											}},
											Subtract: &openfgav1.Userset{Userset: &openfgav1.Userset_ComputedUserset{
												ComputedUserset: &openfgav1.ObjectRelation{
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
			err: serverErrors.UnknownRelation("owner"),
		},
		{
			_name: "fails if unknown relation in difference subtract argument",
			request: &openfgav1.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1.TypeDefinitions{
					TypeDefinitions: []*openfgav1.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1.Userset{
								"writer": {Userset: &openfgav1.Userset_This{}},
								"viewer": {
									Userset: &openfgav1.Userset_Difference{
										Difference: &openfgav1.Difference{
											Base: &openfgav1.Userset{Userset: &openfgav1.Userset_ComputedUserset{
												ComputedUserset: &openfgav1.ObjectRelation{
													Object:   "",
													Relation: "owner",
												},
											}},
											Subtract: &openfgav1.Userset{Userset: &openfgav1.Userset_ComputedUserset{
												ComputedUserset: &openfgav1.ObjectRelation{
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
			err: serverErrors.UnknownRelation("owner"),
		},
		{
			_name: "fails if difference includes same relation twice",
			request: &openfgav1.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1.TypeDefinitions{
					TypeDefinitions: []*openfgav1.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1.Userset{
								"viewer": {
									Userset: &openfgav1.Userset_Difference{
										Difference: &openfgav1.Difference{
											Base: &openfgav1.Userset{Userset: &openfgav1.Userset_This{}},
											Subtract: &openfgav1.Userset{Userset: &openfgav1.Userset_ComputedUserset{
												ComputedUserset: &openfgav1.ObjectRelation{
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
			err: serverErrors.CannotAllowMultipleReferencesToOneRelation,
		},
		{
			_name: "fails if union includes same relation twice",
			request: &openfgav1.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1.TypeDefinitions{
					TypeDefinitions: []*openfgav1.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1.Userset{
								"viewer": {
									Userset: &openfgav1.Userset_Union{
										Union: &openfgav1.Usersets{
											Child: []*openfgav1.Userset{
												{Userset: &openfgav1.Userset_ComputedUserset{
													ComputedUserset: &openfgav1.ObjectRelation{
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
			err: serverErrors.CannotAllowMultipleReferencesToOneRelation,
		},
		{
			_name: "fails if intersection includes same relation twice",
			request: &openfgav1.WriteAuthorizationModelRequest{
				TypeDefinitions: &openfgav1.TypeDefinitions{
					TypeDefinitions: []*openfgav1.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgav1.Userset{
								"viewer": {
									Userset: &openfgav1.Userset_Intersection{
										Intersection: &openfgav1.Usersets{Child: []*openfgav1.Userset{
											{Userset: &openfgav1.Userset_ComputedUserset{
												ComputedUserset: &openfgav1.ObjectRelation{
													Relation: "viewer",
												},
											}},
											{Userset: &openfgav1.Userset_This{}},
										}},
									},
								},
							},
						},
					},
				},
			},
			err: serverErrors.CannotAllowMultipleReferencesToOneRelation,
		},
	}

	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	cmd := NewWriteAuthorizationModelCommand(mockDatastore, logger)
	mockDatastore.EXPECT().MaxTypesInTypeDefinition().AnyTimes().Return(2)
	mockDatastore.EXPECT().WriteAuthorizationModel(ctx, writeTestStore, gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			actualResponse, actualError := cmd.Execute(ctx, test.request)

			if test.err != nil {
				if actualError == nil {
					t.Errorf("Expected error '%s', but got none", test.err)
				}
				if test.err.Error() != actualError.Error() {
					t.Errorf("Expected error '%s', actual '%s'", test.err, actualError)
				}
			}
			if test.err == nil && actualError != nil {
				t.Errorf("Did not expect an error but got one: %v", actualError)
			}

			if actualResponse == nil && test.err == nil {
				t.Error("Expected non nil response, got nil")
			} else {
				if diff := cmp.Diff(actualResponse, test.response, ignoreUnexported, ignoreFields, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("Unexpected response (-got +want):\n%s", diff)
				}
			}
		})
	}

}
