package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-errors/errors"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func WriteAuthorizationModelTest(t *testing.T, datastore storage.OpenFGADatastore) {
	storeID, err := id.NewString()
	require.NoError(t, err)

	items := make([]*openfgapb.TypeDefinition, datastore.MaxTypesInTypeDefinition()+1)
	for i := 0; i < datastore.MaxTypesInTypeDefinition(); i++ {
		items[i] = &openfgapb.TypeDefinition{
			Type: fmt.Sprintf("type%v", i),
			Relations: map[string]*openfgapb.Userset{
				"admin": {Userset: &openfgapb.Userset_This{}},
			},
		}
	}

	var tests = []struct {
		name    string
		request *openfgapb.WriteAuthorizationModelRequest
		err     error
	}{
		{
			name: "succeeds",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
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
		{
			name: "succeeds part II",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: "somestoreid",
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
		{
			name: "fails if too many types",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId:         storeID,
				TypeDefinitions: items,
			},
			err: serverErrors.ExceededEntityLimit("type definitions in an authorization model", datastore.MaxTypesInTypeDefinition()),
		},
		{
			name: "empty relations is valid",
			request: &openfgapb.WriteAuthorizationModelRequest{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
					},
				},
			},
		},
		{
			name: "zero length relations is valid",
			request: &openfgapb.WriteAuthorizationModelRequest{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type:      "repo",
						Relations: map[string]*openfgapb.Userset{},
					},
				},
			},
		},
		{
			name: "ExecuteWriteFailsIfSameTypeTwice",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
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
			err: serverErrors.InvalidAuthorizationModelInput(typesystem.ErrDuplicateTypes),
		},
		{
			name: "ExecuteWriteFailsIfEmptyRewrites",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"owner": {},
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(typesystem.InvalidRelationError("repo", "owner")),
		},
		{
			name: "ExecuteWriteFailsIfUnknownRelationInComputedUserset",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
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
			err: serverErrors.InvalidAuthorizationModelInput(typesystem.RelationDoesNotExistError("repo", "owner")),
		},
		{
			name: "ExecuteWriteFailsIfUnknownRelationInTupleToUserset",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"writer": {Userset: &openfgapb.Userset_This{}},
							"viewer": {
								Userset: &openfgapb.Userset_TupleToUserset{
									TupleToUserset: &openfgapb.TupleToUserset{
										Tupleset: &openfgapb.ObjectRelation{
											Relation: "writer",
										},
										ComputedUserset: &openfgapb.ObjectRelation{
											Relation: "owner",
										},
									},
								},
							},
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(typesystem.RelationDoesNotExistError("", "owner")),
		},
		{
			name: "ExecuteWriteFailsIfUnknownRelationInUnion",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"writer": {
								Userset: &openfgapb.Userset_This{},
							},
							"viewer": {
								Userset: &openfgapb.Userset_Union{
									Union: &openfgapb.Usersets{
										Child: []*openfgapb.Userset{
											{
												Userset: &openfgapb.Userset_This{},
											},
											{
												Userset: &openfgapb.Userset_ComputedUserset{
													ComputedUserset: &openfgapb.ObjectRelation{
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
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(typesystem.RelationDoesNotExistError("repo", "owner")),
		},
		{
			name: "ExecuteWriteFailsIfUnknownRelationInDifferenceBaseArgument",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"writer": {Userset: &openfgapb.Userset_This{}},
							"viewer": {
								Userset: &openfgapb.Userset_Difference{
									Difference: &openfgapb.Difference{
										Base: &openfgapb.Userset{
											Userset: &openfgapb.Userset_ComputedUserset{
												ComputedUserset: &openfgapb.ObjectRelation{
													Relation: "writer",
												},
											},
										},
										Subtract: &openfgapb.Userset{
											Userset: &openfgapb.Userset_ComputedUserset{
												ComputedUserset: &openfgapb.ObjectRelation{
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
			},
			err: serverErrors.InvalidAuthorizationModelInput(typesystem.RelationDoesNotExistError("repo", "owner")),
		},
		{
			name: "ExecuteWriteFailsIfUnknownRelationInDifferenceSubtractArgument",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"writer": {Userset: &openfgapb.Userset_This{}},
							"viewer": {
								Userset: &openfgapb.Userset_Difference{
									Difference: &openfgapb.Difference{
										Base: &openfgapb.Userset{
											Userset: &openfgapb.Userset_ComputedUserset{
												ComputedUserset: &openfgapb.ObjectRelation{
													Relation: "owner",
												},
											},
										},
										Subtract: &openfgapb.Userset{
											Userset: &openfgapb.Userset_ComputedUserset{
												ComputedUserset: &openfgapb.ObjectRelation{
													Relation: "writer",
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
			err: serverErrors.InvalidAuthorizationModelInput(typesystem.RelationDoesNotExistError("repo", "owner")),
		},
		{
			name: "ExecuteWriteFailsIfUnknownRelationInTupleToUsersetTupleset",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"writer": {
								Userset: &openfgapb.Userset_This{},
							},
							"viewer": {
								Userset: &openfgapb.Userset_TupleToUserset{
									TupleToUserset: &openfgapb.TupleToUserset{
										Tupleset: &openfgapb.ObjectRelation{
											Relation: "owner",
										},
										ComputedUserset: &openfgapb.ObjectRelation{
											Relation: "from",
										},
									},
								},
							},
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(typesystem.RelationDoesNotExistError("repo", "owner")),
		},
		{
			name: "ExecuteWriteFailsIfUnknownRelationInTupleToUsersetComputedUserset",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"writer": {
								Userset: &openfgapb.Userset_This{},
							},
							"viewer": {
								Userset: &openfgapb.Userset_TupleToUserset{
									TupleToUserset: &openfgapb.TupleToUserset{
										Tupleset: &openfgapb.ObjectRelation{
											Relation: "writer",
										},
										ComputedUserset: &openfgapb.ObjectRelation{
											Relation: "owner",
										},
									},
								},
							},
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(typesystem.RelationDoesNotExistError("", "owner")),
		},
		{
			name: "ExecuteWriteFailsIfTupleToUsersetReferencesUnknownRelation",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "foo",
						Relations: map[string]*openfgapb.Userset{
							"writer": {
								Userset: &openfgapb.Userset_ComputedUserset{
									ComputedUserset: &openfgapb.ObjectRelation{
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
										Relation: "writer",
									},
								},
							},
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(typesystem.RelationDoesNotExistError("bar", "writer")),
		},
		{
			name: "ExecuteWriteFailsIfUnknownRelationInIntersection",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"writer": {Userset: &openfgapb.Userset_This{}},
							"viewer": {
								Userset: &openfgapb.Userset_Intersection{
									Intersection: &openfgapb.Usersets{
										Child: []*openfgapb.Userset{
											{
												Userset: &openfgapb.Userset_This{},
											},
											{
												Userset: &openfgapb.Userset_ComputedUserset{
													ComputedUserset: &openfgapb.ObjectRelation{
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
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(typesystem.RelationDoesNotExistError("repo", "owner")),
		},
		{
			name: "ExecuteWriteFailsIfDifferenceIncludesSameRelationTwice",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"viewer": {
								Userset: &openfgapb.Userset_Difference{
									Difference: &openfgapb.Difference{
										Base: &openfgapb.Userset{
											Userset: &openfgapb.Userset_This{},
										},
										Subtract: &openfgapb.Userset{
											Userset: &openfgapb.Userset_ComputedUserset{
												ComputedUserset: &openfgapb.ObjectRelation{
													Relation: "viewer",
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
			err: serverErrors.InvalidAuthorizationModelInput(typesystem.InvalidRelationError("repo", "viewer")),
		},
		{
			name: "ExecuteWriteFailsIfUnionIncludesSameRelationTwice",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
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
			err: serverErrors.InvalidAuthorizationModelInput(typesystem.InvalidRelationError("repo", "viewer")),
		},
		{
			name: "ExecuteWriteFailsIfIntersectionIncludesSameRelationTwice",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
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
			err: serverErrors.InvalidAuthorizationModelInput(typesystem.InvalidRelationError("repo", "viewer")),
		},
		{
			name: "Union Rewrite Contains Repeated Definitions",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.Union(
								typesystem.ComputedUserset("editor"),
								typesystem.ComputedUserset("editor"),
							),
							"editor": typesystem.Union(typesystem.This(), typesystem.This()),
							"manage": typesystem.Union(
								typesystem.TupleToUserset("parent", "manage"),
								typesystem.TupleToUserset("parent", "manage"),
							),
						},
					},
				},
			},
		},
		{
			name: "Intersection Rewrite Contains Repeated Definitions",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.Intersection(
								typesystem.ComputedUserset("editor"),
								typesystem.ComputedUserset("editor"),
							),
							"editor": typesystem.Intersection(typesystem.This(), typesystem.This()),
							"manage": typesystem.Intersection(
								typesystem.TupleToUserset("parent", "manage"),
								typesystem.TupleToUserset("parent", "manage"),
							),
						},
					},
				},
			},
		},
		{
			name: "Exclusion Rewrite Contains Repeated Definitions",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.Difference(
								typesystem.ComputedUserset("editor"),
								typesystem.ComputedUserset("editor"),
							),
							"editor": typesystem.Difference(typesystem.This(), typesystem.This()),
							"manage": typesystem.Difference(
								typesystem.TupleToUserset("parent", "manage"),
								typesystem.TupleToUserset("parent", "manage"),
							),
						},
					},
				},
			},
		},
		{
			name: "Tupleset relation involves ComputedUserset rewrite",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"ancestor": typesystem.This(),
							"parent":   typesystem.ComputedUserset("ancestor"),
							"viewer":   typesystem.TupleToUserset("parent", "viewer"),
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				errors.New("the 'document#parent' relation is referenced in at least one tupleset and thus must be a direct relation"),
			),
		},
		{
			name: "Tupleset relation involves Union rewrite",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"ancestor": typesystem.This(),
							"parent":   typesystem.Union(typesystem.This(), typesystem.ComputedUserset("ancestor")),
							"viewer":   typesystem.TupleToUserset("parent", "viewer"),
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				errors.New("the 'document#parent' relation is referenced in at least one tupleset and thus must be a direct relation"),
			),
		},
		{
			name: "Tupleset relation involves Intersection rewrite",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"ancestor": typesystem.This(),
							"parent":   typesystem.Intersection(typesystem.This(), typesystem.ComputedUserset("ancestor")),
							"viewer":   typesystem.TupleToUserset("parent", "viewer"),
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				errors.New("the 'document#parent' relation is referenced in at least one tupleset and thus must be a direct relation"),
			),
		},
		{
			name: "Tupleset relation involves Exclusion rewrite",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"restricted": typesystem.This(),
							"parent":     typesystem.Difference(typesystem.This(), typesystem.ComputedUserset("restricted")),
							"viewer":     typesystem.TupleToUserset("parent", "viewer"),
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				errors.New("the 'document#parent' relation is referenced in at least one tupleset and thus must be a direct relation"),
			),
		},
		{
			name: "Tupleset relation involves TupleToUserset rewrite",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"ancestor": typesystem.This(),
							"parent":   typesystem.TupleToUserset("ancestor", "viewer"),
							"viewer":   typesystem.TupleToUserset("parent", "viewer"),
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				errors.New("the 'document#parent' relation is referenced in at least one tupleset and thus must be a direct relation"),
			),
		},
	}

	ctx := context.Background()
	logger := logger.NewNoopLogger()

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
