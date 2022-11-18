package test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func WriteAuthorizationModelTest(t *testing.T, datastore storage.OpenFGADatastore) {
	storeID := ulid.Make().String()

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
			name: "succeeds with a simple model",
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
			name: "succeeds with a complex model",
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
			name: "succeeds with empty relations",
			request: &openfgapb.WriteAuthorizationModelRequest{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
					},
				},
			},
		},
		{
			name: "succeeds with zero length relations",
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
			name: "fails if the same type appears twice",
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
			name: "fails if a relation is not defined",
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{ObjectType: "repo", Relation: "owner"}),
		},
		{
			name: "fails if unknown relation in computed userset definition",
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.RelationUndefinedError{ObjectType: "repo", Relation: "owner"}),
		},
		{
			name: "fails if unknown relation in tuple to userset definition (computed userset component)",
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.RelationUndefinedError{ObjectType: "", Relation: "owner"}),
		},
		{
			name: "fails if unknown relation in union",
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.RelationUndefinedError{ObjectType: "repo", Relation: "owner"}),
		},
		{
			name: "fails if unknown relation in difference base argument",
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.RelationUndefinedError{ObjectType: "repo", Relation: "owner"}),
		},
		{
			name: "fails if unknown relation in difference subtract argument",
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.RelationUndefinedError{ObjectType: "repo", Relation: "owner"}),
		},
		{
			name: "fails if unknown relation in tuple to userset definition (tupleset component)",
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.RelationUndefinedError{ObjectType: "repo", Relation: "owner"}),
		},
		{
			name: "fails if unknown relation in computed userset",
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.RelationUndefinedError{ObjectType: "bar", Relation: "writer"}),
		},
		{
			name: "fails if unknown relation in intersection",
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.RelationUndefinedError{ObjectType: "repo", Relation: "owner"}),
		},
		{
			name: "fails if difference includes same relation twice",
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{ObjectType: "repo", Relation: "viewer"}),
		},
		{
			name: "fails if union includes same relation twice",
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{ObjectType: "repo", Relation: "viewer"}),
		},
		{
			name: "fails if intersection includes same relation twice",
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
			err: serverErrors.InvalidAuthorizationModelInput(&typesystem.InvalidRelationError{ObjectType: "repo", Relation: "viewer"}),
		},
		{
			name: "Success if Union Rewrite Contains Repeated Definitions",
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
			name: "Success if Intersection Rewrite Contains Repeated Definitions",
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
			name: "Success if Exclusion Rewrite Contains Repeated Definitions",
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
			name: "Fails if Tupleset relation involves ComputedUserset rewrite",
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
			name: "Fails if Tupleset relation involves Union rewrite",
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
			name: "Fails if Tupleset relation involves Intersection rewrite",
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
			name: "Fails if Tupleset relation involves Exclusion rewrite",
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
			name: "Fails if Tupleset relation involves TupleToUserset rewrite",
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
				_, err = ulid.Parse(resp.AuthorizationModelId)
				require.NoError(t, err)
			}
		})
	}
}
