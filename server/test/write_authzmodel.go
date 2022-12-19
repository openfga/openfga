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

	items := make([]*openfgapb.TypeDefinition, datastore.MaxTypesPerAuthorizationModel()+1)
	for i := 0; i < datastore.MaxTypesPerAuthorizationModel(); i++ {
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
			name: "succeeds_with_a_simple_model",
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
			name: "succeeds_with_a_complex_model",
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
			name: "fails_if_too_many_types",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId:         storeID,
				TypeDefinitions: items,
			},
			err: serverErrors.ExceededEntityLimit("type definitions in an authorization model", datastore.MaxTypesPerAuthorizationModel()),
		},
		{
			name: "succeeds_with_empty_relations",
			request: &openfgapb.WriteAuthorizationModelRequest{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
					},
				},
			},
		},
		{
			name: "succeeds_with_zero_length_relations",
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
			name: "fails_if_the_same_type_appears_twice",
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
			name: "fails_if_a_relation_is_not_defined",
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
			name: "fails_if_unknown_relation_in_computed_userset_definition",
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
			name: "fails_if_unknown_relation_in_tuple_to_userset_definition_(computed_userset_component)",
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
			name: "fails_if_unknown_relation_in_union",
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
			name: "fails_if_unknown_relation_in_difference_base_argument",
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
			name: "fails_if_unknown_relation_in_difference_subtract_argument",
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
			name: "fails_if_unknown_relation_in_tuple_to_userset_definition_(tupleset_component)",
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
			name: "fails_if_unknown_relation_in_computed_userset",
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
			name: "fails_if_unknown_relation_in_intersection",
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
			name: "fails_if_difference_includes_same_relation_twice",
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
			name: "fails_if_union_includes_same_relation_twice",
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
			name: "fails_if_intersection_includes_same_relation_twice",
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
			name: "Success_if_Union_Rewrite_Contains_Repeated_Definitions",
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
			name: "Success_if_Intersection_Rewrite_Contains_Repeated_Definitions",
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
			name: "Success_if_Exclusion_Rewrite_Contains_Repeated_Definitions",
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
			name: "Fails_if_Tupleset_relation_involves_ComputedUserset_rewrite",
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
			name: "Fails_if_Tupleset_relation_involves_Union_rewrite",
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
			name: "Fails_if_Tupleset_relation_involves_Intersection_rewrite",
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
			name: "Fails_if_Tupleset_relation_involves_Exclusion_rewrite",
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
			name: "Fails_if_Tupleset_relation_involves_TupleToUserset_rewrite",
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
		{
			name: "Fails_if_type_info_metadata_is_omitted_in_1.1_model",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId:       storeID,
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": typesystem.This(),
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				errors.New("the assignable relation 'reader' in object type 'document' must contain at least one relation type"),
			),
		},
		{
			name: "Fails_If_Using_This_As_Relation_Name",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"this": typesystem.This(),
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				errors.New("the definition of relation 'this' in object type 'repo' is invalid"),
			),
		},
		{
			name: "Fails_If_Using_Self_As_Relation_Name",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"self": typesystem.This(),
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				errors.New("the definition of relation 'self' in object type 'repo' is invalid"),
			),
		},
		{
			name: "Fails_If_Using_This_As_Type_Name",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "this",
						Relations: map[string]*openfgapb.Userset{
							"viewer": typesystem.This(),
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				errors.New("object type 'this' is invalid"),
			),
		},
		{
			name: "Fails_If_Using_Self_As_Type_Name",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: storeID,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "self",
						Relations: map[string]*openfgapb.Userset{
							"viewer": typesystem.This(),
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				errors.New("object type 'self' is invalid"),
			),
		},
		{
			name: "Fails_If_Auth_Model_1.1_Has_A_Cycle",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId:       storeID,
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.TupleToUserset("parent", "viewer"),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type: "folder",
										},
									},
								},
							},
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				errors.New("the definition of relation 'viewer' in object type 'folder' is invalid"),
			),
		},
		{
			name: "Fails_If_Auth_Model_1.0_Has_A_Cycle",
			request: &openfgapb.WriteAuthorizationModelRequest{
				StoreId:       storeID,
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.TupleToUserset("parent", "viewer"),
						},
					},
				},
			},
			err: serverErrors.InvalidAuthorizationModelInput(
				errors.New("the definition of relation 'viewer' in object type 'folder' is invalid"),
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
