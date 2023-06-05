package typesystem

import (
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestSuccessfulRewriteValidations(t *testing.T) {
	var tests = []struct {
		name  string
		model *openfgapb.AuthorizationModel
	}{
		{
			name: "empty_relations",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
					},
				},
			},
		},
		{
			name: "zero_length_relations_is_valid",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type:      "repo",
						Relations: map[string]*openfgapb.Userset{},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewAndValidate(test.model)
			require.NoError(t, err)
		})
	}
}

func TestInvalidRewriteValidations(t *testing.T) {
	var tests = []struct {
		name  string
		model *openfgapb.AuthorizationModel
		err   error
	}{
		{
			name: "empty_rewrites",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": {},
						},
					},
				},
			},
			err: ErrInvalidUsersetRewrite,
		},
		{
			name: "invalid_relation:_self_reference_in_computedUserset",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": {
								Userset: &openfgapb.Userset_ComputedUserset{
									ComputedUserset: &openfgapb.ObjectRelation{Relation: "reader"},
								},
							},
						},
					},
				},
			},
			err: ErrInvalidUsersetRewrite,
		},
		{
			name: "invalid_relation:_self_reference_in_union",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": {
								Userset: &openfgapb.Userset_Union{
									Union: &openfgapb.Usersets{
										Child: []*openfgapb.Userset{
											{
												Userset: &openfgapb.Userset_This{},
											},
											{
												Userset: &openfgapb.Userset_ComputedUserset{
													ComputedUserset: &openfgapb.ObjectRelation{Relation: "reader"},
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
			err: ErrInvalidUsersetRewrite,
		},
		{
			name: "invalid_relation:_self_reference_in_intersection",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": {
								Userset: &openfgapb.Userset_Intersection{
									Intersection: &openfgapb.Usersets{
										Child: []*openfgapb.Userset{
											{
												Userset: &openfgapb.Userset_This{},
											},
											{
												Userset: &openfgapb.Userset_ComputedUserset{
													ComputedUserset: &openfgapb.ObjectRelation{Relation: "reader"},
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
			err: ErrInvalidUsersetRewrite,
		},
		{
			name: "invalid_relation:_self_reference_in_difference_base",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": {
								Userset: &openfgapb.Userset_Difference{
									Difference: &openfgapb.Difference{
										Base: &openfgapb.Userset{
											Userset: &openfgapb.Userset_ComputedUserset{
												ComputedUserset: &openfgapb.ObjectRelation{Relation: "reader"},
											},
										},
										Subtract: &openfgapb.Userset{
											Userset: &openfgapb.Userset_This{},
										},
									},
								},
							},
						},
					},
				},
			},
			err: ErrInvalidUsersetRewrite,
		},
		{
			name: "invalid_relation:_self_reference_in_difference_subtract",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": {
								Userset: &openfgapb.Userset_Difference{
									Difference: &openfgapb.Difference{
										Base: &openfgapb.Userset{
											Userset: &openfgapb.Userset_This{},
										},
										Subtract: &openfgapb.Userset{
											Userset: &openfgapb.Userset_ComputedUserset{
												ComputedUserset: &openfgapb.ObjectRelation{Relation: "reader"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			err: ErrInvalidUsersetRewrite,
		},
		{
			name: "invalid_relation:_computedUserset_to_relation_which_does_not_exist",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": {
								Userset: &openfgapb.Userset_ComputedUserset{
									ComputedUserset: &openfgapb.ObjectRelation{Relation: "writer"},
								},
							},
						},
					},
				},
			},
			err: ErrRelationUndefined,
		},
		{
			name: "invalid_relation:_computedUserset_in_a_union",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
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
						},
					},
				},
			},
			err: ErrRelationUndefined,
		},
		{
			name: "invalid_relation:_computedUserset_in_a_intersection",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": {
								Userset: &openfgapb.Userset_Intersection{
									Intersection: &openfgapb.Usersets{
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
						},
					},
				},
			},
			err: ErrRelationUndefined,
		},
		{
			name: "invalid_relation:_computedUserset_in_a_difference_base",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": {
								Userset: &openfgapb.Userset_Difference{
									Difference: &openfgapb.Difference{
										Base: &openfgapb.Userset{
											Userset: &openfgapb.Userset_ComputedUserset{
												ComputedUserset: &openfgapb.ObjectRelation{Relation: "writer"},
											},
										},
										Subtract: &openfgapb.Userset{
											Userset: &openfgapb.Userset_This{},
										},
									},
								},
							},
						},
					},
				},
			},
			err: ErrRelationUndefined,
		},
		{
			name: "invalid_relation:_computedUserset_in_a_difference_subtract",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": {
								Userset: &openfgapb.Userset_Difference{
									Difference: &openfgapb.Difference{
										Base: &openfgapb.Userset{
											Userset: &openfgapb.Userset_This{},
										},
										Subtract: &openfgapb.Userset{
											Userset: &openfgapb.Userset_ComputedUserset{
												ComputedUserset: &openfgapb.ObjectRelation{Relation: "writer"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			err: ErrRelationUndefined,
		},
		{
			name: "invalid_relation:_tupleToUserset_where_tupleset_is_not_valid",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "group",
						Relations: map[string]*openfgapb.Userset{
							"member": {
								Userset: &openfgapb.Userset_This{},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": {
								Userset: &openfgapb.Userset_Union{
									Union: &openfgapb.Usersets{
										Child: []*openfgapb.Userset{
											{
												Userset: &openfgapb.Userset_This{},
											},
											{
												Userset: &openfgapb.Userset_TupleToUserset{
													TupleToUserset: &openfgapb.TupleToUserset{
														Tupleset: &openfgapb.ObjectRelation{
															Relation: "notavalidrelation",
														},
														ComputedUserset: &openfgapb.ObjectRelation{
															Relation: "member",
														},
													},
												},
											},
										},
									},
								},
							},
							"writer": {
								Userset: &openfgapb.Userset_This{},
							},
						},
					},
				},
			},
			err: ErrRelationUndefined,
		},
		{
			name: "invalid_relation:_tupleToUserset_where_computed_userset_is_not_valid",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: parser.MustParse(`
				type user

				type document
				  relations
				    define reader as notavalidrelation from writer
					define writer: [user] as self
				`),
			},
			err: ErrRelationUndefined,
		},
		{
			name: "Fails_If_Using_This_As_Relation_Name",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"this": This(),
						},
					},
				},
			},
			err: ErrReservedKeywords,
		},
		{
			name: "Fails_If_Using_Self_As_Relation_Name",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"self": This(),
						},
					},
				},
			},
			err: ErrReservedKeywords,
		},
		{
			name: "Fails_If_Using_This_As_Type_Name",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "this",
						Relations: map[string]*openfgapb.Userset{
							"viewer": This(),
						},
					},
				},
			},
			err: ErrReservedKeywords,
		},
		{
			name: "Fails_If_Using_Self_As_Type_Name",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "self",
						Relations: map[string]*openfgapb.Userset{
							"viewer": This(),
						},
					},
				},
			},
			err: ErrReservedKeywords,
		},
		{
			name: "Fails_If_Auth_Model_1.1_Has_A_Cycle_And_Only_One_Type",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: parser.MustParse(`
				type folder
				  relations
				    define parent: [folder] as self
					define viewer as viewer from parent
				`),
			},
			err: ErrCycle,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewAndValidate(test.model)
			require.ErrorIs(t, err, test.err)
		})
	}
}
func TestSuccessfulRelationTypeRestrictionsValidations(t *testing.T) {
	var tests = []struct {
		name  string
		model *openfgapb.AuthorizationModel
	}{
		{
			name: "succeeds_on_a_valid_typeSystem_with_an_objectType_type",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: parser.MustParse(`
				type user

				type document
				  relations
				    define reader: [user] as self
				`),
			},
		},
		{
			name: "succeeds_on_a_valid_typeSystem_with_a_type_and_type#relation_type",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: parser.MustParse(`
				type user

				type group
				  relations
				    define admin: [user] as self
				    define member: [user] as self

				type document
				  relations
				    define reader: [user, group#member] as self
				    define writer: [user, group#admin] as self
				`),
			},
		},
		{
			name: "self_referencing_type_restriction_with_nonzero_entrypoint_1",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: parser.MustParse(`
				type user

				type document
				  relations
				    define viewer: [document#viewer] as self or editor
				    define editor: [user] as self
				`),
			},
		},
		{
			name: "self_referencing_type_restriction_with_nonzero_entrypoint_2",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: parser.MustParse(`
				type user

				type document
				  relations
				    define viewer: [document#viewer] as self or editor
				    define editor: [document] as self
				`),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewAndValidate(test.model)
			require.NoError(t, err)
		})
	}
}

func TestInvalidRelationTypeRestrictionsValidations(t *testing.T) {
	var tests = []struct {
		name  string
		model *openfgapb.AuthorizationModel
		err   error
	}{
		{
			name: "relational_type_which_does_not_exist",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": {Userset: &openfgapb.Userset_This{}},
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"reader": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type: "group",
										},
									},
								},
							},
						},
					},
				},
			},
			err: InvalidRelationTypeError("document", "reader", "group", ""),
		},
		{
			name: "relation_type_of_form_type#relation_where_relation_doesn't_exist",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "group",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": {Userset: &openfgapb.Userset_This{}},
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"reader": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type:               "group",
											RelationOrWildcard: &openfgapb.RelationReference_Relation{Relation: "admin"},
										},
									},
								},
							},
						},
					},
				},
			},
			err: InvalidRelationTypeError("document", "reader", "group", "admin"),
		},
		{
			name: "assignable_relation_with_no_type:_this",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": {
								Userset: &openfgapb.Userset_This{},
							},
						},
					},
				},
			},
			err: AssignableRelationError("document", "reader"),
		},
		{
			name: "assignable_relation_with_no_type:_union",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"writer": {
								Userset: &openfgapb.Userset_This{},
							},
							"reader": {
								Userset: &openfgapb.Userset_Union{
									Union: &openfgapb.Usersets{
										Child: []*openfgapb.Userset{
											{
												Userset: &openfgapb.Userset_This{},
											},
											{
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
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"writer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
			err: AssignableRelationError("document", "reader"),
		},
		{
			name: "assignable_relation_wit_no_type:_intersection",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"writer": {
								Userset: &openfgapb.Userset_This{},
							},
							"reader": {
								Userset: &openfgapb.Userset_Intersection{
									Intersection: &openfgapb.Usersets{
										Child: []*openfgapb.Userset{
											{
												Userset: &openfgapb.Userset_This{},
											},
											{
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
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"writer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
			err: AssignableRelationError("document", "reader"),
		},
		{
			name: "assignable_relation_with_no_type:_difference base",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"writer": {
								Userset: &openfgapb.Userset_This{},
							},
							"reader": {
								Userset: &openfgapb.Userset_Difference{
									Difference: &openfgapb.Difference{
										Base: &openfgapb.Userset{
											Userset: &openfgapb.Userset_This{},
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
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"writer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
			err: AssignableRelationError("document", "reader"),
		},
		{
			name: "assignable_relation_with_no_type:_difference_subtract",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"writer": {
								Userset: &openfgapb.Userset_This{},
							},
							"reader": {
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
											Userset: &openfgapb.Userset_This{},
										},
									},
								},
							},
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"writer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
			err: AssignableRelationError("document", "reader"),
		},
		{
			name: "non-assignable_relation_with_a_type",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"writer": {
								Userset: &openfgapb.Userset_This{},
							},
							"reader": {
								Userset: &openfgapb.Userset_ComputedUserset{
									ComputedUserset: &openfgapb.ObjectRelation{Relation: "writer"},
								},
							},
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"writer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type: "user",
										},
									},
								},
								"reader": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
			err: NonAssignableRelationError("document", "reader"),
		},
		{
			name: "userset_specified_as_allowed_type_but_the_relation_is_used_in_a_TTU_rewrite",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"member": This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent":   This(),
							"can_view": TupleToUserset("parent", "member"),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										DirectRelationReference("folder", "member"),
									},
								},
							},
						},
					},
				},
			},
			err: InvalidRelationTypeError("document", "parent", "folder", "member"),
		},
		{
			name: "userset_specified_as_allowed_type_but_the_relation_is_used_in_a_TTU_rewrite_included_in_a_union",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"parent": This(),
							"viewer": This(),
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
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": This(),
							"viewer": Union(TupleToUserset("parent", "viewer"), This()),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										DirectRelationReference("folder", "parent"),
									},
								},
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										DirectRelationReference("user", ""),
										DirectRelationReference("folder", "parent"),
									},
								},
							},
						},
					},
				},
			},
			err: InvalidRelationTypeError("document", "parent", "folder", "parent"),
		},
		{
			name: "WildcardNotAllowedInTheTuplesetPartOfTTU",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"viewer": This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": This(),
							"viewer": Union(This(), TupleToUserset("parent", "viewer")),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										WildcardRelationReference("folder"),
									},
								},
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
				},
			},
			err: InvalidRelationTypeError("document", "parent", "folder", ""),
		},
		{
			name: "self_referencing_type_restriction_with_zero_entrypoints_1",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: parser.MustParse(`
				type document
				  relations
				    define viewer: [document#viewer] as self
				`),
			},
			err: &InvalidRelationError{ObjectType: "document", Relation: "viewer", Cause: ErrCycle},
		},
		{
			name: "self_referencing_type_restriction_with_zero_entrypoints_2",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: parser.MustParse(`
				type document
				  relations
				    define viewer: [document#viewer] as self or editor
				    define editor: [document#editor] as self
				`),
			},
			err: &InvalidRelationError{ObjectType: "document", Relation: "editor", Cause: ErrCycle},
		},
		{
			name: "self_referencing_type_restriction_with_zero_entrypoints_3",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: parser.MustParse(`
				type user
				type document
				  relations
				    define viewer: [document#viewer] as self and editor
				    define editor: [user] as self
				`),
			},
			err: &InvalidRelationError{ObjectType: "document", Relation: "viewer", Cause: ErrCycle},
		},
		{
			name: "self_referencing_type_restriction_with_zero_entrypoints_4",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: parser.MustParse(`
				type user
				type document
				  relations
				    define restricted: [user] as self
				    define viewer: [document#viewer] as self but not restricted
				`),
			},
			err: &InvalidRelationError{ObjectType: "document", Relation: "viewer", Cause: ErrCycle},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewAndValidate(test.model)
			require.EqualError(t, err, test.err.Error())
		})
	}
}

func TestRelationInvolvesIntersection(t *testing.T) {
	tests := []struct {
		name        string
		model       string
		rr          *openfgapb.RelationReference
		expected    bool
		expectedErr error
	}{
		{
			name: "indirect_computeduserset_through_ttu_containing_intersection",
			model: `
			type user

			type folder
			  relations
			    define manage: [user] as self
			    define editor: [user] as self and manage

			type document
			  relations
			    define parent: [folder] as self
			    define editor as editor from parent
			    define viewer as editor
			`,
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "ttu_relations_containing_intersection",
			model: `
			type user

			type folder
			  relations
			    define editor: [user] as self
			    define viewer: [user] as self and editor

			type document
			  relations
			    define parent: [folder] as self
			    define viewer as viewer from parent
			`,
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "indirect_relations_containing_intersection",
			model: `
			type user

			type document
			  relations
			    define editor: [user] as self
			    define viewer: [user] as self and editor
			`,
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "undefined_type",
			model: `
			type user
			`,
			rr:          DirectRelationReference("document", "viewer"),
			expected:    false,
			expectedErr: ErrObjectTypeUndefined,
		},
		{
			name: "undefined_relation",
			model: `
			type user
			`,
			rr:          DirectRelationReference("user", "viewer"),
			expected:    false,
			expectedErr: ErrRelationUndefined,
		},
		{
			name: "non-assignable_indirect_type_restriction_involving_intersection",
			model: `
			type user

			type org
			  relations
			    define allowed: [user] as self
			    define dept: [group] as self
			    define dept_member as member from dept
			    define dept_allowed_member as dept_member and allowed

			type resource
			  relations
			    define reader: [user] as self or writer
			    define writer: [org#dept_allowed_member] as self
			`,
			rr:       DirectRelationReference("resource", "reader"),
			expected: true,
		},
		{
			name: "indirect_relationship_through_type_restriction",
			model: `
			type user

			type document
			  relations
			    define allowed: [user] as self
			    define editor: [user] as self and allowed
			    define viewer: [document#editor] as self
			`,
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "github_model",
			model: `
			type user

			type organization
			  relations
			    define member: [user] as self or owner
				define owner: [user] as self
				define repo_admin: [user, organization#member] as self
				define repo_reader: [user, organization#member] as self
				define repo_writer: [user, organization#member] as self

			type team
			  relations
			    define member: [user, team#member] as self

			type repo
			  relations
			    define admin: [user, team#member] as self or repo_admin from owner
				define maintainer: [user, team#member] as self or admin
				define owner: [organization] as self
				define reader: [user, team#member] as self or triager or repo_reader from owner
				define triager: [user, team#member] as self or writer
				define writer: [user, team#member] as self or maintainer or repo_writer from owner
			`,
			rr:       DirectRelationReference("repo", "admin"),
			expected: false,
		},
		{
			name: "github_model",
			model: `
			type user

			type organization
			  relations
			    define member: [user] as self or owner
				define owner: [user] as self
				define repo_admin: [user, organization#member] as self
				define repo_reader: [user, organization#member] as self
				define repo_writer: [user, organization#member] as self

			type team
			  relations
			    define member: [user, team#member] as self

			type repo
			  relations
			    define admin: [user, team#member] as self or repo_admin from owner
				define maintainer: [user, team#member] as self or admin
				define owner: [organization] as self
				define reader: [user, team#member] as self or triager or repo_reader from owner
				define triager: [user, team#member] as self or writer
				define writer: [user, team#member] as self or maintainer or repo_writer from owner
			`,
			rr:       DirectRelationReference("repo", "admin"),
			expected: false,
		},
		{
			name: "direct_relations_related_to_each_other",
			model: `
			type user

			type example
			  relations
			    define editor: [example#viewer] as self
			    define viewer: [example#editor] as self
			`,
			rr:       DirectRelationReference("example", "editor"),
			expected: false,
		},
		{
			name: "cyclical_evaluation_of_tupleset",
			model: `
			type user

			type node
			  relations
			    define parent: [node] as self
			    define editor: [user] as self or editor from parent
			`,
			rr:       DirectRelationReference("node", "editor"),
			expected: false,
		},
		{
			name: "nested_intersection_1",
			model: `
			type user

			type folder
			  relations
			    define allowed: [user] as self
			    define viewer: [user] as self and allowed

			type document
			  relations
			    define parent: [folder] as self
				define viewer as viewer from parent
			`,
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			typedefs := parser.MustParse(test.model)

			typesys := New(&openfgapb.AuthorizationModel{
				TypeDefinitions: typedefs,
			})

			objectType := test.rr.GetType()
			relationStr := test.rr.GetRelation()

			actual, err := typesys.RelationInvolvesIntersection(objectType, relationStr)
			require.ErrorIs(t, err, test.expectedErr)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestRelationInvolvesExclusion(t *testing.T) {

	tests := []struct {
		name        string
		model       string
		rr          *openfgapb.RelationReference
		expected    bool
		expectedErr error
	}{
		{
			name: "indirect_computed_userset_through_ttu_containing_exclusion",
			model: `
			type user

			type folder
			  relations
			    define restricted: [user] as self
			    define editor: [user] as self but not restricted

			type document
			  relations
			    define parent: [folder] as self
			    define editor as editor from parent
			    define viewer as editor
			`,
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "ttu_relations_containing_exclusion",
			model: `
			type user

			type folder
			  relations
			    define restricted: [user] as self
			    define viewer: [user] as self but not restricted

			type document
			  relations
			    define parent: [folder] as self
			    define viewer as viewer from parent
			`,
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "indirect_relations_containing_exclusion",
			model: `
			type user

			type document
			  relations
			    define restricted: [user] as self
			    define editor: [user] as self but not restricted
			    define viewer as editor
			`,
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "undefined_type",
			model: `
			type user
			`,
			rr:          DirectRelationReference("document", "viewer"),
			expected:    false,
			expectedErr: ErrObjectTypeUndefined,
		},
		{
			name: "undefined_relation",
			model: `
			type user
			`,
			rr:          DirectRelationReference("user", "viewer"),
			expected:    false,
			expectedErr: ErrRelationUndefined,
		},
		{
			name: "non-assignable_indirect_type_restriction_involving_exclusion",
			model: `
			type user

			type org
			  relations
			    define removed: [user] as self
			    define dept: [group] as self
			    define dept_member as member from dept
			    define dept_allowed_member as dept_member but not removed

			type resource
			  relations
			    define reader: [user] as self or writer
			    define writer: [org#dept_allowed_member] as self
			`,
			rr:       DirectRelationReference("resource", "reader"),
			expected: true,
		},
		{
			name: "indirect_relationship_through_type_restriction",
			model: `
			type user

			type document
			  relations
			    define restricted: [user] as self
			    define editor: [user] as self but not restricted
			    define viewer: [document#editor] as self
			`,
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "direct_relations_related_to_each_other",
			model: `
			type user

			type example
			  relations
			    define editor: [example#viewer] as self
			    define viewer: [example#editor] as self
			`,
			rr:       DirectRelationReference("example", "editor"),
			expected: false,
		},
		{
			name: "cyclical_evaluation_of_tupleset",
			model: `
			type user

			type node
			  relations
			    define parent: [node] as self
			    define editor: [user] as self or editor from parent
			`,
			rr:       DirectRelationReference("node", "editor"),
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			typedefs := parser.MustParse(test.model)

			typesys := New(&openfgapb.AuthorizationModel{
				TypeDefinitions: typedefs,
			})

			objectType := test.rr.GetType()
			relationStr := test.rr.GetRelation()

			actual, err := typesys.RelationInvolvesExclusion(objectType, relationStr)
			require.ErrorIs(t, err, test.expectedErr)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestIsTuplesetRelation(t *testing.T) {

	tests := []struct {
		name          string
		model         *openfgapb.AuthorizationModel
		objectType    string
		relation      string
		expected      bool
		expectedError error
	}{
		{
			name:          "undefined_object_type_returns_error",
			objectType:    "document",
			relation:      "viewer",
			expected:      false,
			expectedError: ErrObjectTypeUndefined,
		},
		{
			name: "undefined_relation_returns_error",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
					},
				},
			},
			objectType:    "document",
			relation:      "viewer",
			expected:      false,
			expectedError: ErrRelationUndefined,
		},
		{
			name: "direct_tupleset_relation",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": This(),
							"viewer": TupleToUserset("parent", "viewer"),
						},
					},
				},
			},
			objectType: "document",
			relation:   "parent",
			expected:   true,
		},
		{
			name: "tupleset_relation_under_union",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": This(),
							"viewer": Union(
								This(),
								TupleToUserset("parent", "viewer"),
							),
						},
					},
				},
			},
			objectType: "document",
			relation:   "parent",
			expected:   true,
		},
		{
			name: "tupleset_relation_under_intersection",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": This(),
							"viewer": Intersection(
								This(),
								TupleToUserset("parent", "viewer"),
							),
						},
					},
				},
			},
			objectType: "document",
			relation:   "parent",
			expected:   true,
		},
		{
			name: "tupleset_relation_under_exclusion",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": This(),
							"viewer": Difference(
								This(),
								TupleToUserset("parent", "viewer"),
							),
						},
					},
				},
			},
			objectType: "document",
			relation:   "parent",
			expected:   true,
		},
		{
			name: "tupleset_relation_under_nested_union",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": This(),
							"viewer": Intersection(
								This(),
								Union(TupleToUserset("parent", "viewer")),
							),
						},
					},
				},
			},
			objectType: "document",
			relation:   "parent",
			expected:   true,
		},
		{
			name: "tupleset_relation_under_nested_intersection",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": This(),
							"viewer": Union(
								This(),
								Intersection(TupleToUserset("parent", "viewer")),
							),
						},
					},
				},
			},
			objectType: "document",
			relation:   "parent",
			expected:   true,
		},
		{
			name: "tupleset_relation_under_nested_exclusion",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": This(),
							"viewer": Union(
								This(),
								Difference(This(), TupleToUserset("parent", "viewer")),
							),
						},
					},
				},
			},
			objectType: "document",
			relation:   "parent",
			expected:   true,
		},
		{
			name: "not_a_tupleset_relation",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": This(),
							"viewer": TupleToUserset("parent", "viewer"),
						},
					},
				},
			},
			objectType: "document",
			relation:   "viewer",
			expected:   false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			typesys := New(test.model)

			actual, err := typesys.IsTuplesetRelation(test.objectType, test.relation)
			require.ErrorIs(t, err, test.expectedError)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestIsDirectlyRelated(t *testing.T) {
	tests := []struct {
		name   string
		model  string
		target *openfgapb.RelationReference
		source *openfgapb.RelationReference
		result bool
	}{
		{
			name: "wildcard_and_wildcard",
			model: `
			type user

			type document
			  relations
			    define viewer: [user:*] as self
			`,
			target: DirectRelationReference("document", "viewer"),
			source: WildcardRelationReference("user"),
			result: true,
		},
		{
			name: "wildcard_and_direct",
			model: `
			type user

			type document
			  relations
			    define viewer: [user:*] as self
			`,
			target: DirectRelationReference("document", "viewer"),
			source: DirectRelationReference("user", ""),
			result: false,
		},
		{
			name: "direct_and_wildcard",
			model: `
			type user
			
			type document
			  relations
			    define viewer: [user] as self
			`,
			target: DirectRelationReference("document", "viewer"),
			source: WildcardRelationReference("user"),
			result: false,
		},
		{
			name: "direct_type",
			model: `
			type user
			
			type document
			  relations
			    define viewer: [user] as self
			`,
			target: DirectRelationReference("document", "viewer"),
			source: DirectRelationReference("user", ""),
			result: true,
		},
		{
			name: "relation_not_related",
			model: `
			type user
			  relations
			    define manager: [user] as self
			
			type document
			  relations
			    define viewer: [user] as self
			`,
			target: DirectRelationReference("document", "viewer"),
			source: DirectRelationReference("user", "manager"),
			result: false,
		},
		{
			name: "direct_and_userset",
			model: `
			type group
			  relations
			    define member: [group#member] as self
			
			type document
			  relations
			    define viewer: [group#member] as self
			`,
			target: DirectRelationReference("document", "viewer"),
			source: DirectRelationReference("group", "member"),
			result: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			typedefs := parser.MustParse(test.model)
			typesys := New(&openfgapb.AuthorizationModel{
				SchemaVersion:   SchemaVersion1_1,
				TypeDefinitions: typedefs,
			})

			ok, err := typesys.IsDirectlyRelated(test.target, test.source)
			require.NoError(t, err)
			require.Equal(t, test.result, ok)
		})
	}
}

func TestIsPubliclyAssignable(t *testing.T) {
	tests := []struct {
		name       string
		model      string
		target     *openfgapb.RelationReference
		objectType string
		result     bool
	}{
		{
			name: "1",
			model: `
			type user

			type document
			  relations
			    define viewer: [user:*] as self
			`,
			target:     DirectRelationReference("document", "viewer"),
			objectType: "user",
			result:     true,
		},
		{
			name: "2",
			model: `
			type user

			type document
			  relations
			    define viewer: [user] as self
			`,
			target:     DirectRelationReference("document", "viewer"),
			objectType: "user",
			result:     false,
		},
		{
			name: "3",
			model: `
			type user
			type employee

			type document
			  relations
			    define viewer: [employee:*] as self
			`,
			target:     DirectRelationReference("document", "viewer"),
			objectType: "user",
			result:     false,
		},
		{
			name: "4",
			model: `
			type user

			type group
			  relations
			    define member: [user:*] as self

			type document
			  relations
			    define viewer: [group#member] as self
			`,
			target:     DirectRelationReference("document", "viewer"),
			objectType: "user",
			result:     false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			typedefs := parser.MustParse(test.model)
			typesys := New(&openfgapb.AuthorizationModel{
				SchemaVersion:   SchemaVersion1_1,
				TypeDefinitions: typedefs,
			})

			ok, err := typesys.IsPubliclyAssignable(test.target, test.objectType)
			require.NoError(t, err)
			require.Equal(t, ok, test.result)
		})
	}
}

func TestRewriteContainsExclusion(t *testing.T) {
	tests := []struct {
		name     string
		model    string
		rr       *openfgapb.RelationReference
		expected bool
	}{
		{
			name: "simple_exclusion",
			model: `
			type user

			type folder
			  relations
			    define restricted: [user] as self
			    define editor: [user] as self
			    define viewer: [user] as (self or editor) but not restricted
			`,
			rr:       DirectRelationReference("folder", "viewer"),
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			typedefs := parser.MustParse(test.model)

			typesys := New(&openfgapb.AuthorizationModel{
				TypeDefinitions: typedefs,
			})

			rel, err := typesys.GetRelation(test.rr.GetType(), test.rr.GetRelation())
			require.NoError(t, err)

			actual := RewriteContainsExclusion(rel.GetRewrite())
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestRewriteContainsIntersection(t *testing.T) {
	tests := []struct {
		name     string
		model    string
		rr       *openfgapb.RelationReference
		expected bool
	}{
		{
			name: "simple_intersection",
			model: `
			type user

			type folder
			  relations
			    define allowed: [user] as self
			    define editor: [user] as self
			    define viewer: [user] as (self or editor) and allowed
			`,
			rr:       DirectRelationReference("folder", "viewer"),
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			typedefs := parser.MustParse(test.model)

			typesys := New(&openfgapb.AuthorizationModel{
				TypeDefinitions: typedefs,
			})

			rel, err := typesys.GetRelation(test.rr.GetType(), test.rr.GetRelation())
			require.NoError(t, err)

			actual := RewriteContainsIntersection(rel.GetRewrite())
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestGetRelationReferenceAsString(t *testing.T) {
	require.Equal(t, "", GetRelationReferenceAsString(nil))
	require.Equal(t, "team#member", GetRelationReferenceAsString(DirectRelationReference("team", "member")))
	require.Equal(t, "team:*", GetRelationReferenceAsString(WildcardRelationReference("team")))
}
