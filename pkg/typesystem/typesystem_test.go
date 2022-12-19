package typesystem

import (
	"testing"

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
				SchemaVersion: SchemaVersion1_0,
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
				SchemaVersion: SchemaVersion1_0,
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
			err := Validate(test.model)
			require.NoError(t, err)
		})
	}
}

// TODO: update these to v1.1 models and combine the test functions
func TestInvalidRewriteValidations(t *testing.T) {
	var tests = []struct {
		name  string
		model *openfgapb.AuthorizationModel
		err   error
	}{
		{
			name: "empty_rewrites",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_0,
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
				SchemaVersion: SchemaVersion1_0,
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
				SchemaVersion: SchemaVersion1_0,
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
				SchemaVersion: SchemaVersion1_0,
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
				SchemaVersion: SchemaVersion1_0,
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
				SchemaVersion: SchemaVersion1_0,
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
				SchemaVersion: SchemaVersion1_0,
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
				SchemaVersion: SchemaVersion1_0,
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
				SchemaVersion: SchemaVersion1_0,
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
				SchemaVersion: SchemaVersion1_0,
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
				SchemaVersion: SchemaVersion1_0,
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
				SchemaVersion: SchemaVersion1_0,
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
				SchemaVersion: SchemaVersion1_0,
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
															Relation: "writer",
														},
														ComputedUserset: &openfgapb.ObjectRelation{
															Relation: "notavalidrelation",
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := Validate(test.model)
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
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
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
											Type: "user",
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
			name: "succeeds_on_a_valid_typeSystem_with_a_type_and_type#relation_type",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
						Relations: map[string]*openfgapb.Userset{
							"admin":  {Userset: &openfgapb.Userset_This{}},
							"member": {Userset: &openfgapb.Userset_This{}},
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"admin": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type: "user",
										},
									},
								},
								"member": {
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
							"reader": {Userset: &openfgapb.Userset_This{}},
							"writer": {Userset: &openfgapb.Userset_This{}},
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"reader": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										DirectRelationReference("user", ""),
										DirectRelationReference("group", "member"),
									},
								},
								"writer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										DirectRelationReference("user", ""),
										DirectRelationReference("group", "admin"),
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := Validate(test.model)
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := Validate(test.model)
			require.EqualError(t, err, test.err.Error())
		})
	}
}

func TestRelationInvolvesIntersection(t *testing.T) {
	tests := []struct {
		name        string
		model       *openfgapb.AuthorizationModel
		rr          *openfgapb.RelationReference
		expected    bool
		expectedErr error
	}{
		{
			name: "Indirect_ComputedUserset_through_TTU_Containing_Intersection",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": This(),
							"editor": TupleToUserset("parent", "editor"),
							"viewer": ComputedUserset("editor"),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										DirectRelationReference("folder", ""),
									},
								},
							},
						},
					},
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"manage": This(),
							"editor": Intersection(This(), ComputedUserset("manage")),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"manage": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										DirectRelationReference("user", ""),
									},
								},
								"editor": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
				},
			},
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "TupleToUserset_Relations_Containing_Intersection",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": This(),
							"viewer": TupleToUserset("parent", "viewer"),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										DirectRelationReference("folder", ""),
									},
								},
							},
						},
					},
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"editor": This(),
							"viewer": Intersection(This(), ComputedUserset("editor")),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"editor": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										DirectRelationReference("user", ""),
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
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "Indirect_Relations_Containing_Intersection",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"editor": Intersection(
								This(),
							),
							"viewer": ComputedUserset("editor"),
						},
					},
				},
			},
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "Undefined_type",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
				},
			},
			rr:          DirectRelationReference("document", "viewer"),
			expected:    false,
			expectedErr: ErrObjectTypeUndefined,
		},
		{
			name: "Undefined_relation",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
				},
			},
			rr:          DirectRelationReference("user", "viewer"),
			expected:    false,
			expectedErr: ErrRelationUndefined,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			typesys := New(test.model)

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
		model       *openfgapb.AuthorizationModel
		rr          *openfgapb.RelationReference
		expected    bool
		expectedErr error
	}{
		{
			name: "Indirect_ComputedUserset_through_TTU_Containing_Exclusion",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": This(),
							"editor": TupleToUserset("parent", "editor"),
							"viewer": ComputedUserset("editor"),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										DirectRelationReference("folder", ""),
									},
								},
							},
						},
					},
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"restricted": This(),
							"editor":     Difference(This(), ComputedUserset("restricted")),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"restricted": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										DirectRelationReference("user", ""),
									},
								},
								"editor": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
				},
			},
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "TupleToUserset_Relations_Containing_Exclusion",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": This(),
							"viewer": TupleToUserset("parent", "viewer"),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										DirectRelationReference("folder", ""),
									},
								},
							},
						},
					},
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"restricted": This(),
							"viewer":     Difference(This(), ComputedUserset("restricted")),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"restricted": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										DirectRelationReference("user", ""),
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
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "Indirect_Relations_Containing_Exclusion",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"restricted": This(),
							"editor": Difference(
								This(),
								ComputedUserset("restricted"),
							),
							"viewer": ComputedUserset("editor"),
						},
					},
				},
			},
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "Undefined_type",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
				},
			},
			rr:          DirectRelationReference("document", "viewer"),
			expected:    false,
			expectedErr: ErrObjectTypeUndefined,
		},
		{
			name: "Undefined_relation",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
				},
			},
			rr:          DirectRelationReference("user", "viewer"),
			expected:    false,
			expectedErr: ErrRelationUndefined,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			typesys := New(test.model)

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
		model  *openfgapb.AuthorizationModel
		target *openfgapb.RelationReference
		source *openfgapb.RelationReference
		result bool
	}{
		{
			name: "wildcard_and_wildcard",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"viewer": This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										WildcardRelationReference("user"),
									},
								},
							},
						},
					},
				},
			},
			target: DirectRelationReference("document", "viewer"),
			source: WildcardRelationReference("user"),
			result: true,
		},
		{
			name: "wildcard_and_direct",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"viewer": This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										WildcardRelationReference("user"),
									},
								},
							},
						},
					},
				},
			},
			target: DirectRelationReference("document", "viewer"),
			source: DirectRelationReference("user", ""),
			result: true,
		},
		{
			name: "direct_and_wildcard",
			model: &openfgapb.AuthorizationModel{
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
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
				},
			},
			target: DirectRelationReference("document", "viewer"),
			source: WildcardRelationReference("user"),
			result: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			typesys := New(test.model)

			ok, err := typesys.IsDirectlyRelated(test.target, test.source)
			require.NoError(t, err)
			require.Equal(t, ok, test.result)
		})
	}
}
