package typesystem

import (
	"testing"

	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestSchemaVersion(t *testing.T) {
	t.Run("convert string to SchemaVersion successfully", func(t *testing.T) {
		var tests = []struct {
			input  string
			output SchemaVersion
		}{
			{
				input:  "",
				output: SchemaVersion1_0,
			},
			{
				input:  "1.0",
				output: SchemaVersion1_0,
			},
			{
				input:  "1.1",
				output: SchemaVersion1_1,
			},
		}

		for _, test := range tests {
			version, err := NewSchemaVersion(test.input)
			require.NoError(t, err)
			require.Equal(t, test.output, version)
		}
	})

	t.Run("convert string to SchemaVersion errors", func(t *testing.T) {
		var tests = []struct {
			input string
		}{
			{
				input: "1.2",
			},
			{
				input: "1.3",
			},
		}

		for _, test := range tests {
			_, err := NewSchemaVersion(test.input)
			require.ErrorIs(t, err, ErrInvalidSchemaVersion)
		}
	})

	t.Run("convert from SchemaVersion to string", func(t *testing.T) {
		var tests = []struct {
			input  SchemaVersion
			output string
		}{
			{
				input:  SchemaVersion1_0,
				output: "1.0",
			},
			{
				input:  SchemaVersion1_1,
				output: "1.1",
			},
			{
				input:  SchemaVersionUnspecified,
				output: "unspecified",
			},
		}

		for _, test := range tests {
			require.Equal(t, test.output, test.input.String())
		}
	})
}

func TestSuccessfulValidateRelationRewrites(t *testing.T) {
	var tests = []struct {
		name       string
		typeSystem *TypeSystem
	}{
		{
			name: "empty relations",
			typeSystem: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"repo": {
						Type: "repo",
					},
				},
			},
		},
		{
			name: "zero length relations is valid",
			typeSystem: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"repo": {
						Type:      "repo",
						Relations: map[string]*openfgapb.Userset{},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.typeSystem.ValidateRelationRewrites()
			require.NoError(t, err)
		})
	}
}

func TestInvalidValidateRelationRewrites(t *testing.T) {
	var tests = []struct {
		name  string
		model *TypeSystem
		err   error
	}{
		{
			name: "empty rewrites",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"document": {
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": {},
						},
					},
				},
			},
			err: InvalidRelationError("document", "reader"),
		},
		{
			name: "invalid relation: self reference in computedUserset",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"document": {
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
			err: InvalidRelationError("document", "reader"),
		},
		{
			name: "invalid relation: self reference in union",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"document": {
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
			err: InvalidRelationError("document", "reader"),
		},
		{
			name: "invalid relation: self reference in intersection",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"document": {
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
			err: InvalidRelationError("document", "reader"),
		},
		{
			name: "invalid relation: self reference in difference base",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"document": {
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
			err: InvalidRelationError("document", "reader"),
		},
		{
			name: "invalid relation: self reference in difference subtract",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"document": {
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
			err: InvalidRelationError("document", "reader"),
		},
		{
			name: "invalid relation: computedUserset to relation which does not exist",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"document": {
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
			err: RelationDoesNotExistError("document", "writer"),
		},
		{
			name: "invalid relation: computedUserset in a union",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"document": {
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
			err: RelationDoesNotExistError("document", "writer"),
		},
		{
			name: "invalid relation: computedUserset in a intersection",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"document": {
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
			err: RelationDoesNotExistError("document", "writer"),
		},
		{
			name: "invalid relation: computedUserset in a difference base",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"document": {
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
			err: RelationDoesNotExistError("document", "writer"),
		},
		{
			name: "invalid relation: computedUserset in a difference subtract",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"document": {
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
			err: RelationDoesNotExistError("document", "writer"),
		},
		{
			name: "invalid relation: tupleToUserset where tupleset is not valid",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"group": {
						Type: "group",
						Relations: map[string]*openfgapb.Userset{
							"member": {
								Userset: &openfgapb.Userset_This{},
							},
						},
					},
					"document": {
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
			err: RelationDoesNotExistError("document", "notavalidrelation"),
		},
		{
			name: "invalid relation: tupleToUserset where computed userset is not valid",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"group": {
						Type: "group",
						Relations: map[string]*openfgapb.Userset{
							"member": {
								Userset: &openfgapb.Userset_This{},
							},
						},
					},
					"document": {
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
			err: RelationDoesNotExistError("", "notavalidrelation"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.model.ValidateRelationRewrites()
			require.EqualError(t, err, test.err.Error())
		})
	}
}

func TestSuccessfulValidateRelationTypeRestrictions(t *testing.T) {
	var tests = []struct {
		name  string
		model *TypeSystem
	}{
		{
			name: "succeeds on a valid typeSystem with an objectType type",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"user": {
						Type: "user",
					},
					"document": {
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
			name: "succeeds on a valid typeSystem with a type and type#relation type",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"user": {
						Type: "user",
					},
					"group": {
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
					"document": {
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": {Userset: &openfgapb.Userset_This{}},
							"writer": {Userset: &openfgapb.Userset_This{}},
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"reader": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type: "user",
										},
										{
											Type:     "group",
											Relation: "member",
										},
									},
								},
								"writer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type: "user",
										},
										{
											Type:     "group",
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.model.ValidateRelationTypeRestrictions()
			require.NoError(t, err)
		})
	}
}

func TestInvalidAuthorizationModelWithTypeValidations(t *testing.T) {
	var tests = []struct {
		name  string
		model *TypeSystem
		err   error
	}{
		{
			name: "relational type which does not exist",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"document": {
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
			name: "relation type of form type#relation where relation doesn't exist",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"group": {
						Type: "group",
					},
					"document": {
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": {Userset: &openfgapb.Userset_This{}},
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"reader": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type:     "group",
											Relation: "admin",
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
			name: "assignable relation with no type: this",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"document": {
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
			name: "assignable relation with no type: union",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"user": {
						Type: "user",
					},
					"document": {
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
			name: "assignable relation with no type: intersection",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"user": {
						Type: "user",
					},
					"document": {
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
			name: "assignable relation with no type: difference base",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"user": {
						Type: "user",
					},
					"document": {
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
			name: "assignable relation with no type: difference subtract",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"user": {
						Type: "user",
					},
					"document": {
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
			name: "non-assignable relation with a type",
			model: &TypeSystem{
				TypeDefinitions: map[string]*openfgapb.TypeDefinition{
					"user": {
						Type: "user",
					},
					"document": {
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.model.ValidateRelationTypeRestrictions()
			require.EqualError(t, err, test.err.Error())
		})
	}
}
