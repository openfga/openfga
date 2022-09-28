package typesystem

import (
	"testing"

	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestValidateFillsInEmptySchemaVersion(t *testing.T) {
	model, err := Validate(&openfgapb.AuthorizationModel{
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repo",
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "1.0", model.SchemaVersion)
}

func TestSuccessfulRewriteValidations(t *testing.T) {
	var tests = []struct {
		name  string
		model *openfgapb.AuthorizationModel
	}{
		{
			name: "empty relations",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.0",
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
					},
				},
			},
		},
		{
			name: "zero length relations is valid",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.0",
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
			_, err := Validate(test.model)
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
			name: "empty rewrites",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.0",
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
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
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.0",
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
			err: InvalidRelationError("document", "reader"),
		},
		{
			name: "invalid relation: self reference in union",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.0",
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
			err: InvalidRelationError("document", "reader"),
		},
		{
			name: "invalid relation: self reference in intersection",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.0",
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
			err: InvalidRelationError("document", "reader"),
		},
		{
			name: "invalid relation: self reference in difference base",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.0",
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
			err: InvalidRelationError("document", "reader"),
		},
		{
			name: "invalid relation: self reference in difference subtract",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.0",
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
			err: InvalidRelationError("document", "reader"),
		},
		{
			name: "invalid relation: computedUserset to relation which does not exist",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.0",
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
			err: RelationDoesNotExistError("document", "writer"),
		},
		{
			name: "invalid relation: computedUserset in a union",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.0",
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
			err: RelationDoesNotExistError("document", "writer"),
		},
		{
			name: "invalid relation: computedUserset in a intersection",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.0",
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
			err: RelationDoesNotExistError("document", "writer"),
		},
		{
			name: "invalid relation: computedUserset in a difference base",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.0",
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
			err: RelationDoesNotExistError("document", "writer"),
		},
		{
			name: "invalid relation: computedUserset in a difference subtract",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.0",
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
			err: RelationDoesNotExistError("document", "writer"),
		},
		{
			name: "invalid relation: tupleToUserset where tupleset is not valid",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.0",
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
			err: RelationDoesNotExistError("document", "notavalidrelation"),
		},
		{
			name: "invalid relation: tupleToUserset where computed userset is not valid",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.0",
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
			err: RelationDoesNotExistError("", "notavalidrelation"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Validate(test.model)
			require.EqualError(t, err, test.err.Error())
		})
	}
}
func TestSuccessfulRelationTypeRestrictionsValidations(t *testing.T) {
	var tests = []struct {
		name  string
		model *openfgapb.AuthorizationModel
	}{
		{
			name: "succeeds on a valid typeSystem with an objectType type",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.1",
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
			name: "succeeds on a valid typeSystem with a type and type#relation type",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.1",
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
			_, err := Validate(test.model)
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
			name: "relational type which does not exist",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.1",
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
			name: "relation type of form type#relation where relation doesn't exist",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.1",
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
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.1",
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
			name: "assignable relation with no type: union",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.1",
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
			name: "assignable relation with no type: intersection",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.1",
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
			name: "assignable relation with no type: difference base",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.1",
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
			name: "assignable relation with no type: difference subtract",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.1",
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
			name: "non-assignable relation with a type",
			model: &openfgapb.AuthorizationModel{
				SchemaVersion: "1.1",
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Validate(test.model)
			require.EqualError(t, err, test.err.Error())
		})
	}
}
