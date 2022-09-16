package typesystem

import (
	"testing"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/server/errors"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestSchemaVersion(t *testing.T) {
	t.Run("convert from string to SchemaVersion", func(t *testing.T) {
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
			{
				input:  "1.2",
				output: SchemaVersionUnspecified,
			},
			{
				input:  "xyz",
				output: SchemaVersionUnspecified,
			},
		}

		for _, test := range tests {
			require.Equal(t, test.output, NewSchemaVersion(test.input))
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

func TestSuccessfulAuthorizationModelValidations(t *testing.T) {
	id, err := id.NewString()
	require.NoError(t, err)

	var tests = []struct {
		name  string
		model *AuthorizationModel
	}{
		{
			name: "empty relations",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
					},
				},
			},
		},
		{
			name: "zero length relations is valid",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_1,
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
			err := test.model.Validate()
			require.NoError(t, err)
		})
	}
}

func TestInvalidAuthorizationModelValidations(t *testing.T) {
	id, err := id.NewString()
	require.NoError(t, err)

	var tests = []struct {
		name  string
		model *AuthorizationModel
		err   error
	}{
		{
			name: "duplicate types",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
					},
					{
						Type: "document",
					},
				},
			},
			err: errors.CannotAllowDuplicateTypesInOneRequest,
		},
		{
			name: "empty rewrites",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"reader": {},
						},
					},
				},
			},
			err: errors.EmptyRewrites("document", "reader"),
		},
		{
			name: "invalid relation: self reference in computedUserset",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_0,
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
			err: errors.CannotAllowMultipleReferencesToOneRelation,
		},
		{
			name: "invalid relation: self reference in union",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_0,
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
			err: errors.CannotAllowMultipleReferencesToOneRelation,
		},
		{
			name: "invalid relation: self reference in intersection",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_0,
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
			err: errors.CannotAllowMultipleReferencesToOneRelation,
		},
		{
			name: "invalid relation: self reference in difference base",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_0,
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
			err: errors.CannotAllowMultipleReferencesToOneRelation,
		},
		{
			name: "invalid relation: self reference in difference subtract",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_0,
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
			err: errors.CannotAllowMultipleReferencesToOneRelation,
		},
		{
			name: "invalid relation: computedUserset to relation which does not exist",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_0,
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
			err: errors.RelationNotFound("writer", "document", nil),
		},
		{
			name: "invalid relation: computedUserset in a union",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_0,
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
			err: errors.RelationNotFound("writer", "document", nil),
		},
		{
			name: "invalid relation: computedUserset in a intersection",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_0,
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
			err: errors.RelationNotFound("writer", "document", nil),
		},
		{
			name: "invalid relation: computedUserset in a difference base",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_0,
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
			err: errors.RelationNotFound("writer", "document", nil),
		},
		{
			name: "invalid relation: computedUserset in a difference subtract",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_0,
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
			err: errors.RelationNotFound("writer", "document", nil),
		},
		{
			name: "invalid relation: tupleToUserset where tupleset is not valid",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_0,
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
			err: errors.RelationNotFound("notavalidrelation", "document", nil),
		},
		{
			name: "invalid relation: tupleToUserset where computed userset is not valid",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_0,
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
			err: errors.RelationNotFound("notavalidrelation", "", nil),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.model.Validate()
			require.ErrorIs(t, err, test.err)
		})
	}
}

func TestSuccessfulAuthorizationModelWithTypeValidations(t *testing.T) {
	id, err := id.NewString()
	require.NoError(t, err)

	var tests = []struct {
		name  string
		model *AuthorizationModel
	}{
		{
			name: "succeeds on a valid model with an objectType type",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_1,
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
			name: "succeeds on a valid model with a type and type#relation type",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_1,
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
			err := test.model.Validate()
			require.NoError(t, err)
		})
	}
}

func TestInvalidAuthorizationModelWithTypeValidations(t *testing.T) {
	id, err := id.NewString()
	require.NoError(t, err)

	var tests = []struct {
		name  string
		model *AuthorizationModel
		err   error
	}{
		{
			name: "relational type which does not exist",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_1,
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
			err: errors.InvalidRelationType("document", "reader", "group", ""),
		},
		{
			name: "relation type of form type#relation where relation doesn't exist",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_1,
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
			err: errors.InvalidRelationType("document", "reader", "group", "admin"),
		},
		{
			name: "assignable relation with no type: this",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_1,
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
			err: errors.AssignableRelationHasNoTypes("document", "reader"),
		},
		{
			name: "assignable relation with no type: union",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_1,
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
			err: errors.AssignableRelationHasNoTypes("document", "reader"),
		},
		{
			name: "assignable relation with no type: intersection",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_1,
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
			err: errors.AssignableRelationHasNoTypes("document", "reader"),
		},
		{
			name: "assignable relation with no type: difference base",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_1,
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
			err: errors.AssignableRelationHasNoTypes("document", "reader"),
		},
		{
			name: "assignable relation with no type: difference subtract",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_1,
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
			err: errors.AssignableRelationHasNoTypes("document", "reader"),
		},
		{
			name: "non-assignable relation with a type",
			model: &AuthorizationModel{
				Id:      id,
				Version: SchemaVersion1_1,
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
			err: errors.NonassignableRelationHasAType("document", "reader"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.model.Validate()
			require.ErrorIs(t, err, test.err)
		})
	}
}
