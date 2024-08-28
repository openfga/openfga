package typesystem

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/stretchr/testify/require"
	"testing"
)

func usersetsEquals(t *testing.T, a, b *openfgav1.Usersets) {
	if a == nil || b == nil {
		require.Equal(t, a, b)
		return
	}
	require.Equal(t, len(a.Child), len(b.Child))
	for i := 0; i < len(a.Child); i++ {
		rewriteEquals(t, a.Child[i], b.Child[i])
	}
}

func tupleToUsersetEquals(t *testing.T, a, b *openfgav1.TupleToUserset) {
	if a == nil || b == nil {
		require.Equal(t, a, b)
		return
	}

	if a.ComputedUserset == nil || b.ComputedUserset == nil {
		require.Equal(t, a.ComputedUserset, b.ComputedUserset)
		return
	}
	objectRelationEquals(t, a.ComputedUserset, b.ComputedUserset)
}

func objectRelationEquals(t *testing.T, a, b *openfgav1.ObjectRelation) {
	if a == nil || b == nil {
		require.Equal(t, a, b)
		return
	}
	require.Equal(t, a.Relation, b.Relation)
	require.Equal(t, a.Object, b.Object)
}

func rewriteEquals(t *testing.T, a, b *openfgav1.Userset) {
	if a == nil || b == nil {
		require.Equal(t, a, b)
		return
	}

	_, okA := a.Userset.(*openfgav1.Userset_This)
	_, okB := b.Userset.(*openfgav1.Userset_This)
	require.Equal(t, okA, okB, "one is Userset_This")

	cuA, okA := a.Userset.(*openfgav1.Userset_ComputedUserset)
	cuB, okB := b.Userset.(*openfgav1.Userset_ComputedUserset)
	require.Equal(t, okA, okB, "one is Userset_ComputedUserset")
	if cuA == nil || cuB == nil {
		require.Equal(t, cuA, cuB)
		return
	}
	objectRelationEquals(t, cuA.ComputedUserset, cuB.ComputedUserset)

	ttuA, okA := a.Userset.(*openfgav1.Userset_TupleToUserset)
	ttuB, okB := b.Userset.(*openfgav1.Userset_TupleToUserset)
	require.Equal(t, okA, okB, "one is Userset_TupleToUserset")
	if ttuA == nil || ttuB == nil {
		require.Equal(t, ttuA, ttuB)
		return
	}
	tupleToUsersetEquals(t, ttuA.TupleToUserset, ttuB.TupleToUserset)

	uA, okA := a.Userset.(*openfgav1.Userset_Union)
	uB, okB := b.Userset.(*openfgav1.Userset_Union)
	require.Equal(t, okA, okB, "one is Userset_Union")
	if uA == nil || uB == nil {
		require.Equal(t, uA, uB)
		return
	}
	usersetsEquals(t, uA.Union, uB.Union)

	iA, okA := a.Userset.(*openfgav1.Userset_Intersection)
	iB, okB := b.Userset.(*openfgav1.Userset_Intersection)
	require.Equal(t, okA, okB, "one is Userset_Intersection")
	if iA == nil || iB == nil {
		require.Equal(t, iA, iB)
		return
	}
	usersetsEquals(t, iA.Intersection, iB.Intersection)

	dA, okA := a.Userset.(*openfgav1.Userset_Difference)
	dB, okB := b.Userset.(*openfgav1.Userset_Difference)
	require.Equal(t, okA, okB, "one is Userset_Difference")
	if dA == nil || dB == nil {
		require.Equal(t, dA, dB)
		return
	}
	if dA.Difference == nil || dB.Difference == nil {
		require.Equal(t, dA.Difference, dB.Difference)
		return
	}
	rewriteEquals(t, dA.Difference.Base, dB.Difference.Base)
	rewriteEquals(t, dA.Difference.Subtract, dB.Difference.Subtract)
}

func relationReferenceEquals(t *testing.T, a, b *openfgav1.RelationReference) {
	if a == nil || b == nil {
		require.Equal(t, a, b)
		return
	}
	require.Equal(t, a.Condition, b.Condition)
	require.Equal(t, a.Type, b.Type)

	if a.RelationOrWildcard == nil || b.RelationOrWildcard == nil {
		require.Equal(t, a.RelationOrWildcard, b.RelationOrWildcard)
		return
	}
	wA, okA := a.RelationOrWildcard.(*openfgav1.RelationReference_Wildcard)
	wB, okB := b.RelationOrWildcard.(*openfgav1.RelationReference_Wildcard)
	require.Equal(t, okA, okB)
	if wA == nil || wB == nil {
		require.Equal(t, wA, wB)
		return
	}
	require.Equal(t, wA.Wildcard, wB.Wildcard)

	rA, okA := a.RelationOrWildcard.(*openfgav1.RelationReference_Relation)
	rB, okB := b.RelationOrWildcard.(*openfgav1.RelationReference_Relation)
	require.Equal(t, okA, okB)
	if rA == nil || rB == nil {
		require.Equal(t, rA, rB)
		return
	}
	require.Equal(t, rA.Relation, rB.Relation)
}

func relationTypeInfoEquals(t *testing.T, a, b *openfgav1.RelationTypeInfo) {
	if a == nil || b == nil {
		require.Equal(t, a, b)
		return
	}

	if a.DirectlyRelatedUserTypes == nil || b.DirectlyRelatedUserTypes == nil {
		require.Equal(t, a.DirectlyRelatedUserTypes, b.DirectlyRelatedUserTypes)
		return
	}
	require.Equal(t, len(a.DirectlyRelatedUserTypes), len(b.DirectlyRelatedUserTypes))
	for i := 0; i < len(a.DirectlyRelatedUserTypes); i++ {
		relationReferenceEquals(t, a.DirectlyRelatedUserTypes[i], b.DirectlyRelatedUserTypes[i])
	}
}

func relationEquals(t *testing.T, a, b *openfgav1.Relation) {
	require.Equal(t, a.Name, b.Name)
	rewriteEquals(t, a.Rewrite, b.Rewrite)
	relationTypeInfoEquals(t, a.TypeInfo, b.TypeInfo)
}

func sourceInfoEquals(t *testing.T, a, b *openfgav1.SourceInfo) {
	if a == nil || b == nil {
		require.Equal(t, a, b)
		return
	}
	require.Equal(t, a.File, b.File)
}

func relationMetadataEquals(t *testing.T, a, b *openfgav1.RelationMetadata) {
	if a == nil || b == nil {
		require.Equal(t, a, b)
		return
	}
	require.Equal(t, a.Module, b.Module)
	sourceInfoEquals(t, a.SourceInfo, b.SourceInfo)

	if a.DirectlyRelatedUserTypes == nil || b.DirectlyRelatedUserTypes == nil {
		require.Equal(t, a.DirectlyRelatedUserTypes, b.DirectlyRelatedUserTypes)
		return
	}
	require.Equal(t, len(a.DirectlyRelatedUserTypes), len(b.DirectlyRelatedUserTypes))
	for i := 0; i < len(a.DirectlyRelatedUserTypes); i++ {
		relationReferenceEquals(t, a.DirectlyRelatedUserTypes[i], b.DirectlyRelatedUserTypes[i])
	}
}

func metadataEquals(t *testing.T, a, b *openfgav1.Metadata) {
	if a == nil || b == nil {
		require.Equal(t, a, b)
		return
	}
	require.Equal(t, a.Module, b.Module)
	sourceInfoEquals(t, a.SourceInfo, b.SourceInfo)

	if a.Relations == nil || b.Relations == nil {
		require.Equal(t, a.Relations, b.Relations)
		return
	}
	require.Equal(t, len(a.Relations), len(b.Relations))
	for nameA, rA := range a.Relations {
		rB, ok := b.Relations[nameA]
		require.True(t, ok, nameA)
		relationMetadataEquals(t, rA, rB)
	}
}

func typeDefinitionEquals(t *testing.T, a, b *openfgav1.TypeDefinition) {
	require.Equal(t, a.Type, b.Type)
	require.Equal(t, len(a.Relations), len(b.Relations))

	for nameA, rA := range a.Relations {
		rB, ok := b.Relations[nameA]
		require.True(t, ok)
		rewriteEquals(t, rA, rB)
	}
	metadataEquals(t, a.Metadata, b.Metadata)
}

func typeSystemEquals(t *testing.T, a, b *TypeSystem) {
	require.Equal(t, a.schemaVersion, b.schemaVersion)
	require.Equal(t, len(a.typeDefinitions), len(b.typeDefinitions))

	for nameA, tdA := range a.typeDefinitions {
		tdB, ok := b.typeDefinitions[nameA]
		require.True(t, ok)
		typeDefinitionEquals(t, tdA, tdB)
	}

	require.Equal(t, len(a.relations), len(b.relations))

	for typeNameA, relationsA := range a.relations {
		relationsB, ok := b.relations[typeNameA]
		require.True(t, ok)
		require.Equal(t, len(relationsA), len(relationsB))
		for relationNameA, relationA := range relationsA {
			relationB, ok := relationsB[relationNameA]
			require.True(t, ok)
			relationEquals(t, relationA, relationB)
		}
	}

	require.Equal(t, len(a.ttuRelations), len(b.ttuRelations))
	for typeNameA, relationsA := range a.ttuRelations {
		relationsB, ok := b.ttuRelations[typeNameA]
		require.True(t, ok)
		require.Equal(t, len(relationsA), len(relationsB))
		for relationNameA, relationA := range relationsA {
			relationB, ok := relationsB[relationNameA]
			require.True(t, ok)
			require.Equal(t, len(relationA), len(relationB))
			for i := 0; i < len(relationA); i++ {
				tupleToUsersetEquals(t, relationA[i], relationB[i])
			}
		}
	}
}

func TestNewTypeSystem(t *testing.T) {
	tests := map[string]struct {
		model  string
		output TypeSystem
	}{
		// test a direct relationship between two types
		"basic_example": {
			model: `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: [user]`,
			output: TypeSystem{
				schemaVersion: SchemaVersion1_1,
				ttuRelations: map[string]map[string][]*openfgav1.TupleToUserset{
					"user": {},
					"document": {
						"viewer": []*openfgav1.TupleToUserset{},
					},
				},
				conditions: map[string]*condition.EvaluableCondition{},
				relations: map[string]map[string]*openfgav1.Relation{
					"document": {
						"viewer": {
							Name: "viewer",
							Rewrite: &openfgav1.Userset{
								Userset: &openfgav1.Userset_This{},
							},
							TypeInfo: &openfgav1.RelationTypeInfo{
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{
										Type: "user",
									},
								},
							},
						},
					},
					"user": {},
				},
				typeDefinitions: map[string]*openfgav1.TypeDefinition{
					"document": {
						Type: "document",
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
						Relations: map[string]*openfgav1.Userset{
							"viewer": {
								Userset: &openfgav1.Userset_This{},
							},
						},
					},
					"user": {
						Type:      "user",
						Relations: map[string]*openfgav1.Userset{},
					},
				},
			},
		},
		// test that three rewrites in an intersection are represented as
		// direct children of the intersection.
		"three_intersection_children": {
			model: `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: criteria1 and criteria2 and criteria3
						define criteria1: [user]
						define criteria2: [user]
						define criteria3: [user]`,
			output: TypeSystem{
				schemaVersion: SchemaVersion1_1,
				ttuRelations: map[string]map[string][]*openfgav1.TupleToUserset{
					"user": {},
					"document": {
						"viewer":    []*openfgav1.TupleToUserset{},
						"criteria1": []*openfgav1.TupleToUserset{},
						"criteria2": []*openfgav1.TupleToUserset{},
						"criteria3": []*openfgav1.TupleToUserset{},
					},
				},
				conditions: map[string]*condition.EvaluableCondition{},
				relations: map[string]map[string]*openfgav1.Relation{
					"document": {
						"viewer": {
							Name: "viewer",
							Rewrite: &openfgav1.Userset{
								Userset: &openfgav1.Userset_Intersection{
									Intersection: &openfgav1.Usersets{
										Child: []*openfgav1.Userset{
											{
												Userset: &openfgav1.Userset_ComputedUserset{
													ComputedUserset: &openfgav1.ObjectRelation{
														Relation: "criteria1",
													},
												},
											},
											{
												Userset: &openfgav1.Userset_ComputedUserset{
													ComputedUserset: &openfgav1.ObjectRelation{
														Relation: "criteria2",
													},
												},
											},
											{
												Userset: &openfgav1.Userset_ComputedUserset{
													ComputedUserset: &openfgav1.ObjectRelation{
														Relation: "criteria3",
													},
												},
											},
										},
									},
								},
							},
							TypeInfo: &openfgav1.RelationTypeInfo{
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{},
							},
						},
						"criteria1": {
							Name: "criteria1",
							Rewrite: &openfgav1.Userset{
								Userset: &openfgav1.Userset_This{},
							},
							TypeInfo: &openfgav1.RelationTypeInfo{
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{
										Type: "user",
									},
								},
							},
						},
						"criteria2": {
							Name: "criteria2",
							Rewrite: &openfgav1.Userset{
								Userset: &openfgav1.Userset_This{},
							},
							TypeInfo: &openfgav1.RelationTypeInfo{
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{
										Type: "user",
									},
								},
							},
						},
						"criteria3": {
							Name: "criteria3",
							Rewrite: &openfgav1.Userset{
								Userset: &openfgav1.Userset_This{},
							},
							TypeInfo: &openfgav1.RelationTypeInfo{
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{
										Type: "user",
									},
								},
							},
						},
					},
					"user": {},
				},
				typeDefinitions: map[string]*openfgav1.TypeDefinition{
					"document": {
						Type: "document",
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{},
								},
								"criteria1": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
								"criteria2": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
								"criteria3": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
						Relations: map[string]*openfgav1.Userset{
							"criteria1": {
								Userset: &openfgav1.Userset_This{},
							},
							"criteria2": {
								Userset: &openfgav1.Userset_This{},
							},
							"criteria3": {
								Userset: &openfgav1.Userset_This{},
							},
							"viewer": {
								Userset: &openfgav1.Userset_Intersection{
									Intersection: &openfgav1.Usersets{
										Child: []*openfgav1.Userset{
											{
												Userset: &openfgav1.Userset_ComputedUserset{
													ComputedUserset: &openfgav1.ObjectRelation{
														Relation: "criteria1",
													},
												},
											},
											{
												Userset: &openfgav1.Userset_Intersection{
													Intersection: &openfgav1.Usersets{
														Child: []*openfgav1.Userset{
															{
																Userset: &openfgav1.Userset_ComputedUserset{
																	ComputedUserset: &openfgav1.ObjectRelation{
																		Relation: "criteria2",
																	},
																},
															},
															{
																Userset: &openfgav1.Userset_ComputedUserset{
																	ComputedUserset: &openfgav1.ObjectRelation{
																		Relation: "criteria3",
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
					},
					"user": {
						Type:      "user",
						Relations: map[string]*openfgav1.Userset{},
					},
				},
			},
		},
		// test that parenthesis around an intersection will make that
		// intersection a child of the intersection outside the parenthesis.
		"nested_intersection_using_parenthesis": {
			model: `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: criteria1 and (criteria2 and criteria3)
						define criteria1: [user]
						define criteria2: [user]
						define criteria3: [user]`,
			output: TypeSystem{
				schemaVersion: SchemaVersion1_1,
				ttuRelations: map[string]map[string][]*openfgav1.TupleToUserset{
					"user": {},
					"document": {
						"viewer":    []*openfgav1.TupleToUserset{},
						"criteria1": []*openfgav1.TupleToUserset{},
						"criteria2": []*openfgav1.TupleToUserset{},
						"criteria3": []*openfgav1.TupleToUserset{},
					},
				},
				conditions: map[string]*condition.EvaluableCondition{},
				relations: map[string]map[string]*openfgav1.Relation{
					"document": {
						"viewer": {
							Name: "viewer",
							Rewrite: &openfgav1.Userset{
								Userset: &openfgav1.Userset_Intersection{
									Intersection: &openfgav1.Usersets{
										Child: []*openfgav1.Userset{
											{
												Userset: &openfgav1.Userset_ComputedUserset{
													ComputedUserset: &openfgav1.ObjectRelation{
														Relation: "criteria1",
													},
												},
											},
											{
												Userset: &openfgav1.Userset_Intersection{
													Intersection: &openfgav1.Usersets{
														Child: []*openfgav1.Userset{
															{
																Userset: &openfgav1.Userset_ComputedUserset{
																	ComputedUserset: &openfgav1.ObjectRelation{
																		Relation: "criteria2",
																	},
																},
															},
															{
																Userset: &openfgav1.Userset_ComputedUserset{
																	ComputedUserset: &openfgav1.ObjectRelation{
																		Relation: "criteria3",
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
							TypeInfo: &openfgav1.RelationTypeInfo{
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{},
							},
						},
						"criteria1": {
							Name: "criteria1",
							Rewrite: &openfgav1.Userset{
								Userset: &openfgav1.Userset_This{},
							},
							TypeInfo: &openfgav1.RelationTypeInfo{
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{
										Type: "user",
									},
								},
							},
						},
						"criteria2": {
							Name: "criteria2",
							Rewrite: &openfgav1.Userset{
								Userset: &openfgav1.Userset_This{},
							},
							TypeInfo: &openfgav1.RelationTypeInfo{
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{
										Type: "user",
									},
								},
							},
						},
						"criteria3": {
							Name: "criteria3",
							Rewrite: &openfgav1.Userset{
								Userset: &openfgav1.Userset_This{},
							},
							TypeInfo: &openfgav1.RelationTypeInfo{
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{
										Type: "user",
									},
								},
							},
						},
					},
					"user": {},
				},
				typeDefinitions: map[string]*openfgav1.TypeDefinition{
					"document": {
						Type: "document",
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{},
								},
								"criteria1": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
								"criteria2": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
								"criteria3": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
						Relations: map[string]*openfgav1.Userset{
							"criteria1": {
								Userset: &openfgav1.Userset_This{},
							},
							"criteria2": {
								Userset: &openfgav1.Userset_This{},
							},
							"criteria3": {
								Userset: &openfgav1.Userset_This{},
							},
							"viewer": {
								Userset: &openfgav1.Userset_Intersection{
									Intersection: &openfgav1.Usersets{
										Child: []*openfgav1.Userset{
											{
												Userset: &openfgav1.Userset_ComputedUserset{
													ComputedUserset: &openfgav1.ObjectRelation{
														Relation: "criteria1",
													},
												},
											},
											{
												Userset: &openfgav1.Userset_Intersection{
													Intersection: &openfgav1.Usersets{
														Child: []*openfgav1.Userset{
															{
																Userset: &openfgav1.Userset_ComputedUserset{
																	ComputedUserset: &openfgav1.ObjectRelation{
																		Relation: "criteria2",
																	},
																},
															},
															{
																Userset: &openfgav1.Userset_ComputedUserset{
																	ComputedUserset: &openfgav1.ObjectRelation{
																		Relation: "criteria3",
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
					},
					"user": {
						Type:      "user",
						Relations: map[string]*openfgav1.Userset{},
					},
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(tc.model)
			ts := New(model)
			tc.output.modelID = ts.modelID
			typeSystemEquals(t, &tc.output, ts)
		})
	}
}
