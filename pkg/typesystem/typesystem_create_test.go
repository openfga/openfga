package typesystem

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/pkg/testutils"
)

// usersetsEquals tests that the two *openfgav1.Usersets
// values provided are equal. This is a deep equality check.
func usersetsEquals(t *testing.T, a, b *openfgav1.Usersets) {
	t.Helper()
	if a == nil && b == nil {
		return
	}
	require.NotNil(t, a)
	require.NotNil(t, b)

	childA, childB := a.GetChild(), b.GetChild()
	// nil case covered by len check
	require.Len(t, childB, len(childA))
	for ndx, v := range childA {
		rewriteEquals(t, v, childB[ndx])
	}
}

// tupleToUsersetEquals tests that the two *openfgav1.TupleToUserset
// values provided are equal. This is a deep equality check.
func tupleToUsersetEquals(t *testing.T, a, b *openfgav1.TupleToUserset) {
	t.Helper()
	if a == nil && b == nil {
		return
	}
	require.NotNil(t, a)
	require.NotNil(t, b)

	cuA, cuB := a.GetComputedUserset(), b.GetComputedUserset()
	objectRelationEquals(t, cuA, cuB)

	tsA, tsB := a.GetTupleset(), b.GetTupleset()
	objectRelationEquals(t, tsA, tsB)
}

// objectRelationEquals tests that the two *openfgav1.ObjectRelation
// values provided are equal. This is a deep equality check.
func objectRelationEquals(t *testing.T, a, b *openfgav1.ObjectRelation) {
	t.Helper()
	if a == nil && b == nil {
		return
	}
	require.NotNil(t, a)
	require.NotNil(t, b)

	assert.Equal(t, a.GetRelation(), b.GetRelation())
	assert.Equal(t, a.GetObject(), b.GetObject())
}

// rewriteEquals tests that the two *openfgav1.Userset
// values provided are equal. This is a deep equality check.
func rewriteEquals(t *testing.T, a, b *openfgav1.Userset) {
	t.Helper()
	if a == nil && b == nil {
		return
	}
	require.NotNil(t, a)
	require.NotNil(t, b)

	usersetA, usersetB := a.GetUserset(), b.GetUserset()
	if usersetA == nil && usersetB == nil {
		return
	}
	require.NotNil(t, usersetA)
	require.NotNil(t, usersetB)

	switch x := usersetA.(type) {
	case *openfgav1.Userset_This:
		_, ok := usersetB.(*openfgav1.Userset_This)
		require.True(t, ok, "expected Userset_This type")
	case *openfgav1.Userset_ComputedUserset:
		y, ok := usersetB.(*openfgav1.Userset_ComputedUserset)
		require.True(t, ok, "expected Userset_ComputedUserset type")
		objectRelationEquals(t, x.ComputedUserset, y.ComputedUserset)
	case *openfgav1.Userset_TupleToUserset:
		y, ok := usersetB.(*openfgav1.Userset_TupleToUserset)
		require.True(t, ok, "expected Userset_TupleToUserset type")
		tupleToUsersetEquals(t, x.TupleToUserset, y.TupleToUserset)
	case *openfgav1.Userset_Union:
		y, ok := usersetB.(*openfgav1.Userset_Union)
		require.True(t, ok, "expected Userset_Union type")
		usersetsEquals(t, x.Union, y.Union)
	case *openfgav1.Userset_Intersection:
		y, ok := usersetB.(*openfgav1.Userset_Intersection)
		require.True(t, ok, "expected Userset_Intersection type")
		usersetsEquals(t, x.Intersection, y.Intersection)
	case *openfgav1.Userset_Difference:
		y, ok := usersetB.(*openfgav1.Userset_Difference)
		require.True(t, ok, "expected Userset_Difference type")

		if x.Difference == nil && y.Difference == nil {
			return
		}
		require.NotNil(t, x.Difference)
		require.NotNil(t, y.Difference)

		rewriteEquals(t, x.Difference.GetBase(), y.Difference.GetBase())
		rewriteEquals(t, x.Difference.GetSubtract(), y.Difference.GetSubtract())
	default:
		// we should never arrive here in valid scenarios.
		t.Error("unexpected user set type")
	}
}

// relationReferenceEquals tests that the two *openfgav1.RelationReference
// values provided are equal. This is a deep equality check.
func relationReferenceEquals(t *testing.T, a, b *openfgav1.RelationReference) {
	t.Helper()
	if a == nil && b == nil {
		return
	}
	require.NotNil(t, a)
	require.NotNil(t, b)

	assert.Equal(t, a.GetCondition(), b.GetCondition())
	assert.Equal(t, a.GetType(), b.GetType())

	rwA, rwB := a.GetRelationOrWildcard(), b.GetRelationOrWildcard()

	if rwA == nil && rwB == nil {
		return
	}
	require.NotNil(t, rwA)
	require.NotNil(t, rwB)

	switch x := rwA.(type) {
	case *openfgav1.RelationReference_Wildcard:
		y, ok := rwB.(*openfgav1.RelationReference_Wildcard)
		require.True(t, ok, "expected RelationReference_Wildcard type")
		assert.Equal(t, x.Wildcard, y.Wildcard)
	case *openfgav1.RelationReference_Relation:
		y, ok := rwB.(*openfgav1.RelationReference_Relation)
		require.True(t, ok, "expected RelationReference_Relation type")
		assert.Equal(t, x.Relation, y.Relation)
	default:
		// we should never arrive here in valid scenarios.
		t.Error("unexpected relation type")
	}
}

// relationTypeInfoEquals tests that the two *openfgav1.RelationTypeInfo
// values provided are equal. This is a deep equality check.
func relationTypeInfoEquals(t *testing.T, a, b *openfgav1.RelationTypeInfo) {
	t.Helper()
	if a == nil && b == nil {
		return
	}
	require.NotNil(t, a)
	require.NotNil(t, b)

	directA, directB := a.GetDirectlyRelatedUserTypes(), b.GetDirectlyRelatedUserTypes()
	// nil case covered by len check
	require.Len(t, directB, len(directA))
	for ndx, v := range directA {
		relationReferenceEquals(t, v, directB[ndx])
	}
}

// relationEquals tests that the two *openfgav1.Relation
// values provided are equal. This is a deep equality check.
func relationEquals(t *testing.T, a, b *openfgav1.Relation) {
	t.Helper()
	if a == nil && b == nil {
		return
	}
	require.NotNil(t, a)
	require.NotNil(t, b)

	assert.Equal(t, a.GetName(), b.GetName())
	rewriteEquals(t, a.GetRewrite(), b.GetRewrite())
	relationTypeInfoEquals(t, a.GetTypeInfo(), b.GetTypeInfo())
}

// sourceInfoEquals tests that the two *openfgav1.SourceInfo
// values provided are equal. This is a deep equality check.
func sourceInfoEquals(t *testing.T, a, b *openfgav1.SourceInfo) {
	t.Helper()
	if a == nil && b == nil {
		return
	}
	require.NotNil(t, a)
	require.NotNil(t, b)

	assert.Equal(t, a.GetFile(), b.GetFile())
}

// relationMetadataEquals tests that the two *openfgav1.RelationMetadata
// values provided are equal. This is a deep equality check.
func relationMetadataEquals(t *testing.T, a, b *openfgav1.RelationMetadata) {
	t.Helper()
	if a == nil && b == nil {
		return
	}
	require.NotNil(t, a)
	require.NotNil(t, b)

	assert.Equal(t, a.GetModule(), b.GetModule())
	sourceInfoEquals(t, a.GetSourceInfo(), b.GetSourceInfo())

	directA, directB := a.GetDirectlyRelatedUserTypes(), b.GetDirectlyRelatedUserTypes()
	// nil case covered by len check
	require.Len(t, directB, len(directA))
	for ndx, v := range directA {
		relationReferenceEquals(t, v, directB[ndx])
	}
}

// metadataEquals tests that the two *openfgav1.Metadata
// values provided are equal. This is a deep equality check.
func metadataEquals(t *testing.T, a, b *openfgav1.Metadata) {
	t.Helper()
	if a == nil && b == nil {
		return
	}
	require.NotNil(t, a)
	require.NotNil(t, b)

	assert.Equal(t, a.GetModule(), b.GetModule())
	sourceInfoEquals(t, a.GetSourceInfo(), b.GetSourceInfo())

	relationA, relationB := a.GetRelations(), b.GetRelations()
	// nil case covered by len check
	require.Len(t, relationB, len(relationA))
	for nameA, rA := range relationA {
		rB, ok := relationB[nameA]
		require.True(t, ok, nameA)
		relationMetadataEquals(t, rA, rB)
	}
}

// typeDefinitionEquals tests that the two *openfgav1.TypeDefinition
// values provided are equal. This is a deep equality check.
func typeDefinitionEquals(t *testing.T, a, b *openfgav1.TypeDefinition) {
	t.Helper()
	if a == nil && b == nil {
		return
	}
	require.NotNil(t, a)
	require.NotNil(t, b)

	assert.Equal(t, a.GetType(), b.GetType())

	relationsA, relationsB := a.GetRelations(), b.GetRelations()
	// nil case covered by len check
	require.Len(t, relationsB, len(relationsA))
	for nameA, rA := range relationsA {
		rB, ok := relationsB[nameA]
		require.True(t, ok, nameA)
		rewriteEquals(t, rA, rB)
	}
	metadataEquals(t, a.GetMetadata(), b.GetMetadata())
}

// conditionParamTypeDefEquals tests that the two *openfgav1.ConditionParamTypeRef
// values provided are equal. This is a deep equality check.
func conditionParamTypeDefEquals(t *testing.T, a, b *openfgav1.ConditionParamTypeRef) {
	t.Helper()
	if a == nil && b == nil {
		return
	}
	require.NotNil(t, a)
	require.NotNil(t, b)

	assert.Equal(t, a.GetTypeName(), b.GetTypeName())

	paramsA, paramsB := a.GetGenericTypes(), b.GetGenericTypes()
	// nil case covered by len check
	require.Len(t, paramsB, len(paramsA))
	for ndx, pA := range paramsA {
		pB := paramsB[ndx]
		conditionParamTypeDefEquals(t, pA, pB)
	}
}

// conditionMetadataEquals tests that the two *openfgav1.ConditionMetadata
// values provided are equal. This is a deep equality check.
func conditionMetadataEquals(t *testing.T, a, b *openfgav1.ConditionMetadata) {
	t.Helper()
	if a == nil && b == nil {
		return
	}
	require.NotNil(t, a)
	require.NotNil(t, b)

	assert.Equal(t, a.GetModule(), b.GetModule())
	sourceInfoEquals(t, a.GetSourceInfo(), b.GetSourceInfo())
}

// conditionEquals tests that the two *openfgav1.Condition
// values provided are equal. This is a deep equality check.
func conditionEquals(t *testing.T, a, b *openfgav1.Condition) {
	t.Helper()
	if a == nil && b == nil {
		return
	}
	require.NotNil(t, a)
	require.NotNil(t, b)

	assert.Equal(t, a.GetName(), b.GetName())
	assert.Equal(t, a.GetExpression(), b.GetExpression())
	conditionMetadataEquals(t, a.GetMetadata(), b.GetMetadata())

	paramsA, paramsB := a.GetParameters(), b.GetParameters()
	// nil case covered by len check
	require.Len(t, paramsB, len(paramsA))
	for pName, pA := range paramsA {
		pB, ok := paramsB[pName]
		require.True(t, ok, pName)
		conditionParamTypeDefEquals(t, pA, pB)
	}
}

// evaluableConditionEquals tests that the two *condition.EvaluableCondition
// values provided are equal. This is a deep equality check.
func evaluableConditionEquals(t *testing.T, a, b *condition.EvaluableCondition) {
	t.Helper()
	if a == nil && b == nil {
		return
	}
	require.NotNil(t, a)
	require.NotNil(t, b)

	conditionEquals(t, a.Condition, b.Condition)
}

// typeSystemEquals tests that the two *TypeSystem
// values provided are equal. This is a deep equality check.
func typeSystemEquals(t *testing.T, a, b *TypeSystem) {
	t.Helper()
	if a == nil && b == nil {
		return
	}
	require.NotNil(t, a)
	require.NotNil(t, b)

	assert.Equal(t, a.GetSchemaVersion(), b.GetSchemaVersion())

	// nil case covered by len check
	require.Len(t, b.typeDefinitions, len(a.typeDefinitions))
	for nameA, tdA := range a.typeDefinitions {
		tdB, ok := b.typeDefinitions[nameA]
		require.True(t, ok, nameA)
		typeDefinitionEquals(t, tdA, tdB)
	}

	// nil case covered by len check
	require.Len(t, b.relations, len(a.relations))
	for typeNameA, relationsA := range a.relations {
		relationsB, ok := b.relations[typeNameA]
		require.True(t, ok)
		require.Len(t, relationsB, len(relationsA))
		for relationNameA, relationA := range relationsA {
			relationB, ok := relationsB[relationNameA]
			require.True(t, ok)
			relationEquals(t, relationA, relationB)
		}
	}

	// nil case covered by len check
	require.Len(t, b.ttuRelations, len(a.ttuRelations))
	for typeNameA, relationsA := range a.ttuRelations {
		relationsB, ok := b.ttuRelations[typeNameA]
		require.True(t, ok, typeNameA)

		// nil case covered by len check
		require.Len(t, relationsB, len(relationsA))
		for relationNameA, relationA := range relationsA {
			relationB, ok := relationsB[relationNameA]
			require.True(t, ok, relationNameA)

			// nil case covered by len check
			require.Len(t, relationB, len(relationA))
			for i := 0; i < len(relationA); i++ {
				tupleToUsersetEquals(t, relationA[i], relationB[i])
			}
		}
	}

	// nil case covered by len check
	require.Len(t, b.conditions, len(a.conditions))
	for conditionNameA, conditionA := range a.conditions {
		conditionB, ok := b.conditions[conditionNameA]
		require.True(t, ok, conditionNameA)
		evaluableConditionEquals(t, conditionA, conditionB)
	}
}

func TestNewTypeSystem(t *testing.T) {
	tests := map[string]struct {
		model  string
		output *TypeSystem
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
			output: &TypeSystem{
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
			output: &TypeSystem{
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
			ts, err := New(model)
			require.NoError(t, err)
			tc.output.modelID = ts.modelID
			typeSystemEquals(t, tc.output, ts)
		})
	}
}
