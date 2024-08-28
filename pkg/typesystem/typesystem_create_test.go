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
	childA := a.GetChild()
	childB := b.GetChild()
	require.Equal(t, len(childA), len(childB))
	for i := 0; i < len(childA); i++ {
		rewriteEquals(t, childA[i], childB[i])
	}
}

func tupleToUsersetEquals(t *testing.T, a, b *openfgav1.TupleToUserset) {
	if a == nil || b == nil {
		require.Equal(t, a, b)
		return
	}
	cuA := a.GetComputedUserset()
	cuB := b.GetComputedUserset()

	if cuA == nil || cuB == nil {
		require.Equal(t, cuA, cuB)
		return
	}
	objectRelationEquals(t, cuA, cuB)
}

func objectRelationEquals(t *testing.T, a, b *openfgav1.ObjectRelation) {
	if a == nil || b == nil {
		require.Equal(t, a, b)
		return
	}
	require.Equal(t, a.GetRelation(), b.GetRelation())
	require.Equal(t, a.GetObject(), b.GetObject())
}

func rewriteEquals(t *testing.T, a, b *openfgav1.Userset) {
	if a == nil || b == nil {
		require.Equal(t, a, b)
		return
	}

	usersetA := a.GetUserset()
	usersetB := b.GetUserset()

	_, okA := usersetA.(*openfgav1.Userset_This)
	_, okB := usersetB.(*openfgav1.Userset_This)
	require.Equal(t, okA, okB, "one is Userset_This")

	cuA, okA := usersetA.(*openfgav1.Userset_ComputedUserset)
	cuB, okB := usersetB.(*openfgav1.Userset_ComputedUserset)
	require.Equal(t, okA, okB, "one is Userset_ComputedUserset")
	if cuA == nil || cuB == nil {
		require.Equal(t, cuA, cuB)
		return
	}
	objectRelationEquals(t, cuA.ComputedUserset, cuB.ComputedUserset)

	ttuA, okA := usersetA.(*openfgav1.Userset_TupleToUserset)
	ttuB, okB := usersetB.(*openfgav1.Userset_TupleToUserset)
	require.Equal(t, okA, okB, "one is Userset_TupleToUserset")
	if ttuA == nil || ttuB == nil {
		require.Equal(t, ttuA, ttuB)
		return
	}
	tupleToUsersetEquals(t, ttuA.TupleToUserset, ttuB.TupleToUserset)

	uA, okA := usersetA.(*openfgav1.Userset_Union)
	uB, okB := usersetB.(*openfgav1.Userset_Union)
	require.Equal(t, okA, okB, "one is Userset_Union")
	if uA == nil || uB == nil {
		require.Equal(t, uA, uB)
		return
	}
	usersetsEquals(t, uA.Union, uB.Union)

	iA, okA := usersetA.(*openfgav1.Userset_Intersection)
	iB, okB := usersetB.(*openfgav1.Userset_Intersection)
	require.Equal(t, okA, okB, "one is Userset_Intersection")
	if iA == nil || iB == nil {
		require.Equal(t, iA, iB)
		return
	}
	usersetsEquals(t, iA.Intersection, iB.Intersection)

	dA, okA := usersetA.(*openfgav1.Userset_Difference)
	dB, okB := usersetB.(*openfgav1.Userset_Difference)
	require.Equal(t, okA, okB, "one is Userset_Difference")
	if dA == nil || dB == nil {
		require.Equal(t, dA, dB)
		return
	}
	if dA.Difference == nil || dB.Difference == nil {
		require.Equal(t, dA.Difference, dB.Difference)
		return
	}
	rewriteEquals(t, dA.Difference.GetBase(), dB.Difference.GetBase())
	rewriteEquals(t, dA.Difference.GetSubtract(), dB.Difference.GetSubtract())
}

func relationReferenceEquals(t *testing.T, a, b *openfgav1.RelationReference) {
	if a == nil || b == nil {
		require.Equal(t, a, b)
		return
	}
	require.Equal(t, a.GetCondition(), b.GetCondition())
	require.Equal(t, a.GetType(), b.GetType())

	rwA := a.GetRelationOrWildcard()
	rwB := b.GetRelationOrWildcard()
	if rwA == nil || rwB == nil {
		require.Equal(t, rwA, rwB)
		return
	}
	wA, okA := rwA.(*openfgav1.RelationReference_Wildcard)
	wB, okB := rwB.(*openfgav1.RelationReference_Wildcard)
	require.Equal(t, okA, okB)
	if wA == nil || wB == nil {
		require.Equal(t, wA, wB)
		return
	}
	require.Equal(t, wA.Wildcard, wB.Wildcard)

	rA, okA := rwA.(*openfgav1.RelationReference_Relation)
	rB, okB := rwB.(*openfgav1.RelationReference_Relation)
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
	directA := a.GetDirectlyRelatedUserTypes()
	directB := b.GetDirectlyRelatedUserTypes()

	if directA == nil || directB == nil {
		require.Equal(t, directA, directB)
		return
	}
	require.Equal(t, len(directA), len(directB))
	for i := 0; i < len(directA); i++ {
		relationReferenceEquals(t, directA[i], directB[i])
	}
}

func relationEquals(t *testing.T, a, b *openfgav1.Relation) {
	require.Equal(t, a.GetName(), b.GetName())
	rewriteEquals(t, a.GetRewrite(), b.GetRewrite())
	relationTypeInfoEquals(t, a.GetTypeInfo(), b.GetTypeInfo())
}

func sourceInfoEquals(t *testing.T, a, b *openfgav1.SourceInfo) {
	if a == nil || b == nil {
		require.Equal(t, a, b)
		return
	}
	require.Equal(t, a.GetFile(), b.GetFile())
}

func relationMetadataEquals(t *testing.T, a, b *openfgav1.RelationMetadata) {
	if a == nil || b == nil {
		require.Equal(t, a, b)
		return
	}
	require.Equal(t, a.GetModule(), b.GetModule())
	sourceInfoEquals(t, a.GetSourceInfo(), b.GetSourceInfo())

	directA := a.GetDirectlyRelatedUserTypes()
	directB := b.GetDirectlyRelatedUserTypes()
	if directA == nil || directB == nil {
		require.Equal(t, directA, directB)
		return
	}
	require.Equal(t, len(directA), len(directB))
	for i := 0; i < len(directA); i++ {
		relationReferenceEquals(t, directA[i], directB[i])
	}
}

func metadataEquals(t *testing.T, a, b *openfgav1.Metadata) {
	if a == nil || b == nil {
		require.Equal(t, a, b)
		return
	}
	require.Equal(t, a.GetModule(), b.GetModule())
	sourceInfoEquals(t, a.GetSourceInfo(), b.GetSourceInfo())

	relationA := a.GetRelations()
	relationB := b.GetRelations()
	if relationA == nil || relationB == nil {
		require.Equal(t, relationA, relationB)
		return
	}
	require.Equal(t, len(relationA), len(relationB))
	for nameA, rA := range relationA {
		rB, ok := relationB[nameA]
		require.True(t, ok, nameA)
		relationMetadataEquals(t, rA, rB)
	}
}

func typeDefinitionEquals(t *testing.T, a, b *openfgav1.TypeDefinition) {
	require.Equal(t, a.GetType(), b.GetType())

	relationsA := a.GetRelations()
	relationsB := b.GetRelations()
	require.Equal(t, len(relationsA), len(relationsB))

	for nameA, rA := range relationsA {
		rB, ok := relationsB[nameA]
		require.True(t, ok)
		rewriteEquals(t, rA, rB)
	}
	metadataEquals(t, a.GetMetadata(), b.GetMetadata())
}

func typeSystemEquals(t *testing.T, a, b *TypeSystem) {
	require.Equal(t, a.GetSchemaVersion(), b.GetSchemaVersion())
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
