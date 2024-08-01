package typesystem

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
)

// map[_objectType_]map[_relation_]map[_subjectType_] terminalRelation.
type TypesystemConnectedTypes map[string]map[string]map[string][]string

func (f TypesystemConnectedTypes) assign(objectType string, relation string, subjectType string, terminalRelation string) {
	if f[objectType] == nil {
		f[objectType] = make(map[string]map[string][]string)
	}

	if f[objectType][relation] == nil {
		f[objectType][relation] = make(map[string][]string)
	}

	for _, v := range f[objectType][relation][subjectType] {
		if v == terminalRelation {
			return // already exists
		}
	}

	f[objectType][relation][subjectType] = append(f[objectType][relation][subjectType], terminalRelation)
}

// AssignTerminalTypes will populate the `connectedTypes` property on the typesystem to indicate for a given
// object type what the terminal user types and relations are. This is useful for quickly determining if two types
// are connected via a relation and also for determining if the TTU "fast path" optimization can be applied.
func (t *TypeSystem) AssignTerminalTypes(typeName, relationName string) {
	terminalTypesAndRelations := t.getTerminalUserTypeAndRelationsForConnectedTypes(typeName, relationName, 0)
	for _, terminalTypesAndRelation := range terminalTypesAndRelations {
		for _, terminalType := range terminalTypesAndRelation.terminalTypes {
			t.connectedTypes.assign(typeName, relationName, terminalType, terminalTypesAndRelation.terminalRelation)
		}
	}
}

type terminalTypesAndRelation struct {
	terminalTypes    []string
	terminalRelation string
}

func (t *TypeSystem) getTerminalUserTypeAndRelationsForConnectedTypes(
	typeName string,
	relationName string,
	numTTU int,
) []terminalTypesAndRelation {
	rewrite := t.typeDefinitions[typeName].GetRelations()[relationName]

	cache, ok := t.connectedTypes[typeName][relationName]
	if ok {
		terminalTypesAndRelations := []terminalTypesAndRelation{}

		for subjectType, terminalRelations := range cache {
			for _, terminalRelation := range terminalRelations {
				terminalTypesAndRelations = append(terminalTypesAndRelations, terminalTypesAndRelation{
					terminalTypes:    []string{subjectType},
					terminalRelation: terminalRelation,
				})
			}
		}

		return terminalTypesAndRelations
	}

	switch rw := rewrite.GetUserset().(type) {
	case *openfgav1.Userset_This:
		assignableTypes := []string{}

		thisRelation, ok := t.relations[typeName][relationName]
		if !ok {
			return []terminalTypesAndRelation{}
		}

		for _, assignableType := range thisRelation.GetTypeInfo().GetDirectlyRelatedUserTypes() {
			if assignableType.GetRelation() != "" {
				return []terminalTypesAndRelation{} // Usersets not yet supported
			}

			t := assignableType.GetType()
			if assignableType.GetWildcard() != nil {
				t = tuple.TypedPublicWildcard(assignableType.GetType())
			}
			assignableTypes = append(assignableTypes, t)
		}

		return []terminalTypesAndRelation{
			{
				terminalTypes:    assignableTypes,
				terminalRelation: relationName,
			},
		}
	case *openfgav1.Userset_ComputedUserset:
		return t.getTerminalUserTypeAndRelationsForConnectedTypes(typeName, rw.ComputedUserset.GetRelation(), numTTU)
	case *openfgav1.Userset_TupleToUserset:
		if numTTU > 0 {
			// Ensures that no chained TTU rewrites are eligible for fast-path TTU
			return []terminalTypesAndRelation{}
		}

		tuplesetRelationName := rw.TupleToUserset.GetTupleset().GetRelation()
		computedRelationName := rw.TupleToUserset.GetComputedUserset().GetRelation()

		tuplesetRelation, ok := t.relations[typeName][tuplesetRelationName]
		if !ok {
			return []terminalTypesAndRelation{}
		}

		for _, assignableType := range tuplesetRelation.GetTypeInfo().GetDirectlyRelatedUserTypes() {
			assignableTypeName := assignableType.GetType()
			if _, ok := t.relations[assignableTypeName][computedRelationName]; ok {
				return t.getTerminalUserTypeAndRelationsForConnectedTypes(assignableTypeName, computedRelationName, numTTU+1)
			}
		}
	case *openfgav1.Userset_Intersection, *openfgav1.Userset_Difference, *openfgav1.Userset_Union:
		return []terminalTypesAndRelation{}
	}

	return []terminalTypesAndRelation{}
}
