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

	f[objectType][relation][subjectType] = append(f[objectType][relation][subjectType], terminalRelation)
}

// AssignTerminalTypes will populate the `connectedTypes` property on the typesystem to indicate for a given object type
// what the terminal user types and relations.
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

	relation, ok := t.relations[typeName][relationName]
	if !ok {
		return []terminalTypesAndRelation{}
	}

	switch rw := rewrite.GetUserset().(type) {
	case *openfgav1.Userset_This:
		assignableTypes := []string{}
		var terminalRelation string

		for _, assignableType := range relation.GetTypeInfo().GetDirectlyRelatedUserTypes() {
			if assignableType.GetRelation() != "" {
				// Usersets not yet supported
				return []terminalTypesAndRelation{}
			}

			t := assignableType.GetType()
			if assignableType.GetWildcard() != nil {
				t = tuple.TypedPublicWildcard(assignableType.GetType())
			}
			assignableTypes = append(assignableTypes, t)

			terminalRelation = relationName
		}

		return []terminalTypesAndRelation{
			{
				terminalTypes:    assignableTypes,
				terminalRelation: terminalRelation,
			},
		}
	case *openfgav1.Userset_ComputedUserset:
		return t.getTerminalUserTypeAndRelationsForConnectedTypes(typeName, rw.ComputedUserset.GetRelation(), numTTU)
	case *openfgav1.Userset_TupleToUserset:
		if numTTU > 0 {
			return []terminalTypesAndRelation{}
		}

		tuplesetRelationName := rw.TupleToUserset.GetTupleset().GetRelation()
		computedRelationName := rw.TupleToUserset.GetComputedUserset().GetRelation()

		tuplesetRelation, ok := t.relations[typeName][tuplesetRelationName]
		if !ok {
			return []terminalTypesAndRelation{}
		}

		for _, assignableType := range tuplesetRelation.GetTypeInfo().GetDirectlyRelatedUserTypes() {
			// if assignableType.GetCondition() != "" {
			// 	// Conditions not yet supported
			// 	return []terminalTypesAndRelation{}
			// }

			assignableTypeName := assignableType.GetType()

			if _, ok := t.relations[assignableTypeName][computedRelationName]; ok {
				return t.getTerminalUserTypeAndRelationsForConnectedTypes(assignableTypeName, computedRelationName, numTTU+1)
			}
		}
	}

	return []terminalTypesAndRelation{}
}
