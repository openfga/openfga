package typesystem

import (
	"encoding/json"
	"fmt"
	"log"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

// map[objectType]map[relation]map[subjectType]terminalRelation.
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

func (t *TypeSystem) AreTypesConnectedViaRelations(objectType, relation, subjectType string) (bool, []string) {
	terminalRelations, areConnected := t.connectedTypes[objectType][relation][subjectType]
	if !areConnected {
		return false, []string{}
	}

	return true, terminalRelations
}

func TMPPrettyPrint(data interface{}) {
	// Marshal the map to a pretty-printed JSON string
	jsonData, err := json.MarshalIndent(data, "", "    ")
	if err != nil {
		log.Fatalf("Error pretty-printing map: %v", err)
	}
	fmt.Println(string(jsonData))
}

// validateRelation applies all the validation rules to a relation definition in a model. A relation
// must meet all the rewrite validation, type restriction validation, and entrypoint validation criteria
// for it to be valid. Otherwise, an error is returned.
func (t *TypeSystem) AssignTerminalTypes(typeName, relationName string, relationMap map[string]*openfgav1.Userset) {
	rewrite := relationMap[relationName]
	terminalTypesAndRelations := t.getTerminalSubjectTypesAndRelation(typeName, relationName, rewrite)

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

func (t *TypeSystem) getTerminalSubjectTypesAndRelation(
	typeName string,
	relationName string,
	rewrite *openfgav1.Userset,
) []terminalTypesAndRelation {
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
			assignableTypes = append(assignableTypes, assignableType.GetType())
			terminalRelation = relationName
		}

		return []terminalTypesAndRelation{
			{
				terminalTypes:    assignableTypes,
				terminalRelation: terminalRelation,
			},
		}
	case *openfgav1.Userset_ComputedUserset:
		computedRelationName := rw.ComputedUserset.GetRelation()
		computedRelation := t.relations[typeName][computedRelationName]
		return t.getTerminalSubjectTypesAndRelation(typeName, computedRelationName, computedRelation.GetRewrite())
	case *openfgav1.Userset_TupleToUserset:
		tuplesetRelationName := rw.TupleToUserset.GetTupleset().GetRelation()
		computedRelationName := rw.TupleToUserset.GetComputedUserset().GetRelation()

		tuplesetRelation, ok := t.relations[typeName][tuplesetRelationName]
		if !ok {
			return []terminalTypesAndRelation{}
		}

		for _, assignableType := range tuplesetRelation.GetTypeInfo().GetDirectlyRelatedUserTypes() {
			assignableTypeName := assignableType.GetType()

			if assignableRelation, ok := t.relations[assignableTypeName][computedRelationName]; ok {
				return t.getTerminalSubjectTypesAndRelation(assignableTypeName, computedRelationName, assignableRelation.GetRewrite())
			}
		}

	case *openfgav1.Userset_Union:
		assignableTypesForRelation := []terminalTypesAndRelation{}

		for _, child := range rw.Union.GetChild() {
			tt := t.getTerminalSubjectTypesAndRelation(typeName, relationName, child)
			assignableTypesForRelation = append(assignableTypesForRelation, tt...)
		}

		return assignableTypesForRelation

	case *openfgav1.Userset_Intersection:
		intersectionAssignableTypesAndRelations := []terminalTypesAndRelation{}

		for _, child := range rw.Intersection.GetChild() {
			_ = t.getTerminalSubjectTypesAndRelation(typeName, relationName, child)
		}

		return intersectionAssignableTypesAndRelations
	}

	// case *openfgav1.Userset_Difference:
	// 	// All the children must have an entrypoint.
	// 	baseTypes, baseTerminalRelation := t.getTerminalSubjectTypesAndRelation(typeName, relationName, rw.Difference.GetBase())
	// 	subTypes, subTerminalRelation := t.getTerminalSubjectTypesAndRelation(typeName, relationName, rw.Difference.GetSubtract())

	// 	return
	// }

	panic("unexpected userset rewrite encountered")
}
