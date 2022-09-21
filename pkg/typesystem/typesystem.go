package typesystem

import (
	"fmt"

	"github.com/go-errors/errors"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type SchemaVersion int

const (
	SchemaVersionUnspecified SchemaVersion = 0
	SchemaVersion1_0         SchemaVersion = 1
	SchemaVersion1_1         SchemaVersion = 2
)

var (
	ErrInvalidSchemaVersion = errors.New("invalid schema version")
)

func NewSchemaVersion(s string) (SchemaVersion, error) {
	switch s {
	case "", "1.0":
		return SchemaVersion1_0, nil
	case "1.1":
		return SchemaVersion1_1, nil
	default:
		return SchemaVersionUnspecified, ErrInvalidSchemaVersion
	}
}

func (v SchemaVersion) String() string {
	switch v {
	case SchemaVersion1_0:
		return "1.0"
	case SchemaVersion1_1:
		return "1.1"
	default:
		return "unspecified"
	}
}

type TypeSystem struct {
	TypeDefinitions map[string]*openfgapb.TypeDefinition
}

func NewTypeSystem(typeDefinitions []*openfgapb.TypeDefinition) *TypeSystem {
	tds := map[string]*openfgapb.TypeDefinition{}
	for _, td := range typeDefinitions {
		tds[td.GetType()] = td
	}

	return &TypeSystem{
		TypeDefinitions: tds,
	}
}

// TODO: In a future PR refactor WriteAuthorizationModel to take a map[string]*openfgapb.TypeDefinition
// and get rid of this method
func (t *TypeSystem) GetTypeDefinitions() []*openfgapb.TypeDefinition {
	tds := make([]*openfgapb.TypeDefinition, 0, len(t.TypeDefinitions))
	for _, td := range t.TypeDefinitions {
		tds = append(tds, td)
	}

	return tds
}

func (t *TypeSystem) GetRelations(objectType string) (map[string]*openfgapb.Relation, error) {
	td, ok := t.TypeDefinitions[objectType]
	if !ok {
		return nil, ObjectTypeDoesNotExistError(objectType)
	}

	relations := map[string]*openfgapb.Relation{}

	for relation, rewrite := range td.GetRelations() {
		r := &openfgapb.Relation{
			Rewrite: rewrite,
		}

		if metadata, ok := td.GetMetadata().GetRelations()[relation]; ok {
			r.DirectlyRelatedUserTypes = metadata.GetDirectlyRelatedUserTypes()
		}

		relations[relation] = r
	}

	return relations, nil
}

func (t *TypeSystem) GetRelation(objectType, relation string) (*openfgapb.Relation, error) {
	relations, err := t.GetRelations(objectType)
	if err != nil {
		return nil, err
	}

	r, ok := relations[relation]
	if !ok {
		return nil, RelationDoesNotExistError(objectType, relation)
	}

	return r, nil
}

// ValidateRelationRewrites validates the type system according to the following rules:
//  1. For every rewrite the relations in the rewrite must:
//     a. Be valid relations on the same type in the authorization typeSystem (in cases of computedUserset)
//     b. Be valid relations on another existing type (in cases of tupleToUserset)
//  2. Do not allow duplicate types or duplicate relations (but that is inherent in the map structure so nothing to
//     actually check)
func (t *TypeSystem) ValidateRelationRewrites() error {
	if err := t.areUsersetRewritesValid(); err != nil {
		return err
	}

	return nil
}

func (t *TypeSystem) areUsersetRewritesValid() error {
	allRelations := map[string]struct{}{}
	typeToRelations := map[string]map[string]struct{}{}
	for objectType, td := range t.TypeDefinitions {
		typeToRelations[objectType] = map[string]struct{}{}
		for relation := range td.GetRelations() {
			typeToRelations[objectType][relation] = struct{}{}
			allRelations[relation] = struct{}{}
		}
	}

	for objectType, td := range t.TypeDefinitions {
		for relation, rewrite := range td.GetRelations() {
			err := isUsersetRewriteValid(allRelations, typeToRelations[objectType], objectType, relation, rewrite)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// isUsersetRewriteValid checks if a particular userset rewrite is valid. The first argument is all the relations in
// the typeSystem, the second argument is the subset of relations on the type where the rewrite occurs.
func isUsersetRewriteValid(allRelations map[string]struct{}, relationsOnType map[string]struct{}, objectType, relation string, rewrite *openfgapb.Userset) error {
	if rewrite.GetUserset() == nil {
		return InvalidRelationError(objectType, relation)
	}

	switch t := rewrite.GetUserset().(type) {
	case *openfgapb.Userset_ComputedUserset:
		computedUserset := t.ComputedUserset.GetRelation()
		if computedUserset == relation {
			return InvalidRelationError(objectType, relation)
		}
		if _, ok := relationsOnType[computedUserset]; !ok {
			return RelationDoesNotExistError(objectType, computedUserset)
		}
	case *openfgapb.Userset_TupleToUserset:
		tupleset := t.TupleToUserset.GetTupleset().GetRelation()
		if _, ok := relationsOnType[tupleset]; !ok {
			return RelationDoesNotExistError(objectType, tupleset)
		}

		computedUserset := t.TupleToUserset.GetComputedUserset().GetRelation()
		if _, ok := allRelations[computedUserset]; !ok {
			return RelationDoesNotExistError("", computedUserset)
		}
	case *openfgapb.Userset_Union:
		for _, child := range t.Union.GetChild() {
			err := isUsersetRewriteValid(allRelations, relationsOnType, objectType, relation, child)
			if err != nil {
				return err
			}
		}
	case *openfgapb.Userset_Intersection:
		for _, child := range t.Intersection.GetChild() {
			err := isUsersetRewriteValid(allRelations, relationsOnType, objectType, relation, child)
			if err != nil {
				return err
			}
		}
	case *openfgapb.Userset_Difference:
		err := isUsersetRewriteValid(allRelations, relationsOnType, objectType, relation, t.Difference.Base)
		if err != nil {
			return err
		}

		err = isUsersetRewriteValid(allRelations, relationsOnType, objectType, relation, t.Difference.Subtract)
		if err != nil {
			return err
		}
	}

	return nil
}

// ValidateRelationTypeRestrictions should only be called on SchemaVersion1_1 type systems.
// If it is a SchemaVersion1_1 type system (with types on relations), it will validate the type system according to the
// following rules:
//  1. Every type on a relation must be a valid type:
//     a. For a type (e.g. user) this means checking that this type is in the TypeSystem
//     b. For a type#relation this means checking that this type with this relation is in the TypeSystem
//  2. Check that a relation is assignable if and only if it has a non-zero list of types
func (t *TypeSystem) ValidateRelationTypeRestrictions() error {
	for objectType := range t.TypeDefinitions {
		relations, err := t.GetRelations(objectType)
		if err != nil {
			return err
		}

		for name, relation := range relations {
			relatedTypes := relation.GetDirectlyRelatedUserTypes()

			assignable := isAssignable(relation.GetRewrite())
			if assignable && len(relatedTypes) == 0 {
				return AssignableRelationError(objectType, name)
			}

			if !assignable && len(relatedTypes) != 0 {
				return NonAssignableRelationError(objectType, name)
			}

			for _, related := range relatedTypes {
				relatedObjectType := related.GetType()
				relatedRelation := related.GetRelation()

				if _, err := t.GetRelations(relatedObjectType); err != nil {
					return InvalidRelationTypeError(objectType, name, relatedObjectType, relatedRelation)
				}

				if relatedRelation != "" {
					if _, err := t.GetRelation(relatedObjectType, relatedRelation); err != nil {
						return InvalidRelationTypeError(objectType, name, relatedObjectType, relatedRelation)
					}
				}
			}
		}
	}

	return nil
}

func isAssignable(rewrite *openfgapb.Userset) bool {
	switch rw := rewrite.GetUserset().(type) {
	case *openfgapb.Userset_This:
		return true
	case *openfgapb.Userset_Union:
		for _, child := range rw.Union.GetChild() {
			if isAssignable(child) {
				return true
			}
		}
	case *openfgapb.Userset_Intersection:
		for _, child := range rw.Intersection.GetChild() {
			if isAssignable(child) {
				return true
			}
		}
	case *openfgapb.Userset_Difference:
		difference := rw.Difference
		if isAssignable(difference.GetBase()) || isAssignable(difference.GetSubtract()) {
			return true
		}
	}

	return false
}

func InvalidRelationError(objectType, relation string) error {
	return errors.Errorf("the definition of relation '%s' in object type '%s' is invalid", relation, objectType)
}

func ObjectTypeDoesNotExistError(objectType string) error {
	return errors.Errorf("object type '%s' does not exist", objectType)
}

// RelationDoesNotExistError may have an empty objectType, but must have a relation
// (otherwise the error won't make much sense).
func RelationDoesNotExistError(objectType, relation string) error {
	msg := fmt.Sprintf("relation '%s'", relation)
	if objectType != "" {
		msg = fmt.Sprintf("%s in object type '%s'", msg, objectType)
	}
	return errors.Errorf("%s does not exist", msg)
}

func AssignableRelationError(objectType, relation string) error {
	return errors.Errorf("the assignable relation '%s' in object type '%s' must contain at least one relation type", relation, objectType)
}

func NonAssignableRelationError(objectType, relation string) error {
	return errors.Errorf("the non-assignable relation '%s' in object type '%s' should not contain a relation type", objectType, relation)
}

func InvalidRelationTypeError(objectType, relation, relatedObjectType, relatedRelation string) error {
	relationType := relatedObjectType
	if relatedRelation != "" {
		relationType = fmt.Sprintf("%s#%s", relatedObjectType, relatedRelation)
	}

	return errors.Errorf("the relation type '%s' on '%s' in object type '%s' is not valid", relationType, relation, objectType)
}
