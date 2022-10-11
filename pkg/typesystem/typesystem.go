package typesystem

import (
	"fmt"

	"github.com/go-errors/errors"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

const (
	SchemaVersion1_0 = "1.0"
	SchemaVersion1_1 = "1.1"
)

var (
	ErrDuplicateTypes       = errors.New("an authorization model cannot contain duplicate types")
	ErrInvalidSchemaVersion = errors.New("invalid schema version")
)

func RelationReference(objectType, relation string) *openfgapb.RelationReference {
	return &openfgapb.RelationReference{
		Type:     objectType,
		Relation: relation,
	}
}

func This() *openfgapb.Userset {
	return &openfgapb.Userset{
		Userset: &openfgapb.Userset_This{},
	}
}

func ComputedUserset(relation string) *openfgapb.Userset {
	return &openfgapb.Userset{
		Userset: &openfgapb.Userset_ComputedUserset{
			ComputedUserset: &openfgapb.ObjectRelation{
				Relation: relation,
			},
		},
	}
}

func TupleToUserset(tuplesetRelation, targetRelation string) *openfgapb.Userset {
	return &openfgapb.Userset{
		Userset: &openfgapb.Userset_TupleToUserset{
			TupleToUserset: &openfgapb.TupleToUserset{
				Tupleset: &openfgapb.ObjectRelation{
					Relation: tuplesetRelation,
				},
				ComputedUserset: &openfgapb.ObjectRelation{
					Relation: targetRelation,
				},
			},
		},
	}
}

func Union(children ...*openfgapb.Userset) *openfgapb.Userset {
	return &openfgapb.Userset{
		Userset: &openfgapb.Userset_Union{
			Union: &openfgapb.Usersets{
				Child: children,
			},
		},
	}
}

func Intersection(children ...*openfgapb.Userset) *openfgapb.Userset {
	return &openfgapb.Userset{
		Userset: &openfgapb.Userset_Intersection{
			Intersection: &openfgapb.Usersets{
				Child: children,
			},
		},
	}
}

func Difference(base *openfgapb.Userset, sub *openfgapb.Userset) *openfgapb.Userset {
	return &openfgapb.Userset{
		Userset: &openfgapb.Userset_Difference{
			Difference: &openfgapb.Difference{
				Base:     base,
				Subtract: sub,
			},
		},
	}
}

type TypeSystem struct {
	schemaVersion   string
	typeDefinitions map[string]*openfgapb.TypeDefinition
}

// New creates a *TypeSystem from an *openfgapb.AuthorizationModel. New assumes that the model
// has already been validated.
func New(model *openfgapb.AuthorizationModel) *TypeSystem {
	tds := map[string]*openfgapb.TypeDefinition{}
	for _, td := range model.GetTypeDefinitions() {
		tds[td.GetType()] = td
	}

	return &TypeSystem{
		schemaVersion:   model.GetSchemaVersion(),
		typeDefinitions: tds,
	}
}

func (t *TypeSystem) GetSchemaVersion() string {
	return t.schemaVersion
}

func (t *TypeSystem) GetTypeDefinitions() map[string]*openfgapb.TypeDefinition {
	return t.typeDefinitions
}

func (t *TypeSystem) GetTypeDefinition(objectType string) (*openfgapb.TypeDefinition, bool) {
	if typeDefinition, ok := t.typeDefinitions[objectType]; ok {
		return typeDefinition, true
	}
	return nil, false
}

func (t *TypeSystem) GetRelations(objectType string) (map[string]*openfgapb.Relation, bool) {
	td, ok := t.typeDefinitions[objectType]
	if !ok {
		return nil, false
	}

	relations := map[string]*openfgapb.Relation{}

	for relation, rewrite := range td.GetRelations() {
		r := &openfgapb.Relation{
			Name:     relation,
			Rewrite:  rewrite,
			TypeInfo: &openfgapb.RelationTypeInfo{},
		}

		if metadata, ok := td.GetMetadata().GetRelations()[relation]; ok {
			r.TypeInfo.DirectlyRelatedUserTypes = metadata.GetDirectlyRelatedUserTypes()
		}

		relations[relation] = r
	}

	return relations, true
}

func (t *TypeSystem) GetRelation(objectType, relation string) (*openfgapb.Relation, bool) {
	relations, ok := t.GetRelations(objectType)
	if !ok {
		return nil, false
	}

	r, ok := relations[relation]
	if !ok {
		return nil, false
	}

	return r, true
}

func (t *TypeSystem) GetDirectlyRelatedUserTypes(objectType, relation string) []*openfgapb.RelationReference {
	if r, ok := t.GetRelation(objectType, relation); ok {
		return r.GetTypeInfo().GetDirectlyRelatedUserTypes()
	}

	return nil
}

// IsDirectlyRelated determines whether the type of the target RelationReference contains the source RelationReference.
func (t *TypeSystem) IsDirectlyRelated(target *openfgapb.RelationReference, source *openfgapb.RelationReference) bool {
	if relation, ok := t.GetRelation(target.GetType(), target.GetRelation()); ok {
		for _, relationReference := range relation.GetTypeInfo().GetDirectlyRelatedUserTypes() {
			if source.GetType() == relationReference.GetType() && source.GetRelation() == relationReference.GetRelation() {
				return true
			}
		}
	}

	return false
}

// Validate validates an *openfgapb.AuthorizationModel according to the following rules:
//  1. Checks that the model have a valid schema version.
//  2. For every rewrite the relations in the rewrite must:
//     a. Be valid relations on the same type in the authorization model (in cases of computedUserset)
//     b. Be valid relations on another existing type (in cases of tupleToUserset)
//  3. Do not allow duplicate types or duplicate relations (only need to check types as relations are
//     in a map so cannot contain duplicates)
//
// If the authorization model has a v1.1 schema version  (with types on relations), then additionally
// validate the type system according to the following rules:
//  3. Every type restriction on a relation must be a valid type:
//     a. For a type (e.g. user) this means checking that this type is in the TypeSystem
//     b. For a type#relation this means checking that this type with this relation is in the TypeSystem
//  4. Check that a relation is assignable if and only if it has a non-zero list of types
func Validate(model *openfgapb.AuthorizationModel) error {
	schemaVersion := model.GetSchemaVersion()

	if schemaVersion != SchemaVersion1_0 && schemaVersion != SchemaVersion1_1 {
		return ErrInvalidSchemaVersion
	}

	if containsDuplicateType(model) {
		return ErrDuplicateTypes
	}

	if err := validateRelationRewrites(model); err != nil {
		return err
	}

	if schemaVersion == SchemaVersion1_1 {
		if err := validateRelationTypeRestrictions(model); err != nil {
			return err
		}
	}

	return nil
}

func containsDuplicateType(model *openfgapb.AuthorizationModel) bool {
	seen := map[string]struct{}{}
	for _, td := range model.TypeDefinitions {
		objectType := td.GetType()
		if _, ok := seen[objectType]; ok {
			return true
		}
		seen[objectType] = struct{}{}
	}
	return false
}

func validateRelationRewrites(model *openfgapb.AuthorizationModel) error {
	typeDefinitions := model.GetTypeDefinitions()

	allRelations := map[string]struct{}{}
	typeToRelations := map[string]map[string]struct{}{}
	for _, td := range typeDefinitions {
		objectType := td.GetType()
		typeToRelations[objectType] = map[string]struct{}{}
		for relation := range td.GetRelations() {
			typeToRelations[objectType][relation] = struct{}{}
			allRelations[relation] = struct{}{}
		}
	}

	for _, td := range typeDefinitions {
		objectType := td.GetType()
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

func validateRelationTypeRestrictions(model *openfgapb.AuthorizationModel) error {
	t := New(model)

	for objectType := range t.typeDefinitions {
		relations, ok := t.GetRelations(objectType)
		if !ok {
			return InvalidRelationError(objectType, "")
		}

		for name, relation := range relations {
			relatedTypes := relation.GetTypeInfo().GetDirectlyRelatedUserTypes()

			assignable := t.IsDirectlyAssignable(relation)
			if assignable && len(relatedTypes) == 0 {
				return AssignableRelationError(objectType, name)
			}

			if !assignable && len(relatedTypes) != 0 {
				return NonAssignableRelationError(objectType, name)
			}

			for _, related := range relatedTypes {
				relatedObjectType := related.GetType()
				relatedRelation := related.GetRelation()

				if _, ok := t.GetRelations(relatedObjectType); !ok {
					return InvalidRelationTypeError(objectType, name, relatedObjectType, relatedRelation)
				}

				if relatedRelation != "" {
					if _, ok := t.GetRelation(relatedObjectType, relatedRelation); !ok {
						return InvalidRelationTypeError(objectType, name, relatedObjectType, relatedRelation)
					}
				}
			}
		}
	}

	return nil
}

func (t *TypeSystem) IsDirectlyAssignable(relation *openfgapb.Relation) bool {
	rewrite := relation.GetRewrite()

	return ContainsSelf(rewrite)
}

func ContainsSelf(rewrite *openfgapb.Userset) bool {
	switch rw := rewrite.GetUserset().(type) {
	case *openfgapb.Userset_This:
		return true
	case *openfgapb.Userset_Union:
		for _, child := range rw.Union.GetChild() {
			if ContainsSelf(child) {
				return true
			}
		}
	case *openfgapb.Userset_Intersection:
		for _, child := range rw.Intersection.GetChild() {
			if ContainsSelf(child) {
				return true
			}
		}
	case *openfgapb.Userset_Difference:
		difference := rw.Difference
		if ContainsSelf(difference.GetBase()) || ContainsSelf(difference.GetSubtract()) {
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
