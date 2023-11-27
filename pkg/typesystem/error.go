// Package typesystem contains code to manipulate authorization models
package typesystem

import (
	"errors"
	"fmt"

	"github.com/openfga/openfga/pkg/tuple"
)

var (
	ErrModelNotFound          = errors.New("authorization model not found")
	ErrDuplicateTypes         = errors.New("an authorization model cannot contain duplicate types")
	ErrInvalidSchemaVersion   = errors.New("invalid schema version")
	ErrInvalidModel           = errors.New("invalid authorization model encountered")
	ErrRelationUndefined      = errors.New("undefined relation")
	ErrObjectTypeUndefined    = errors.New("undefined object type")
	ErrInvalidUsersetRewrite  = errors.New("invalid userset rewrite definition")
	ErrReservedKeywords       = errors.New("self and this are reserved keywords")
	ErrCycle                  = errors.New("an authorization model cannot contain a cycle")
	ErrNoEntrypoints          = errors.New("no entrypoints defined")
	ErrNoEntryPointsLoop      = errors.New("potential loop")
	ErrNoConditionForRelation = errors.New("no condition defined for relation")
)

type InvalidTypeError struct {
	ObjectType string
	Cause      error
}

func (e *InvalidTypeError) Error() string {
	return fmt.Sprintf("the definition of type '%s' is invalid", e.ObjectType)
}

func (e *InvalidTypeError) Unwrap() error {
	return e.Cause
}

type InvalidRelationError struct {
	ObjectType string
	Relation   string
	Cause      error
}

func (e *InvalidRelationError) Error() string {
	return fmt.Sprintf("the definition of relation '%s' in object type '%s' is invalid: %s", e.Relation, e.ObjectType, e.Cause)
}

func (e *InvalidRelationError) Unwrap() error {
	return e.Cause
}

type ObjectTypeUndefinedError struct {
	ObjectType string
	Err        error
}

func (e *ObjectTypeUndefinedError) Error() string {
	return fmt.Sprintf("'%s' is an undefined object type", e.ObjectType)
}

func (e *ObjectTypeUndefinedError) Unwrap() error {
	return e.Err
}

type RelationUndefinedError struct {
	ObjectType string
	Relation   string
	Err        error
}

func (e *RelationUndefinedError) Error() string {
	if e.ObjectType != "" {
		return fmt.Sprintf("'%s#%s' relation is undefined", e.ObjectType, e.Relation)
	}

	return fmt.Sprintf("'%s' relation is undefined", e.Relation)
}

func (e *RelationUndefinedError) Unwrap() error {
	return e.Err
}

type RelationConditionError struct {
	Condition string
	Relation  string
	Err       error
}

func (e *RelationConditionError) Error() string {
	return fmt.Sprintf("condition %s is undefined for relation %s", e.Condition, e.Relation)
}

func (e *RelationConditionError) Unwrap() error {
	return e.Err
}

func AssignableRelationError(objectType, relation string) error {
	return fmt.Errorf("the assignable relation '%s' in object type '%s' must contain at least one relation type", relation, objectType)
}

func NonAssignableRelationError(objectType, relation string) error {
	return fmt.Errorf("the non-assignable relation '%s' in object type '%s' should not contain a relation type", relation, objectType)
}

func InvalidRelationTypeError(objectType, relation, relatedObjectType, relatedRelation string) error {
	relationType := relatedObjectType
	if relatedRelation != "" {
		relationType = tuple.ToObjectRelationString(relatedObjectType, relatedRelation)
	}

	return fmt.Errorf("the relation type '%s' on '%s' in object type '%s' is not valid", relationType, relation, objectType)
}
