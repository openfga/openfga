package typesystem

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/openfga/openfga/pkg/tuple"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type ctxKey string

const (
	SchemaVersion1_0 string = "1.0"
	SchemaVersion1_1 string = "1.1"

	typesystemCtxKey ctxKey = "typesystem-context-key"
)

var (
	ErrDuplicateTypes        = errors.New("an authorization model cannot contain duplicate types")
	ErrInvalidSchemaVersion  = errors.New("invalid schema version")
	ErrInvalidModel          = errors.New("invalid authorization model encountered")
	ErrRelationUndefined     = errors.New("undefined relation")
	ErrObjectTypeUndefined   = errors.New("undefined object type")
	ErrInvalidUsersetRewrite = errors.New("invalid userset rewrite definition")
	ErrReservedKeywords      = errors.New("self and this are reserved keywords")
	ErrCycle                 = errors.New("an authorization model cannot contain a cycle")
)

func IsSchemaVersionSupported(version string) bool {
	switch version {
	case SchemaVersion1_1:
		return true
	default:
		return false
	}
}

// ContextWithTypesystem attaches the provided TypeSystem to the parent context.
func ContextWithTypesystem(parent context.Context, typesys *TypeSystem) context.Context {
	return context.WithValue(parent, typesystemCtxKey, typesys)
}

// TypesystemFromContext returns the TypeSystem from the provided context (if any).
func TypesystemFromContext(ctx context.Context) (*TypeSystem, bool) {
	typesys, ok := ctx.Value(typesystemCtxKey).(*TypeSystem)
	return typesys, ok
}

func DirectRelationReference(objectType, relation string) *openfgapb.RelationReference {
	relationReference := &openfgapb.RelationReference{
		Type: objectType,
	}
	if relation != "" {
		relationReference.RelationOrWildcard = &openfgapb.RelationReference_Relation{
			Relation: relation,
		}
	}

	return relationReference
}

func WildcardRelationReference(objectType string) *openfgapb.RelationReference {
	return &openfgapb.RelationReference{
		Type: objectType,
		RelationOrWildcard: &openfgapb.RelationReference_Wildcard{
			Wildcard: &openfgapb.Wildcard{},
		},
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

func TupleToUserset(tupleset, computedUserset string) *openfgapb.Userset {
	return &openfgapb.Userset{
		Userset: &openfgapb.Userset_TupleToUserset{
			TupleToUserset: &openfgapb.TupleToUserset{
				Tupleset: &openfgapb.ObjectRelation{
					Relation: tupleset,
				},
				ComputedUserset: &openfgapb.ObjectRelation{
					Relation: computedUserset,
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
	// [objectType] => typeDefinition
	typeDefinitions map[string]*openfgapb.TypeDefinition
	// [objectType] => [relationName] => relation
	relations     map[string]map[string]*openfgapb.Relation
	modelID       string
	schemaVersion string
}

// New creates a *TypeSystem from an *openfgapb.AuthorizationModel.
// It assumes that the input model is valid. If you need to run validations, use NewAndValidate.
func New(model *openfgapb.AuthorizationModel) *TypeSystem {
	tds := make(map[string]*openfgapb.TypeDefinition, len(model.GetTypeDefinitions()))
	relations := make(map[string]map[string]*openfgapb.Relation, len(model.GetTypeDefinitions()))

	for _, td := range model.GetTypeDefinitions() {
		tds[td.GetType()] = td
		tdRelations := make(map[string]*openfgapb.Relation, len(td.GetRelations()))

		for relation, rewrite := range td.GetRelations() {
			r := &openfgapb.Relation{
				Name:     relation,
				Rewrite:  rewrite,
				TypeInfo: &openfgapb.RelationTypeInfo{},
			}

			if metadata, ok := td.GetMetadata().GetRelations()[relation]; ok {
				r.TypeInfo.DirectlyRelatedUserTypes = metadata.GetDirectlyRelatedUserTypes()
			}

			tdRelations[relation] = r
		}
		relations[td.GetType()] = tdRelations
	}

	return &TypeSystem{
		modelID:         model.GetId(),
		schemaVersion:   model.GetSchemaVersion(),
		typeDefinitions: tds,
		relations:       relations,
	}
}

// GetAuthorizationModelID returns the id for the authorization model this
// TypeSystem was constructed for.
func (t *TypeSystem) GetAuthorizationModelID() string {
	return t.modelID
}

func (t *TypeSystem) GetSchemaVersion() string {
	return t.schemaVersion
}

func (t *TypeSystem) GetTypeDefinition(objectType string) (*openfgapb.TypeDefinition, bool) {
	if typeDefinition, ok := t.typeDefinitions[objectType]; ok {
		return typeDefinition, true
	}
	return nil, false
}

// GetRelations returns all relations in the TypeSystem for a given type
func (t *TypeSystem) GetRelations(objectType string) (map[string]*openfgapb.Relation, error) {
	_, ok := t.GetTypeDefinition(objectType)
	if !ok {
		return nil, &ObjectTypeUndefinedError{
			ObjectType: objectType,
			Err:        ErrObjectTypeUndefined,
		}
	}

	return t.relations[objectType], nil
}

func (t *TypeSystem) GetRelation(objectType, relation string) (*openfgapb.Relation, error) {
	relations, err := t.GetRelations(objectType)
	if err != nil {
		return nil, err
	}

	r, ok := relations[relation]
	if !ok {
		return nil, &RelationUndefinedError{
			ObjectType: objectType,
			Relation:   relation,
			Err:        ErrRelationUndefined,
		}
	}

	return r, nil
}

// GetRelationReferenceAsString returns team#member, or team:*, or an empty string if the input is nil.
func GetRelationReferenceAsString(rr *openfgapb.RelationReference) string {
	if rr == nil {
		return ""
	}
	if _, ok := rr.RelationOrWildcard.(*openfgapb.RelationReference_Relation); ok {
		return fmt.Sprintf("%s#%s", rr.GetType(), rr.GetRelation())
	}
	if _, ok := rr.RelationOrWildcard.(*openfgapb.RelationReference_Wildcard); ok {
		return fmt.Sprintf("%s:*", rr.GetType())
	}

	panic("unexpected relation reference")
}

func (t *TypeSystem) GetDirectlyRelatedUserTypes(objectType, relation string) ([]*openfgapb.RelationReference, error) {

	r, err := t.GetRelation(objectType, relation)
	if err != nil {
		return nil, err
	}

	return r.GetTypeInfo().GetDirectlyRelatedUserTypes(), nil
}

// IsDirectlyRelated determines whether the type of the target DirectRelationReference contains the source DirectRelationReference.
func (t *TypeSystem) IsDirectlyRelated(target *openfgapb.RelationReference, source *openfgapb.RelationReference) (bool, error) {

	relation, err := t.GetRelation(target.GetType(), target.GetRelation())
	if err != nil {
		return false, err
	}

	for _, typeRestriction := range relation.GetTypeInfo().GetDirectlyRelatedUserTypes() {
		if source.GetType() == typeRestriction.GetType() {

			// type with no relation or wildcard (e.g. 'user')
			if typeRestriction.GetRelationOrWildcard() == nil && source.GetRelationOrWildcard() == nil {
				return true, nil
			}

			// typed wildcard (e.g. 'user:*')
			if typeRestriction.GetWildcard() != nil && source.GetWildcard() != nil {
				return true, nil
			}

			if typeRestriction.GetRelation() != "" && source.GetRelation() != "" &&
				typeRestriction.GetRelation() == source.GetRelation() {
				return true, nil
			}
		}
	}

	return false, nil
}

/*
 * IsPubliclyAssignable returns true if the provided objectType is part of a typed wildcard type restriction
 * on the target relation.
 *
 * type user
 *
 * type document
 *   relations
 *     define viewer: [user:*]
 *
 * In the example above, the 'user' objectType is publicly assignable to the 'document#viewer' relation.
 */
func (t *TypeSystem) IsPubliclyAssignable(target *openfgapb.RelationReference, objectType string) (bool, error) {

	relation, err := t.GetRelation(target.GetType(), target.GetRelation())
	if err != nil {
		return false, err
	}

	for _, typeRestriction := range relation.GetTypeInfo().GetDirectlyRelatedUserTypes() {
		if typeRestriction.GetType() == objectType {
			if typeRestriction.GetWildcard() != nil {
				return true, nil
			}
		}
	}

	return false, nil
}

func (t *TypeSystem) HasTypeInfo(objectType, relation string) (bool, error) {
	r, err := t.GetRelation(objectType, relation)
	if err != nil {
		return false, err
	}

	if t.GetSchemaVersion() == SchemaVersion1_1 && r.GetTypeInfo() != nil {
		return true, nil
	}

	return false, nil
}

// RelationInvolvesIntersection returns true if the provided relation's userset rewrite
// is defined by one or more direct or indirect intersections or any of the types related to
// the provided relation are defined by one or more direct or indirect intersections.
func (t *TypeSystem) RelationInvolvesIntersection(objectType, relation string) (bool, error) {
	visited := map[string]struct{}{}
	return t.relationInvolvesIntersection(objectType, relation, visited)
}

func (t *TypeSystem) relationInvolvesIntersection(objectType, relation string, visited map[string]struct{}) (bool, error) {

	key := tuple.ToObjectRelationString(objectType, relation)
	if _, ok := visited[key]; ok {
		return false, nil
	}

	visited[key] = struct{}{}

	rel, err := t.GetRelation(objectType, relation)
	if err != nil {
		return false, err
	}

	rewrite := rel.GetRewrite()

	result, err := WalkUsersetRewrite(rewrite, func(r *openfgapb.Userset) interface{} {

		switch rw := r.GetUserset().(type) {
		case *openfgapb.Userset_ComputedUserset:
			rewrittenRelation := rw.ComputedUserset.GetRelation()
			rewritten, err := t.GetRelation(objectType, rewrittenRelation)
			if err != nil {
				return err
			}

			containsIntersection, err := t.relationInvolvesIntersection(
				objectType,
				rewritten.GetName(),
				visited,
			)
			if err != nil {
				return err
			}

			if containsIntersection {
				return true
			}

		case *openfgapb.Userset_TupleToUserset:
			tupleset := rw.TupleToUserset.GetTupleset().GetRelation()
			rewrittenRelation := rw.TupleToUserset.ComputedUserset.GetRelation()

			tuplesetRel, err := t.GetRelation(objectType, tupleset)
			if err != nil {
				return err
			}

			directlyRelatedTypes := tuplesetRel.GetTypeInfo().GetDirectlyRelatedUserTypes()
			for _, relatedType := range directlyRelatedTypes {
				// must be of the form 'objectType' by this point since we disallow `tupleset` relations of the form `objectType:id#relation`
				r := relatedType.GetRelation()
				if r != "" {
					return fmt.Errorf(
						"invalid type restriction '%s#%s' specified on tupleset relation '%s#%s': %w",
						relatedType.GetType(),
						relatedType.GetRelation(),
						objectType,
						tupleset,
						ErrInvalidModel,
					)
				}

				rel, err := t.GetRelation(relatedType.GetType(), rewrittenRelation)
				if err != nil {
					if errors.Is(err, ErrObjectTypeUndefined) || errors.Is(err, ErrRelationUndefined) {
						continue
					}

					return err
				}

				containsIntersection, err := t.relationInvolvesIntersection(
					relatedType.GetType(),
					rel.GetName(),
					visited,
				)
				if err != nil {
					return err
				}

				if containsIntersection {
					return true
				}
			}

			return nil

		case *openfgapb.Userset_Intersection:
			return true
		}

		return nil
	})
	if err != nil {
		return false, err
	}

	if result != nil && result.(bool) {
		return true, nil
	}

	for _, typeRestriction := range rel.GetTypeInfo().GetDirectlyRelatedUserTypes() {
		if typeRestriction.GetRelation() != "" {

			key := tuple.ToObjectRelationString(typeRestriction.GetType(), typeRestriction.GetRelation())
			if _, ok := visited[key]; ok {
				continue
			}

			containsIntersection, err := t.relationInvolvesIntersection(
				typeRestriction.GetType(),
				typeRestriction.GetRelation(),
				visited,
			)
			if err != nil {
				return false, err
			}

			if containsIntersection {
				return true, nil
			}
		}
	}

	return false, nil
}

// RelationInvolvesExclusion returns true if the provided relation's userset rewrite
// is defined by one or more direct or indirect exclusions or any of the types related to
// the provided relation are defined by one or more direct or indirect exclusions.
func (t *TypeSystem) RelationInvolvesExclusion(objectType, relation string) (bool, error) {
	visited := map[string]struct{}{}
	return t.relationInvolvesExclusion(objectType, relation, visited)

}

func (t *TypeSystem) relationInvolvesExclusion(objectType, relation string, visited map[string]struct{}) (bool, error) {

	key := tuple.ToObjectRelationString(objectType, relation)
	if _, ok := visited[key]; ok {
		return false, nil
	}

	visited[key] = struct{}{}

	rel, err := t.GetRelation(objectType, relation)
	if err != nil {
		return false, err
	}

	rewrite := rel.GetRewrite()

	result, err := WalkUsersetRewrite(rewrite, func(r *openfgapb.Userset) interface{} {
		switch rw := r.GetUserset().(type) {
		case *openfgapb.Userset_ComputedUserset:
			rewrittenRelation := rw.ComputedUserset.GetRelation()
			rewritten, err := t.GetRelation(objectType, rewrittenRelation)
			if err != nil {
				return err
			}

			containsExclusion, err := t.relationInvolvesExclusion(
				objectType,
				rewritten.GetName(),
				visited,
			)
			if err != nil {
				return err
			}

			if containsExclusion {
				return true
			}

		case *openfgapb.Userset_TupleToUserset:
			tupleset := rw.TupleToUserset.GetTupleset().GetRelation()
			rewrittenRelation := rw.TupleToUserset.ComputedUserset.GetRelation()

			tuplesetRel, err := t.GetRelation(objectType, tupleset)
			if err != nil {
				return err
			}

			directlyRelatedTypes := tuplesetRel.GetTypeInfo().GetDirectlyRelatedUserTypes()
			for _, relatedType := range directlyRelatedTypes {
				// must be of the form 'objectType' by this point since we disallow `tupleset` relations of the form `objectType:id#relation`
				r := relatedType.GetRelation()
				if r != "" {
					return fmt.Errorf(
						"invalid type restriction '%s#%s' specified on tupleset relation '%s#%s': %w",
						relatedType.GetType(),
						relatedType.GetRelation(),
						objectType,
						tupleset,
						ErrInvalidModel,
					)
				}

				rel, err := t.GetRelation(relatedType.GetType(), rewrittenRelation)
				if err != nil {
					if errors.Is(err, ErrObjectTypeUndefined) || errors.Is(err, ErrRelationUndefined) {
						continue
					}

					return err
				}

				containsExclusion, err := t.relationInvolvesExclusion(
					relatedType.GetType(),
					rel.GetName(),
					visited,
				)
				if err != nil {
					return err
				}

				if containsExclusion {
					return true
				}
			}

			return nil

		case *openfgapb.Userset_Difference:
			return true
		}

		return nil
	})
	if err != nil {
		return false, err
	}

	if result != nil && result.(bool) {
		return true, nil
	}

	for _, typeRestriction := range rel.GetTypeInfo().GetDirectlyRelatedUserTypes() {
		if typeRestriction.GetRelation() != "" {

			key := tuple.ToObjectRelationString(typeRestriction.GetType(), typeRestriction.GetRelation())
			if _, ok := visited[key]; ok {
				continue
			}

			containsExclusion, err := t.relationInvolvesExclusion(
				typeRestriction.GetType(),
				typeRestriction.GetRelation(),
				visited,
			)
			if err != nil {
				return false, err
			}

			if containsExclusion {
				return true, nil
			}
		}
	}

	return false, nil
}

// NewAndValidate is like New but also validates the model according to the following rules:
//  1. Checks that the *TypeSystem have a valid schema version.
//  2. For every rewrite the relations in the rewrite must:
//     a. Be valid relations on the same type in the *TypeSystem (in cases of computedUserset)
//     b. Be valid relations on another existing type (in cases of tupleToUserset)
//  3. Do not allow duplicate types or duplicate relations (only need to check types as relations are
//     in a map so cannot contain duplicates)
//
// If the *TypeSystem has a v1.1 schema version (with types on relations), then additionally
// validate the *TypeSystem according to the following rules:
//  3. Every type restriction on a relation must be a valid type:
//     a. For a type (e.g. user) this means checking that this type is in the *TypeSystem
//     b. For a type#relation this means checking that this type with this relation is in the *TypeSystem
//  4. Check that a relation is assignable if and only if it has a non-zero list of types
func NewAndValidate(model *openfgapb.AuthorizationModel) (*TypeSystem, error) {
	t := New(model)
	schemaVersion := t.GetSchemaVersion()

	if !IsSchemaVersionSupported(schemaVersion) {
		return nil, ErrInvalidSchemaVersion
	}

	if containsDuplicateType(model) {
		return nil, ErrDuplicateTypes
	}

	if err := t.validateNames(); err != nil {
		return nil, err
	}

	// Validate the userset rewrites
	for _, td := range t.typeDefinitions {
		for relation, rewrite := range td.GetRelations() {
			err := t.isUsersetRewriteValid(td.GetType(), relation, rewrite)
			if err != nil {
				return nil, err
			}
		}
	}

	if err := t.ensureNoCyclesInTupleToUsersetDefinitions(); err != nil {
		return nil, err
	}

	if err := t.ensureNoCyclesInComputedRewrite(); err != nil {
		return nil, err
	}

	if schemaVersion == SchemaVersion1_1 {
		if err := t.validateRelationTypeRestrictions(); err != nil {
			return nil, err
		}
	}

	return t, nil
}

func containsDuplicateType(model *openfgapb.AuthorizationModel) bool {
	seen := make(map[string]struct{}, len(model.GetTypeDefinitions()))
	for _, td := range model.GetTypeDefinitions() {
		objectType := td.GetType()
		if _, ok := seen[objectType]; ok {
			return true
		}
		seen[objectType] = struct{}{}
	}
	return false
}

// validateNames ensures that a model doesn't have object types or relations
// called "self" or "this"
func (t *TypeSystem) validateNames() error {
	for _, td := range t.typeDefinitions {
		objectType := td.GetType()
		if objectType == "self" || objectType == "this" {
			return &InvalidTypeError{ObjectType: objectType, Cause: ErrReservedKeywords}
		}

		for relation := range td.GetRelations() {
			if relation == "self" || relation == "this" {
				return &InvalidRelationError{ObjectType: objectType, Relation: relation, Cause: ErrReservedKeywords}
			}
		}
	}

	return nil
}

// isUsersetRewriteValid checks if the rewrite on objectType#relation is valid.
func (t *TypeSystem) isUsersetRewriteValid(objectType, relation string, rewrite *openfgapb.Userset) error {
	if rewrite.GetUserset() == nil {
		return &InvalidRelationError{ObjectType: objectType, Relation: relation, Cause: ErrInvalidUsersetRewrite}
	}

	switch r := rewrite.GetUserset().(type) {
	case *openfgapb.Userset_ComputedUserset:
		computedUserset := r.ComputedUserset.GetRelation()
		if computedUserset == relation {
			return &InvalidRelationError{ObjectType: objectType, Relation: relation, Cause: ErrInvalidUsersetRewrite}
		}
		if _, err := t.GetRelation(objectType, computedUserset); err != nil {
			return &RelationUndefinedError{ObjectType: objectType, Relation: computedUserset, Err: ErrRelationUndefined}
		}
	case *openfgapb.Userset_TupleToUserset:
		tupleset := r.TupleToUserset.GetTupleset().GetRelation()

		tuplesetRelation, err := t.GetRelation(objectType, tupleset)
		if err != nil {
			return &RelationUndefinedError{ObjectType: objectType, Relation: tupleset, Err: ErrRelationUndefined}
		}

		// tupleset relations must only be direct relationships, no rewrites are allowed on them
		tuplesetRewrite := tuplesetRelation.GetRewrite()
		if reflect.TypeOf(tuplesetRewrite.GetUserset()) != reflect.TypeOf(&openfgapb.Userset_This{}) {
			return fmt.Errorf("the '%s#%s' relation is referenced in at least one tupleset and thus must be a direct relation", objectType, tupleset)
		}

		computedUserset := r.TupleToUserset.GetComputedUserset().GetRelation()

		if t.GetSchemaVersion() == SchemaVersion1_1 {
			// for 1.1 models, relation `computedUserset` has to be defined in one of the types declared by the tupleset's list of allowed types
			userTypes := tuplesetRelation.GetTypeInfo().GetDirectlyRelatedUserTypes()
			for _, rr := range userTypes {
				if _, err := t.GetRelation(rr.GetType(), computedUserset); err == nil {
					return nil
				}
			}

			return errors.Join(ErrRelationUndefined, fmt.Errorf("%s does not appear as a relation in any of the directly related user types %v", computedUserset, userTypes))
		} else {
			// for 1.0 models, relation `computedUserset` has to be defined _somewhere_ in the model
			for typeName := range t.relations {
				if _, err := t.GetRelation(typeName, computedUserset); err == nil {
					return nil
				}
			}
			return &RelationUndefinedError{ObjectType: "", Relation: computedUserset, Err: ErrRelationUndefined}
		}
	case *openfgapb.Userset_Union:
		for _, child := range r.Union.GetChild() {
			err := t.isUsersetRewriteValid(objectType, relation, child)
			if err != nil {
				return err
			}
		}
	case *openfgapb.Userset_Intersection:
		for _, child := range r.Intersection.GetChild() {
			err := t.isUsersetRewriteValid(objectType, relation, child)
			if err != nil {
				return err
			}
		}
	case *openfgapb.Userset_Difference:
		err := t.isUsersetRewriteValid(objectType, relation, r.Difference.Base)
		if err != nil {
			return err
		}

		err = t.isUsersetRewriteValid(objectType, relation, r.Difference.Subtract)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *TypeSystem) validateRelationTypeRestrictions() error {
	for objectType := range t.typeDefinitions {
		relations, err := t.GetRelations(objectType)
		if err != nil {
			return err
		}

		for name, relation := range relations {
			relatedTypes := relation.GetTypeInfo().GetDirectlyRelatedUserTypes()
			assignable := t.IsDirectlyAssignable(relation)

			if assignable && len(relatedTypes) == 0 {
				return AssignableRelationError(objectType, name)
			}

			if assignable && len(relatedTypes) == 1 {
				relatedObjectType := relatedTypes[0].GetType()
				relatedRelation := relatedTypes[0].GetRelation()
				if objectType == relatedObjectType && name == relatedRelation {
					return &InvalidRelationError{ObjectType: objectType, Relation: name, Cause: ErrCycle}
				}
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

				if related.GetRelationOrWildcard() != nil {
					// The type of the relation cannot contain a userset or wildcard if the relation is a tupleset relation.
					if ok, _ := t.IsTuplesetRelation(objectType, name); ok {
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
	}

	return nil
}

// ensureNoCyclesInTupleToUsersetDefinitions throws an error on the following models because `viewer` is a cycle.
//
//	type folder
//	  relations
//	    define parent: [folder] as self
//	    define viewer as viewer from parent
//
// and
//
//	type folder
//	  relations
//	    define parent as self
//	    define viewer as viewer from parent
func (t *TypeSystem) ensureNoCyclesInTupleToUsersetDefinitions() error {
	for objectType := range t.typeDefinitions {
		relations, err := t.GetRelations(objectType)
		if err == nil {
			for relationName, relation := range relations {
				switch cyclicDefinition := relation.GetRewrite().Userset.(type) {
				case *openfgapb.Userset_TupleToUserset:
					// define viewer as viewer from parent
					if cyclicDefinition.TupleToUserset.ComputedUserset.GetRelation() == relationName {
						tuplesetRelationName := cyclicDefinition.TupleToUserset.GetTupleset().GetRelation()
						tuplesetRelation, err := t.GetRelation(objectType, tuplesetRelationName)
						// define parent: [folder] as self
						if err == nil {
							switch tuplesetRelation.GetRewrite().Userset.(type) {
							case *openfgapb.Userset_This:
								if t.schemaVersion == SchemaVersion1_0 && len(t.typeDefinitions) == 1 {
									return &InvalidRelationError{ObjectType: objectType, Relation: relationName, Cause: ErrCycle}
								}
								if t.schemaVersion == SchemaVersion1_1 && len(tuplesetRelation.TypeInfo.DirectlyRelatedUserTypes) == 1 && tuplesetRelation.TypeInfo.DirectlyRelatedUserTypes[0].Type == objectType {
									return &InvalidRelationError{ObjectType: objectType, Relation: relationName, Cause: ErrCycle}
								}
							}
						}
					}
				}
			}
		}
	}

	return nil
}

// ensureNoCyclesInComputedRewrite throws an error on the following model because `folder` type is a cycle.
//
//	 type folder
//		 relations
//		  define parent as child
//		  define child as parent
func (t *TypeSystem) ensureNoCyclesInComputedRewrite() error {
	for objectType := range t.typeDefinitions {
		relations, err := t.GetRelations(objectType)
		if err == nil {
			for sourceRelationName, relation := range relations {
				switch source := relation.GetRewrite().Userset.(type) {
				case *openfgapb.Userset_ComputedUserset:
					target := source.ComputedUserset.GetRelation()
					targetRelation, err := t.GetRelation(objectType, target)
					if err == nil {
						switch rewrite := targetRelation.GetRewrite().Userset.(type) {
						case *openfgapb.Userset_ComputedUserset:
							if rewrite.ComputedUserset.GetRelation() == sourceRelationName {
								return &InvalidTypeError{ObjectType: objectType, Cause: ErrCycle}
							}
						}
					}
				}
			}
		}

	}

	return nil
}

func (t *TypeSystem) IsDirectlyAssignable(relation *openfgapb.Relation) bool {
	return RewriteContainsSelf(relation.GetRewrite())
}

// RewriteContainsSelf returns true if the provided userset rewrite
// is defined by one or more self referencing definitions.
func RewriteContainsSelf(rewrite *openfgapb.Userset) bool {

	result, err := WalkUsersetRewrite(rewrite, func(r *openfgapb.Userset) interface{} {
		if _, ok := r.Userset.(*openfgapb.Userset_This); ok {
			return true
		}

		return nil
	})
	if err != nil {
		panic("unexpected error during rewrite evaluation")
	}

	return result != nil && result.(bool) // type-cast matches the return from the WalkRelationshipRewriteHandler above
}

// RewriteContainsIntersection returns true if the provided userset rewrite
// is defined by one or more direct or indirect intersections.
func RewriteContainsIntersection(rewrite *openfgapb.Userset) bool {

	result, err := WalkUsersetRewrite(rewrite, func(r *openfgapb.Userset) interface{} {
		if _, ok := r.Userset.(*openfgapb.Userset_Intersection); ok {
			return true
		}

		return nil
	})
	if err != nil {
		panic("unexpected error during rewrite evaluation")
	}

	return result != nil && result.(bool) // type-cast matches the return from the WalkRelationshipRewriteHandler above
}

// RewriteContainsExclusion returns true if the provided userset rewrite
// is defined by one or more direct or indirect exclusions.
func RewriteContainsExclusion(rewrite *openfgapb.Userset) bool {

	result, err := WalkUsersetRewrite(rewrite, func(r *openfgapb.Userset) interface{} {
		if _, ok := r.Userset.(*openfgapb.Userset_Difference); ok {
			return true
		}

		return nil
	})
	if err != nil {
		panic("unexpected error during rewrite evaluation")
	}

	return result != nil && result.(bool) // type-cast matches the return from the WalkRelationshipRewriteHandler above
}

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
	return fmt.Sprintf("the definition of relation '%s' in object type '%s' is invalid", e.Relation, e.ObjectType)
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

func AssignableRelationError(objectType, relation string) error {
	return fmt.Errorf("the assignable relation '%s' in object type '%s' must contain at least one relation type", relation, objectType)
}

func NonAssignableRelationError(objectType, relation string) error {
	return fmt.Errorf("the non-assignable relation '%s' in object type '%s' should not contain a relation type", objectType, relation)
}

func InvalidRelationTypeError(objectType, relation, relatedObjectType, relatedRelation string) error {
	relationType := relatedObjectType
	if relatedRelation != "" {
		relationType = tuple.ToObjectRelationString(relatedObjectType, relatedRelation)
	}

	return fmt.Errorf("the relation type '%s' on '%s' in object type '%s' is not valid", relationType, relation, objectType)
}

// getAllTupleToUsersetsDefinitions returns a map where the key is the object type and the value
// is another map where key=relationName, value=list of tuple to usersets declared in that relation
func (t *TypeSystem) getAllTupleToUsersetsDefinitions() map[string]map[string][]*openfgapb.TupleToUserset {
	response := make(map[string]map[string][]*openfgapb.TupleToUserset, 0)
	for typeName, typeDef := range t.typeDefinitions {
		response[typeName] = make(map[string][]*openfgapb.TupleToUserset, 0)
		for relationName, relationDef := range typeDef.GetRelations() {
			ttus := make([]*openfgapb.TupleToUserset, 0)
			response[typeName][relationName] = t.tupleToUsersetsDefinitions(relationDef, &ttus)
		}
	}

	return response
}

// IsTuplesetRelation returns a boolean indicating if the provided relation is defined under a
// TupleToUserset rewrite as a tupleset relation (i.e. the right hand side of a `X from Y`).
func (t *TypeSystem) IsTuplesetRelation(objectType, relation string) (bool, error) {

	_, err := t.GetRelation(objectType, relation)
	if err != nil {
		return false, err
	}

	for _, ttuDefinitions := range t.getAllTupleToUsersetsDefinitions()[objectType] {
		for _, ttuDef := range ttuDefinitions {
			if ttuDef.Tupleset.Relation == relation {
				return true, nil
			}
		}
	}

	return false, nil
}

func (t *TypeSystem) tupleToUsersetsDefinitions(relationDef *openfgapb.Userset, resp *[]*openfgapb.TupleToUserset) []*openfgapb.TupleToUserset {
	if relationDef.GetTupleToUserset() != nil {
		*resp = append(*resp, relationDef.GetTupleToUserset())
	}
	if relationDef.GetUnion() != nil {
		for _, child := range relationDef.GetUnion().GetChild() {
			t.tupleToUsersetsDefinitions(child, resp)
		}
	}
	if relationDef.GetIntersection() != nil {
		for _, child := range relationDef.GetIntersection().GetChild() {
			t.tupleToUsersetsDefinitions(child, resp)
		}
	}
	if relationDef.GetDifference() != nil {
		t.tupleToUsersetsDefinitions(relationDef.GetDifference().GetBase(), resp)
		t.tupleToUsersetsDefinitions(relationDef.GetDifference().GetSubtract(), resp)
	}
	return *resp
}

// WalkUsersetRewriteHandler is a userset rewrite handler that is applied to a node in a userset rewrite
// tree. Implementations of the WalkUsersetRewriteHandler should return a non-nil value when the traversal
// over the rewrite tree should terminate and nil if traversal should proceed to other nodes in the tree.
type WalkUsersetRewriteHandler func(rewrite *openfgapb.Userset) interface{}

// WalkUsersetRewrite recursively walks the provided userset rewrite and invokes the provided WalkUsersetRewriteHandler
// to each node in the userset rewrite tree until the first non-nil response is encountered.
func WalkUsersetRewrite(rewrite *openfgapb.Userset, handler WalkUsersetRewriteHandler) (interface{}, error) {

	var children []*openfgapb.Userset

	if result := handler(rewrite); result != nil {
		return result, nil
	}

	switch t := rewrite.Userset.(type) {
	case *openfgapb.Userset_This:
		return handler(rewrite), nil
	case *openfgapb.Userset_ComputedUserset:
		return handler(rewrite), nil
	case *openfgapb.Userset_TupleToUserset:
		return handler(rewrite), nil
	case *openfgapb.Userset_Union:
		children = t.Union.GetChild()
	case *openfgapb.Userset_Intersection:
		children = t.Intersection.GetChild()
	case *openfgapb.Userset_Difference:
		children = append(children, t.Difference.GetBase(), t.Difference.GetSubtract())
	default:
		return nil, fmt.Errorf("unexpected userset rewrite type encountered")
	}

	for _, child := range children {
		result, err := WalkUsersetRewrite(child, handler)
		if err != nil {
			return nil, err
		}

		if result != nil {
			return result, nil
		}
	}

	return nil, nil
}
