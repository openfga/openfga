package typesystem

import (
	"errors"
	"fmt"
	"reflect"

	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

const (
	SchemaVersion1_0 = "1.0"
	SchemaVersion1_1 = "1.1"
)

var (
	ErrDuplicateTypes        = errors.New("an authorization model cannot contain duplicate types")
	ErrInvalidSchemaVersion  = errors.New("invalid schema version")
	ErrInvalidModel          = errors.New("invalid authorization model encountered")
	ErrRelationUndefined     = errors.New("undefined relation")
	ErrObjectTypeUndefined   = errors.New("undefined object type")
	ErrInvalidUsersetRewrite = errors.New("invalid userset rewrite definition")
)

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
	model           *openfgapb.AuthorizationModel
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
		model:           model,
		schemaVersion:   model.GetSchemaVersion(),
		typeDefinitions: tds,
	}
}

// GetAuthorizationModel returns the underlying AuthorizationModel this TypeSystem was
// constructed from.
func (t *TypeSystem) GetAuthorizationModel() *openfgapb.AuthorizationModel {
	return t.model
}

// GetAuthorizationModelID returns the id for the authorization model this
// TypeSystem was constructed for.
func (t *TypeSystem) GetAuthorizationModelID() string {
	return t.model.GetId()
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

func (t *TypeSystem) GetRelations(objectType string) (map[string]*openfgapb.Relation, error) {
	td, ok := t.typeDefinitions[objectType]
	if !ok {
		return nil, &ObjectTypeUndefinedError{
			ObjectType: objectType,
			Err:        ErrObjectTypeUndefined,
		}
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

	return relations, nil
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

	for _, relationReference := range relation.GetTypeInfo().GetDirectlyRelatedUserTypes() {
		if source.GetType() == relationReference.GetType() {
			// Either the relations are not wildcards and are the same
			// or the relationReference is a wildcard type (e.g. user:*) and the source may or may not have a wildcard (but certainly no relation)
			if relationReference.GetWildcard() == nil && source.GetWildcard() == nil && relationReference.GetRelation() == source.GetRelation() ||
				(relationReference.GetWildcard() != nil && source.GetRelation() == "") {
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
// is defined by one or more direct or indirect intersections.
func (t *TypeSystem) RelationInvolvesIntersection(objectType, relation string) (bool, error) {

	rel, err := t.GetRelation(objectType, relation)
	if err != nil {
		return false, err
	}

	rewrite := rel.GetRewrite()

	switch rw := rewrite.GetUserset().(type) {
	case *openfgapb.Userset_This:
		return false, nil
	case *openfgapb.Userset_ComputedUserset:
		rewrittenRelation := rw.ComputedUserset.GetRelation()
		rewritten, err := t.GetRelation(objectType, rewrittenRelation)
		if err != nil {
			return false, err
		}

		return t.RelationInvolvesIntersection(objectType, rewritten.GetName())
	case *openfgapb.Userset_TupleToUserset:
		tupleset := rw.TupleToUserset.GetTupleset().GetRelation()
		rewrittenRelation := rw.TupleToUserset.ComputedUserset.GetRelation()

		tuplesetRel, err := t.GetRelation(objectType, tupleset)
		if err != nil {
			return false, err
		}

		directlyRelatedTypes := tuplesetRel.GetTypeInfo().GetDirectlyRelatedUserTypes()
		for _, relatedType := range directlyRelatedTypes {
			// must be of the form 'objectType' by this point since we disallow `tupleset` relations of the form `objectType:id#relation`
			r := relatedType.GetRelation()
			if r != "" {
				return false, fmt.Errorf(
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

				return false, err
			}

			containsIntersection, err := t.RelationInvolvesIntersection(relatedType.GetType(), rel.GetName())
			if err != nil {
				return false, err
			}

			if containsIntersection {
				return true, nil
			}
		}
	case *openfgapb.Userset_Intersection:
		return true, nil
	case *openfgapb.Userset_Union:
		for _, child := range rw.Union.GetChild() {
			if RewriteContainsIntersection(child) {
				return true, nil
			}
		}
	case *openfgapb.Userset_Difference:
		difference := rw.Difference
		if RewriteContainsIntersection(difference.GetBase()) || RewriteContainsIntersection(difference.GetSubtract()) {
			return true, nil
		}
	}

	return false, nil
}

// RelationInvolvesExclusion returns true if the provided relation's userset rewrite
// is defined by one or more direct or indirect exclusions.
func (t *TypeSystem) RelationInvolvesExclusion(objectType, relation string) (bool, error) {
	rel, err := t.GetRelation(objectType, relation)
	if err != nil {
		return false, err
	}

	rewrite := rel.GetRewrite()

	switch rw := rewrite.GetUserset().(type) {
	case *openfgapb.Userset_This:
		return false, nil
	case *openfgapb.Userset_ComputedUserset:
		rewrittenRelation := rw.ComputedUserset.GetRelation()
		rewritten, err := t.GetRelation(objectType, rewrittenRelation)
		if err != nil {
			return false, err
		}

		return t.RelationInvolvesExclusion(objectType, rewritten.GetName())
	case *openfgapb.Userset_TupleToUserset:
		tupleset := rw.TupleToUserset.GetTupleset().GetRelation()
		rewrittenRelation := rw.TupleToUserset.ComputedUserset.GetRelation()

		tuplesetRel, err := t.GetRelation(objectType, tupleset)
		if err != nil {
			return false, err
		}

		directlyRelatedTypes := tuplesetRel.GetTypeInfo().GetDirectlyRelatedUserTypes()
		for _, relatedType := range directlyRelatedTypes {
			// must be of the form 'objectType' by this point since we disallow `tupleset` relations of the form `objectType:id#relation`
			r := relatedType.GetRelation()
			if r != "" {
				return false, fmt.Errorf(
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

				return false, err
			}

			containsExclusion, err := t.RelationInvolvesExclusion(relatedType.GetType(), rel.GetName())
			if err != nil {
				return false, err
			}

			if containsExclusion {
				return true, nil
			}
		}

	case *openfgapb.Userset_Intersection:
		for _, child := range rw.Intersection.GetChild() {
			if RewriteContainsExclusion(child) {
				return true, nil
			}
		}
	case *openfgapb.Userset_Union:
		for _, child := range rw.Union.GetChild() {
			if RewriteContainsExclusion(child) {
				return true, nil
			}
		}
	case *openfgapb.Userset_Difference:
		return true, nil
	}

	return false, nil
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

	if err := validateNames(model); err != nil {
		return err
	}

	if err := validateRelationRewrites(model); err != nil {
		return err
	}

	if schemaVersion == SchemaVersion1_1 {
		if err := validateRelationTypeRestrictions(model); err != nil {
			return err
		}
	}

	if err := ensureNoCyclesInTupleToUsersetDefinitions(model); err != nil {
		return err
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

func validateNames(model *openfgapb.AuthorizationModel) error {
	for _, td := range model.TypeDefinitions {
		objectType := td.GetType()
		if objectType == "self" || objectType == "this" {
			return fmt.Errorf("object type '%s' is invalid", objectType)
		}
		for relation := range td.GetRelations() {
			if relation == "self" || relation == "this" {
				return fmt.Errorf("the definition of relation '%s' in object type '%s' is invalid", relation, objectType)
			}
		}
	}
	return nil
}

func validateRelationRewrites(model *openfgapb.AuthorizationModel) error {
	typeDefinitions := model.GetTypeDefinitions()

	relations := map[string]*openfgapb.Relation{}
	typerels := map[string]map[string]*openfgapb.Relation{}

	for _, td := range typeDefinitions {
		objectType := td.GetType()

		typerels[objectType] = map[string]*openfgapb.Relation{}

		for relation, rewrite := range td.GetRelations() {
			relationMetadata := td.GetMetadata().GetRelations()
			md, ok := relationMetadata[relation]

			var typeinfo *openfgapb.RelationTypeInfo
			if ok {
				typeinfo = &openfgapb.RelationTypeInfo{
					DirectlyRelatedUserTypes: md.GetDirectlyRelatedUserTypes(),
				}
			}

			r := &openfgapb.Relation{
				Name:     relation,
				Rewrite:  rewrite,
				TypeInfo: typeinfo,
			}

			typerels[objectType][relation] = r
			relations[relation] = r
		}
	}

	for _, td := range typeDefinitions {
		objectType := td.GetType()

		for relation, rewrite := range td.GetRelations() {
			err := isUsersetRewriteValid(relations, typerels[objectType], objectType, relation, rewrite)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// isUsersetRewriteValid checks if a particular userset rewrite is valid. The first argument is all the relations in
// the typeSystem, the second argument is the subset of relations on the type where the rewrite occurs.
func isUsersetRewriteValid(
	allRelations map[string]*openfgapb.Relation,
	relationsOnType map[string]*openfgapb.Relation,
	objectType, relation string,
	rewrite *openfgapb.Userset,
) error {
	if rewrite.GetUserset() == nil {
		return &InvalidRelationError{ObjectType: objectType, Relation: relation, Cause: ErrInvalidUsersetRewrite}
	}

	switch t := rewrite.GetUserset().(type) {
	case *openfgapb.Userset_ComputedUserset:
		computedUserset := t.ComputedUserset.GetRelation()
		if computedUserset == relation {
			return &InvalidRelationError{ObjectType: objectType, Relation: relation, Cause: ErrInvalidUsersetRewrite}
		}
		if _, ok := relationsOnType[computedUserset]; !ok {
			return &RelationUndefinedError{ObjectType: objectType, Relation: computedUserset, Err: ErrRelationUndefined}
		}
	case *openfgapb.Userset_TupleToUserset:
		tupleset := t.TupleToUserset.GetTupleset().GetRelation()

		tuplesetRelation, ok := relationsOnType[tupleset]
		if !ok {
			return &RelationUndefinedError{ObjectType: objectType, Relation: tupleset, Err: ErrRelationUndefined}
		}

		// tupleset relations must only be direct relationships, no rewrites
		// are allowed on them
		tuplesetRewrite := tuplesetRelation.GetRewrite()
		if reflect.TypeOf(tuplesetRewrite.GetUserset()) != reflect.TypeOf(&openfgapb.Userset_This{}) {
			return fmt.Errorf("the '%s#%s' relation is referenced in at least one tupleset and thus must be a direct relation", objectType, tupleset)
		}

		computedUserset := t.TupleToUserset.GetComputedUserset().GetRelation()
		if _, ok := allRelations[computedUserset]; !ok {
			return &RelationUndefinedError{ObjectType: "", Relation: computedUserset, Err: ErrRelationUndefined}
		}

		if relation == tupleset && relation == computedUserset {
			return &InvalidRelationError{ObjectType: objectType, Relation: relation, Cause: nil}
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
	typesys := New(model)

	for objectType := range typesys.typeDefinitions {
		relations, err := typesys.GetRelations(objectType)
		if err != nil {
			return err
		}

		for name, relation := range relations {
			relatedTypes := relation.GetTypeInfo().GetDirectlyRelatedUserTypes()
			assignable := typesys.IsDirectlyAssignable(relation)

			if assignable && len(relatedTypes) == 0 {
				return AssignableRelationError(objectType, name)
			}

			if !assignable && len(relatedTypes) != 0 {
				return NonAssignableRelationError(objectType, name)
			}

			for _, related := range relatedTypes {
				relatedObjectType := related.GetType()
				relatedRelation := related.GetRelation()

				if _, err := typesys.GetRelations(relatedObjectType); err != nil {
					return InvalidRelationTypeError(objectType, name, relatedObjectType, relatedRelation)
				}

				if related.GetRelationOrWildcard() != nil {
					// The type of the relation cannot contain a userset or wildcard if the relation is a tupleset relation.
					if ok, _ := typesys.IsTuplesetRelation(objectType, name); ok {
						return InvalidRelationTypeError(objectType, name, relatedObjectType, relatedRelation)
					}

					if relatedRelation != "" {
						if _, err := typesys.GetRelation(relatedObjectType, relatedRelation); err != nil {
							return InvalidRelationTypeError(objectType, name, relatedObjectType, relatedRelation)
						}
					}
				}

			}

		}
	}

	return nil
}

// ensureNoCyclesInTupleToUsersetDefinitions throws an error on the following model because `viewer` is a cycle.
// type folder
//
//	 relations
//		define parent: [folder] as self
//		define viewer as viewer from parent
//
// and
//
// type folder
//
//	 relations
//		define parent as self
//		define viewer as viewer from parent
func ensureNoCyclesInTupleToUsersetDefinitions(model *openfgapb.AuthorizationModel) error {
	typesys := New(model)
	for objectType := range typesys.typeDefinitions {
		relations, err := typesys.GetRelations(objectType)
		if err != nil {
			return err
		}
		for relationName, relation := range relations {
			switch cyclicDefinition := relation.GetRewrite().Userset.(type) {
			case *openfgapb.Userset_TupleToUserset:
				// define viewer as viewer from parent
				if cyclicDefinition.TupleToUserset.ComputedUserset.GetRelation() == relationName {
					tuplesetRelationName := cyclicDefinition.TupleToUserset.GetTupleset().GetRelation()
					tuplesetRelation, err := typesys.GetRelation(objectType, tuplesetRelationName)
					// define parent: [folder] as self
					if err == nil {
						switch tuplesetRelation.GetRewrite().Userset.(type) {
						case *openfgapb.Userset_This:
							if typesys.schemaVersion == SchemaVersion1_0 && len(typesys.typeDefinitions) == 1 {
								return &InvalidRelationError{ObjectType: objectType, Relation: relationName, Cause: nil}
							}
							if typesys.schemaVersion == SchemaVersion1_1 && tuplesetRelation.TypeInfo.DirectlyRelatedUserTypes[0].Type == objectType {
								return &InvalidRelationError{ObjectType: objectType, Relation: relationName, Cause: nil}
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
	rewrite := relation.GetRewrite()

	return RewriteContainsSelf(rewrite)
}

// RewriteContainsSelf returns true if the provided userset rewrite
// is defined by one or more self referencing definitions.
func RewriteContainsSelf(rewrite *openfgapb.Userset) bool {
	switch rw := rewrite.GetUserset().(type) {
	case *openfgapb.Userset_This:
		return true
	case *openfgapb.Userset_Union:
		for _, child := range rw.Union.GetChild() {
			if RewriteContainsSelf(child) {
				return true
			}
		}
	case *openfgapb.Userset_Intersection:
		for _, child := range rw.Intersection.GetChild() {
			if RewriteContainsSelf(child) {
				return true
			}
		}
	case *openfgapb.Userset_Difference:
		difference := rw.Difference
		if RewriteContainsSelf(difference.GetBase()) || RewriteContainsSelf(difference.GetSubtract()) {
			return true
		}
	}

	return false
}

// RewriteContainsIntersection returns true if the provided userset rewrite
// is defined by one or more direct or indirect intersections.
func RewriteContainsIntersection(rewrite *openfgapb.Userset) bool {
	switch rw := rewrite.GetUserset().(type) {
	case *openfgapb.Userset_Intersection:
		return true
	case *openfgapb.Userset_Union:
		for _, child := range rw.Union.GetChild() {
			if RewriteContainsIntersection(child) {
				return true
			}
		}
	case *openfgapb.Userset_Difference:
		difference := rw.Difference
		if RewriteContainsIntersection(difference.GetBase()) || RewriteContainsIntersection(difference.GetSubtract()) {
			return true
		}
	}

	return false
}

// RewriteContainsExclusion returns true if the provided userset rewrite
// is defined by one or more direct or indirect exclusions.
func RewriteContainsExclusion(rewrite *openfgapb.Userset) bool {
	switch rw := rewrite.GetUserset().(type) {
	case *openfgapb.Userset_Intersection:
		for _, child := range rw.Intersection.GetChild() {
			if RewriteContainsExclusion(child) {
				return true
			}
		}
	case *openfgapb.Userset_Union:
		for _, child := range rw.Union.GetChild() {
			if RewriteContainsExclusion(child) {
				return true
			}
		}
	case *openfgapb.Userset_Difference:
		return true
	}

	return false
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
		relationType = fmt.Sprintf("%s#%s", relatedObjectType, relatedRelation)
	}

	return fmt.Errorf("the relation type '%s' on '%s' in object type '%s' is not valid", relationType, relation, objectType)
}

// getAllTupleToUsersetsDefinitions returns a map where the key is the object type and the value
// is another map where key=relationName, value=list of tuple to usersets declared in that relation
func (t *TypeSystem) getAllTupleToUsersetsDefinitions() map[string]map[string][]*openfgapb.TupleToUserset {
	response := make(map[string]map[string][]*openfgapb.TupleToUserset, 0)
	for typeName, typeDef := range t.GetTypeDefinitions() {
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
