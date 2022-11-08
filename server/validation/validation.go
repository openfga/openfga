package validation

import (
	"errors"
	"fmt"

	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

// ValidateTuple checks whether a tuple is valid. If it is not, returns as error. A tuple is valid
// if it validates according to the following rules:
//
// Rule 1:
// Rule 2:
//
// Rule 3: Wildcards '*' are not peritted on tupleset relations.
// If there exists a rewrite definition of the form `viewer from parent` anywhere in the model, then no * may be written to that 'parent' relation.
//
// Rule 4: Usersets 'object#relation' are not permitted on tupleset relations. 
// If there exists a rewrite definition of the form 'viewer from parent' anywhere in the model, then no userset may be written to that 'parent' relation.
//
// etc..
func ValidateTuple(model *openfgapb.AuthorizationModel, tk *openfgapb.TupleKey) error {

	typesys := typesystem.New(model)

	schemaVersion := typesys.GetSchemaVersion()

	// if schemaVersion == 1.1 then the user has to at least be an object (e.g. 'type:id') but could (optionally) be an ObjectRelation (object#relation) AND the `objectType` and (optionally) relation have to validate against the model
	// if schemaVersion == 1.0 then the user has to match the user_id regex OR be an ObjectRelation (object#relation) AND if an ObjectRelation the `objectType` and `relation` have to validate against the model.

	if !tuple.IsValidUser(tk.GetUser()) {
		return &tuple.InvalidTupleError{Reason: "the 'user' field is invalid", TupleKey: tk}
	}

	relation := tk.GetRelation()

	if !tuple.IsValidRelation(relation) {
		return &tuple.InvalidTupleError{Reason: "invalid relation", TupleKey: tk}
	}

	if !tuple.IsValidObject(tk.GetObject()) {
		return &tuple.InvalidObjectFormatError{TupleKey: tk}
	}

	objectType, objectID := tuple.SplitObject(tk.GetObject())
	if objectType == "" || objectID == "" {
		return &tuple.InvalidObjectFormatError{TupleKey: tk}
	}

	_, err := typesys.GetRelation(objectType, relation)
	if err != nil {
		if errors.Is(err, typesystem.ErrObjectTypeUndefined) {
			return &tuple.TypeNotFoundError{TypeName: objectType}
		}

		if errors.Is(err, typesystem.ErrRelationUndefined) {
			return &tuple.RelationNotFoundError{Relation: relation, TypeName: objectType, TupleKey: tk}
		}

		return err
	}

	return nil
}

func NoopFilterFunc() storage.TupleKeyFilterFunc {
	return func(tupleKey *openfgapb.TupleKey) bool {
		return true
	}
}

// FilterInvalidTuples implements the TupleFilterFunc signature and can be used to provide
// a generic filtering mechanism when reading tuples. It is particularly useful to filter
// out tuples that aren't valid according to the provided model, which can help filter
// tuples that were introduced due to another authorization model.
func FilterInvalidTuples(model *openfgapb.AuthorizationModel) storage.TupleKeyFilterFunc {

	typesys := typesystem.New(model)

	return func(tupleKey *openfgapb.TupleKey) bool {

		err := ValidateTuple(model, tupleKey)
		if err != nil {
			return false
		}

		return true

	// 	schemaVersion := typesys.GetSchemaVersion()

	// 	objectType, _ := tuple.SplitObject(tupleKey.GetObject())

	// 	if err := ValidateObject(tupleKey.GetObject(), model); err != nil {
	// 		return false
	// 	}

	// 	relation := tupleKey.GetRelation()
	// 	if err := ValidateRelation(objectType, relation, model); err != nil {
	// 		return false
	// 	}

	// 	user := tupleKey.GetUser()
	// 	if err := ValidateUser(user, model); err != nil {
	// 		return false
	// 	}

	// 	if schemaVersion == typesystem.SchemaVersion1_1 {
	// 		// validate type restrictions
	// 		relatedTypes, err := typesys.GetDirectlyRelatedUserTypes(objectType, relation)
	// 		if err != nil {
	// 			// handle error
	// 		}

	// 		userObjectType, userObjectID := tuple.SplitObject(tupleKey.GetUser())
	// 		_, userRelation := tuple.SplitObjectRelation(tupleKey.GetUser())

	// 		if userRelation == "" && userObjectID != "*" {
	// 			for _, typeInfo := range relatedTypes {
	// 				if typeInfo.GetType() == userObjectType {
	// 					return true
	// 				}
	// 			}
	// 		} else if userRelation != "" {
	// 			for _, typeInfo := range relatedTypes {
	// 				if typeInfo.GetType() == userObjectType && typeInfo.GetRelation() == userRelation {
	// 					return true
	// 				}
	// 			}
	// 		} else if userObjectID == "*" {
	// 			for _, typeInfo := range relatedTypes {
	// 				if typeInfo.GetType() != "" && typeInfo.GetRelation() == "" {
	// 					return true
	// 				}
	// 			}
	// 		}

	// 		return false
	// 	}

	// 	// default behavior is to filter tuples unless they explicitly match the conditions above
	// 	return false
	// }
}

// ValidateObject validates the provided object string 'type:id' against the provided
// model. An object is valid if it is welformed AND it validates against one of the
// type definitions included in the provided model.
func ValidateObject(object string, model *openfgapb.AuthorizationModel) error {
	if !tuple.IsValidObject(object) {
		// todo: handle error
	}

	objectType, _ := tuple.SplitObject(object)

	typesys := typesystem.New(model)
	_, ok := typesys.GetTypeDefinition(objectType)
	if !ok {
		return &tuple.TypeNotFoundError{TypeName: objectType}
	}

	return nil
}

// ValidateRelation validates the relation on the provided objectType against the given model.
// A relation is valid if it is welformed AND it is defined as a relation for the type definition
// of the given objectType.
func ValidateRelation(objectType, relation string, model *openfgapb.AuthorizationModel) error {
	if !tuple.IsValidRelation(relation) {
		// todo: handle error
	}

	// todo(jon-whit): memoize this
	typesys := typesystem.New(model)

	_, err := typesys.GetRelation(objectType, relation)
	if err != nil {
		if errors.Is(err, typesystem.ErrObjectTypeUndefined) {

		}

		if errors.Is(err, typesystem.ErrRelationUndefined) {

		}

		return err
	}

	return nil
}

func ValidateUser(user string, model *openfgapb.AuthorizationModel) error {
	return fmt.Errorf("not implemented")
}
