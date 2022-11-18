package validation

import (
	"context"
	"errors"

	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

// ValidateTuple checks whether a tuple is valid. If it is not, returns as error.
// If it is, it grabs the type of the tuple's object, and the tuple's relation, and returns its corresponding userset.
func ValidateTuple(ctx context.Context, backend storage.TypeDefinitionReadBackend, store, authorizationModelID string, tk *openfgapb.TupleKey) (*openfgapb.Userset, error) {
	if err := tuple.ValidateUser(tk); err != nil {
		return nil, err
	}
	user := tk.GetUser()
	if tuple.IsObjectRelation(user) {
		if err := ValidateUserIfItsAUserset(ctx, backend, store, authorizationModelID, user, tk); err != nil {
			return nil, err
		}
	}

	return ValidateObjectsRelations(ctx, backend, store, authorizationModelID, tk)
}

// ValidateObjectsRelations checks whether a tuple's object and relations are valid. If they are not, returns an error.
// If they are, it grabs the type of the tuple's object, and the tuple's relation, and returns its corresponding userset.
func ValidateObjectsRelations(ctx context.Context, backend storage.TypeDefinitionReadBackend, store, modelID string, t *openfgapb.TupleKey) (*openfgapb.Userset, error) {
	if !tuple.IsValidRelation(t.GetRelation()) {
		return nil, &tuple.InvalidTupleError{Reason: "invalid relation", TupleKey: t}
	}
	if !tuple.IsValidObject(t.GetObject()) {
		return nil, &tuple.InvalidObjectFormatError{TupleKey: t}
	}
	objectType, objectID := tuple.SplitObject(t.GetObject())
	if objectType == "" || objectID == "" {
		return nil, &tuple.InvalidObjectFormatError{TupleKey: t}
	}

	ns, err := backend.ReadTypeDefinition(ctx, store, modelID, objectType)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, &tuple.TypeNotFoundError{TypeName: objectType}
		}
		return nil, err
	}
	userset, ok := ns.Relations[t.Relation]
	if !ok {
		return nil, &tuple.RelationNotFoundError{Relation: t.GetRelation(), TypeName: ns.GetType(), TupleKey: t}
	}

	return userset, nil
}

// ValidateUserIfItsAUserset returns an error if the user is a userset and its type/relation don't exist in the model
func ValidateUserIfItsAUserset(ctx context.Context, backend storage.TypeDefinitionReadBackend, store, modelID, user string, tk *openfgapb.TupleKey) error {
	object, relation := tuple.SplitObjectRelation(user)
	objectType, _ := tuple.SplitObject(object)

	typeDefinition, err := backend.ReadTypeDefinition(ctx, store, modelID, objectType)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return &tuple.TypeNotFoundError{TypeName: objectType}
		}

		return err
	}

	_, ok := typeDefinition.Relations[relation]
	if !ok {
		return &tuple.RelationNotFoundError{Relation: relation, TypeName: objectType, TupleKey: tk}
	}
	return nil
}
