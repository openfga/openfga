package tuple

import (
	"context"
	"fmt"

	"github.com/go-errors/errors"
	"github.com/openfga/openfga/pkg/utils"
	"github.com/openfga/openfga/storage"
	"go.buf.build/openfga/go/openfga/api/openfga"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

// This file provides validation utility that are shared across different commands

// InvalidTupleError is returned if the tuple is invalid
type InvalidTupleError struct {
	Reason   string
	TupleKey *openfga.TupleKey
}

func (i *InvalidTupleError) Error() string {
	return fmt.Sprintf("Invalid tuple '%s'. Reason: %s", i.TupleKey, i.Reason)
}

// InvalidObjectFormatError is returned if the object is invalid
type InvalidObjectFormatError struct {
	TupleKey *openfga.TupleKey
}

func (i *InvalidObjectFormatError) Error() string {
	return fmt.Sprintf("Invalid object format '%s'.", i.TupleKey.String())
}

// TypeNotFoundError is returned if type is not found
type TypeNotFoundError struct {
	TypeName string
}

func (i *TypeNotFoundError) Error() string {
	return fmt.Sprintf("Type not found for %s", i.TypeName)
}

// RelationNotFoundError is returned if the relation is not found
type RelationNotFoundError struct {
	TupleKey *openfga.TupleKey
	Relation string
	TypeName string
}

func (i *RelationNotFoundError) Error() string {
	return fmt.Sprintf("Relation '%s' not found in type definition '%s' for tuple (%s)", i.Relation, i.TypeName, i.TupleKey.String())
}

// ValidateTuple returns whether a *openfga.TupleKey is valid
func ValidateTuple(ctx context.Context, backend storage.TypeDefinitionReadBackend, store, authorizationModelID string, tk *openfga.TupleKey, dbCallsCounter utils.DBCallCounter) (*openfgav1pb.Userset, error) {
	if !IsValidUser(tk.GetUser()) {
		return nil, &InvalidTupleError{Reason: "missing user", TupleKey: tk}
	}
	return ValidateObjectsRelations(ctx, backend, store, authorizationModelID, tk, dbCallsCounter)
}

// ValidateObjectsRelations returns whether a tuple's object and relations are valid
func ValidateObjectsRelations(ctx context.Context, backend storage.TypeDefinitionReadBackend, store, authorizationModelId string, t *openfga.TupleKey, dbCallsCounter utils.DBCallCounter) (*openfgav1pb.Userset, error) {
	if !IsValidRelation(t.GetRelation()) {
		return nil, &InvalidTupleError{Reason: "invalid relation", TupleKey: t}
	}
	if !IsValidObject(t.GetObject()) {
		return nil, &InvalidObjectFormatError{TupleKey: t}
	}
	objectType, objectID := SplitObject(t.GetObject())
	if objectType == "" || objectID == "" {
		return nil, &InvalidObjectFormatError{TupleKey: t}
	}

	dbCallsCounter.AddReadCall()
	ns, err := backend.ReadTypeDefinition(ctx, store, authorizationModelId, objectType)
	if err != nil {
		if errors.Is(err, storage.NotFound) {
			return nil, &TypeNotFoundError{TypeName: objectType}
		}
		return nil, err
	}
	userset, ok := ns.Relations[t.Relation]
	if !ok {
		return nil, &RelationNotFoundError{Relation: t.GetRelation(), TypeName: ns.GetType(), TupleKey: t}
	}
	return userset, nil
}
