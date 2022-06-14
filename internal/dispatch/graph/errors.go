package graph

import (
	"fmt"

	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type ErrRelationNotFound struct {
	TupleKey *openfgapb.TupleKey
	Relation string
	TypeName string
}

func (e *ErrRelationNotFound) Error() string {
	return fmt.Sprintf("Relation '%s' not found in type definition '%s' for tuple (%s)", e.Relation, e.TypeName, e.TupleKey.String())
}

func NewRelationNotFoundErr(tk *openfgapb.TupleKey, relation, objectType string) error {
	return &ErrRelationNotFound{
		TupleKey: tk,
		Relation: relation,
		TypeName: objectType,
	}
}

// ErrTypeNotFound is returned if the object type definition is not found.
type ErrTypeNotFound struct {
	TypeName string
}

func (e *ErrTypeNotFound) Error() string {
	return fmt.Sprintf("Type not found for %s", e.TypeName)
}

func NewTypeNotFoundErr(typeName string) error {
	return &ErrTypeNotFound{
		TypeName: typeName,
	}
}
