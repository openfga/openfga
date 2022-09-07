package tuple

import (
	"fmt"

	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

// This file provides validation utility that are shared across different commands

// InvalidTupleError is returned if the tuple is invalid
type InvalidTupleError struct {
	Reason   string
	TupleKey *openfgapb.TupleKey
}

func (i *InvalidTupleError) Error() string {
	return fmt.Sprintf("Invalid tuple '%s'. Reason: %s", i.TupleKey, i.Reason)
}

// InvalidObjectFormatError is returned if the object is invalid
type InvalidObjectFormatError struct {
	TupleKey *openfgapb.TupleKey
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
	TupleKey *openfgapb.TupleKey
	Relation string
	TypeName string
}

func (i *RelationNotFoundError) Error() string {
	return fmt.Sprintf("Relation '%s' not found in type definition '%s' for tuple (%s)", i.Relation, i.TypeName, i.TupleKey.String())
}

// IndirectWriteError is used to categorize errors specific to write check logic
type IndirectWriteError struct {
	Reason   string
	TupleKey *openfgapb.TupleKey
}

func (i *IndirectWriteError) Error() string {
	return fmt.Sprintf("Cannot write tuple '%s'. Reason: %s", i.TupleKey, i.Reason)
}

// ValidateUser returns whether the user is valid.  If not, return error
func ValidateUser(tk *openfgapb.TupleKey) error {
	if !IsValidUser(tk.GetUser()) {
		return &InvalidTupleError{Reason: "the 'user' field must be a non-empty string", TupleKey: tk}
	}
	return nil
}
