package tuple

import (
	"fmt"

	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

// InvalidTupleError is returned if the tuple is invalid
type InvalidTupleError struct {
	Cause    error
	TupleKey *openfgapb.TupleKey
}

func (i *InvalidTupleError) Error() string {
	return fmt.Sprintf("Invalid tuple '%s'. Reason: %s", TupleKeyToString(i.TupleKey), i.Cause)
}

func (i *InvalidTupleError) Is(target error) bool {
	_, ok := target.(*InvalidTupleError)
	return ok
}

// InvalidObjectFormatError is returned if the object is invalid
type InvalidObjectFormatError struct {
	TupleKey *openfgapb.TupleKey
}

func (i *InvalidObjectFormatError) Error() string {
	return fmt.Sprintf("Invalid object format '%s'.", TupleKeyToString(i.TupleKey))
}

func (i *InvalidObjectFormatError) Is(target error) bool {
	_, ok := target.(*InvalidObjectFormatError)
	return ok
}

// TypeNotFoundError is returned if type is not found
type TypeNotFoundError struct {
	TypeName string
}

func (i *TypeNotFoundError) Error() string {
	return fmt.Sprintf("type '%s' not found", i.TypeName)
}

func (i *TypeNotFoundError) Is(target error) bool {
	_, ok := target.(*TypeNotFoundError)
	return ok
}

// RelationNotFoundError is returned if the relation is not found
type RelationNotFoundError struct {
	TupleKey *openfgapb.TupleKey
	Relation string
	TypeName string
}

func (i *RelationNotFoundError) Error() string {
	msg := fmt.Sprintf("relation '%s#%s' not found", i.TypeName, i.Relation)
	if i.TupleKey != nil {
		msg += fmt.Sprintf(" for tuple '%s'", TupleKeyToString(i.TupleKey))
	}

	return msg
}

func (i *RelationNotFoundError) Is(target error) bool {
	_, ok := target.(*RelationNotFoundError)
	return ok
}

// IndirectWriteError is used to categorize errors specific to write check logic
type IndirectWriteError struct {
	Reason   string
	TupleKey *openfgapb.TupleKey
}

func (i *IndirectWriteError) Error() string {
	return fmt.Sprintf("Cannot write tuple '%s'. Reason: %s", TupleKeyToString(i.TupleKey), i.Reason)
}
