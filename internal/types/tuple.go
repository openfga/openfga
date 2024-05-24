package types

import (
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

type Object struct {
	Type string
	ID   string
}

type Subject interface {
	GetType() string
	isSubject()
}

type RelationshipTuple struct {
	Object    openfgav1.Object
	Relation  string
	Subject   Subject
	Condition openfgav1.Condition
}

func (r *RelationshipTuple) String() string {
	return fmt.Sprintf("%s#%s@%s", r.Object.String(), r.Relation, r.Subject)
}
