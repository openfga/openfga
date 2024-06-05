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
	isSubject() bool
}

type RelationshipTuple struct {
	Object    openfgav1.Object
	Relation  string
	Subject   Subject
	Condition openfgav1.Condition
}

type RelationshipSubject struct {
	Type string
}

func (rs RelationshipSubject) GetType() string {
	return rs.Type
}

func (rs RelationshipSubject) isSubject() bool {
	return rs.Type != ""
}

func (r *RelationshipTuple) String() string {
	return fmt.Sprintf("%s:%s#%s@%s", r.Object.Type, r.Object.Id, r.Relation, r.Subject.GetType())
}
