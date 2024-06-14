package types

import (
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
)

// Object represents an FGA object.
//
// See https://openfga.dev/docs/concepts#what-is-an-object
type Object struct {
	Type string
	ID   string
}

var _ Subject = (*Object)(nil)

// GetType returns the object's assigned type.
func (o Object) GetType() string {
	return o.Type
}

// String returns this object as a string formatted as 'type:id'.
func (o Object) String() string {
	return fmt.Sprintf("%s:%s", o.GetType(), o.ID)
}

func (o Object) isSubject() {}

// Userset reprents a set of FGA users/subjects.
//
// See https://openfga.dev/docs/concepts#what-is-a-user (specifically for `userset`)
type Userset struct {
	Type     string
	ID       string
	Relation string
}

var _ Subject = (*Userset)(nil)

// GetType returns the userset's assigned type.
func (u Userset) GetType() string {
	return u.Type
}

// String returns this userset as a string formatted as 'type:id#relation'.
func (u Userset) String() string {
	return fmt.Sprintf("%s:%s#%s", u.GetType(), u.ID, u.Relation)
}

func (u Userset) isSubject() {}

// TypedWildcardSubject represents a publicly assignable subject that denotes any
// user/subject of the specified type is included in the set of subjects denoted
// by the typed wildcard.
type TypedWildcardSubject struct {
	Type string
}

var _ Subject = (*TypedWildcardSubject)(nil)

// String returns this typed wildcard as a string formatted as 'type:*'.
//
// '*' is used to denote the wildcard representing a type restriction granting
// public access.
func (t TypedWildcardSubject) String() string {
	return tuple.TypedPublicWildcard(t.Type)
}

// GetType returns the typed wildcard's assigned type.
func (t TypedWildcardSubject) GetType() string {
	return t.Type
}

func (t TypedWildcardSubject) isSubject() {}

// Subject represents an FGA user/subject.
//
// A Subject can be any one of an [Object], [TypedWildcardSubject], or
// [Userset].
//
// See https://openfga.dev/docs/concepts#what-is-a-user
type Subject interface {
	String() string
	GetType() string
	isSubject()
}

// SubjectFromString parses the subjectStr and converts it to the appropriate
// Subject reference.
func SubjectFromString(subjectStr string) Subject {
	subject, subjectRelation := tuple.SplitObjectRelation(subjectStr)
	subjectType, subjectID := tuple.SplitObject(subject)
	return SubjectFromParts(subjectType, subjectID, subjectRelation)
}

// SubjectFromParts returns the appropriate Subject reference provided the individual
// parts of the subject (subjectType, subjectID, subjectRelation).
func SubjectFromParts(subjectType, subjectID, subjectRelation string) Subject {
	if subjectRelation == "" && subjectID != tuple.Wildcard {
		return &Object{
			Type: subjectType,
			ID:   subjectID,
		}
	}

	if subjectRelation == "" && subjectID == tuple.Wildcard {
		return &TypedWildcardSubject{
			Type: subjectType,
		}
	}

	return &Userset{
		Type:     subjectType,
		ID:       subjectID,
		Relation: subjectRelation,
	}
}

// RelationshipTuple represents an explicit relationship between some [Object] and some [Subject]
// through a specific relation.
//
// A RelationshipTuple can be conditioned upon some [Condition] which establishes that the relationship
// is only applicable if the condition is also met.
type RelationshipTuple struct {
	Object    Object
	Relation  string
	Subject   Subject
	Condition openfgav1.Condition
}

// String returns this RelationshipTuple as a formatted string of the form 'object#relation@subject' where
// each parameter (object, relation, subject) is convereted to its String representation, respectively.
func (r *RelationshipTuple) String() string {
	return fmt.Sprintf("%s#%s@%s", r.Object.String(), r.Relation, r.Subject)
}
