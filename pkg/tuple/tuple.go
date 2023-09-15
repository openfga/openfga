// Package tuple contains code to manipulate tuples and errors related to tuples.
package tuple

import (
	"fmt"
	"regexp"
	"strings"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

type UserType string

const (
	User    UserType = "user"
	UserSet UserType = "userset"
)

const Wildcard = "*"

var (
	userIDRegex   = regexp.MustCompile(`^[^:#\s]+$`)
	objectRegex   = regexp.MustCompile(`^[^:#\s]+:[^#:\s]+$`)
	userSetRegex  = regexp.MustCompile(`^[^:#\s]+:[^#\s]+#[^:#\s]+$`)
	relationRegex = regexp.MustCompile(`^[^:#@\s]+$`)
)

func ConvertCheckRequestTupleKeyToTupleKey(tk *openfgav1.CheckRequestTupleKey) *openfgav1.TupleKey {
	return &openfgav1.TupleKey{
		Object:   tk.GetObject(),
		Relation: tk.GetRelation(),
		User:     tk.GetUser(),
	}
}

func ConvertReadRequestTupleKeyToTupleKey(tk *openfgav1.ReadRequestTupleKey) *openfgav1.TupleKey {
	return &openfgav1.TupleKey{
		Object:   tk.GetObject(),
		Relation: tk.GetRelation(),
		User:     tk.GetUser(),
	}
}

func NewTupleKey(object, relation, user string) *openfgav1.TupleKey {
	return &openfgav1.TupleKey{
		Object:   object,
		Relation: relation,
		User:     user,
	}
}

func ConvertTupleKeyToWriteTupleKey(tk *openfgav1.TupleKey) *openfgav1.WriteRequestTupleKey {
	return &openfgav1.WriteRequestTupleKey{
		Object:   tk.GetObject(),
		Relation: tk.GetRelation(),
		User:     tk.GetUser(),
	}
}

func ConvertWriteRequestTupleKeyToTupleKey(tk *openfgav1.WriteRequestTupleKey) *openfgav1.TupleKey {
	return &openfgav1.TupleKey{
		Object:   tk.GetObject(),
		Relation: tk.GetRelation(),
		User:     tk.GetUser(),
	}
}

func ConvertWriteRequestsTupleKeyToTupleKeys(tks []*openfgav1.WriteRequestTupleKey) []*openfgav1.TupleKey {
	result := make([]*openfgav1.TupleKey, 0, len(tks))
	for _, tk := range tks {
		result = append(result, ConvertWriteRequestTupleKeyToTupleKey(tk))
	}
	return result
}

func ConvertTupleKeysToWriteRequestTupleKeys(tks []*openfgav1.TupleKey) []*openfgav1.WriteRequestTupleKey {
	result := make([]*openfgav1.WriteRequestTupleKey, 0, len(tks))
	for _, tk := range tks {
		result = append(result, ConvertTupleKeyToWriteTupleKey(tk))
	}
	return result
}

func NewCheckRequestTupleKey(object, relation, user string) *openfgav1.CheckRequestTupleKey {
	return &openfgav1.CheckRequestTupleKey{
		Object:   object,
		Relation: relation,
		User:     user,
	}
}

func NewExpandRequestTupleKey(object, relation string) *openfgav1.ExpandRequestTupleKey {
	return &openfgav1.ExpandRequestTupleKey{
		Object:   object,
		Relation: relation,
	}
}

// ObjectKey returns the canonical key for the provided Object. The ObjectKey of an object
// is the string 'objectType:objectId'.
func ObjectKey(obj *openfgav1.Object) string {
	return fmt.Sprintf("%s:%s", obj.Type, obj.Id)
}

// SplitObject splits an object into an objectType and an objectID. If no type is present, it returns the empty string
// and the original object.
func SplitObject(object string) (string, string) {
	switch i := strings.IndexByte(object, ':'); i {
	case -1:
		return "", object
	case len(object) - 1:
		return object[0:i], ""
	default:
		return object[0:i], object[i+1:]
	}
}

func BuildObject(objectType, objectID string) string {
	return fmt.Sprintf("%s:%s", objectType, objectID)
}

// GetObjectRelationAsString returns a string like "object#relation". If there is no relation it returns "object"
func GetObjectRelationAsString(objectRelation *openfgav1.ObjectRelation) string {
	if objectRelation.GetRelation() != "" {
		return fmt.Sprintf("%s#%s", objectRelation.GetObject(), objectRelation.GetRelation())
	}
	return objectRelation.GetObject()
}

// SplitObjectRelation splits an object relation string into an object ID and relation name. If no relation is present,
// it returns the original string and an empty relation.
func SplitObjectRelation(objectRelation string) (string, string) {
	switch i := strings.LastIndexByte(objectRelation, '#'); i {
	case -1:
		return objectRelation, ""
	case len(objectRelation) - 1:
		return objectRelation[0:i], ""
	default:
		return objectRelation[0:i], objectRelation[i+1:]
	}
}

// GetType returns the type from a supplied Object identifier or an empty string if the object id does not contain a
// type.
func GetType(objectID string) string {
	t, _ := SplitObject(objectID)
	return t
}

// GetRelation returns the 'relation' portion of an object relation string (e.g. `object#relation`), which may be empty if the input is malformed
// (or does not contain a relation).
func GetRelation(objectRelation string) string {
	_, relation := SplitObjectRelation(objectRelation)
	return relation
}

// IsObjectRelation returns true if the given string specifies a valid object and relation.
func IsObjectRelation(userset string) bool {
	return GetType(userset) != "" && GetRelation(userset) != ""
}

// ToObjectRelationString formats an object/relation pair as an object#relation string. This is the inverse of
// SplitObjectRelation.
func ToObjectRelationString(object, relation string) string {
	return fmt.Sprintf("%s#%s", object, relation)
}

// GetUserTypeFromUser returns the type of user (userset or user).
func GetUserTypeFromUser(user string) UserType {
	if IsObjectRelation(user) || IsWildcard(user) {
		return UserSet
	}
	return User
}

// TupleKeyToString converts a tuple key into its string representation. It assumes the tupleKey is valid
// (i.e. no forbidden characters)
func TupleKeyToString(tk *openfgav1.TupleKey) string {
	return fmt.Sprintf("%s#%s@%s", tk.GetObject(), tk.GetRelation(), tk.GetUser())
}

// IsValidObject determines if a string s is a valid object. A valid object contains exactly one `:` and no `#` or spaces.
func IsValidObject(s string) bool {
	return objectRegex.MatchString(s)
}

// IsValidRelation determines if a string s is a valid relation. This means it does not contain any `:`, `#`, or spaces.
func IsValidRelation(s string) bool {
	return relationRegex.MatchString(s)
}

// IsValidUser determines if a string is a valid user. A valid user contains at most one `:`, at most one `#` and no spaces.
func IsValidUser(user string) bool {
	if strings.Count(user, ":") > 1 || strings.Count(user, "#") > 1 {
		return false
	}
	if user == "*" || userIDRegex.MatchString(user) || objectRegex.MatchString(user) || userSetRegex.MatchString(user) {
		return true
	}

	return false
}

// IsWildcard returns true if the string 's' could be interpreted as a typed or untyped wildcard (e.g. '*' or 'type:*')
func IsWildcard(s string) bool {
	return s == Wildcard || IsTypedWildcard(s)
}

// IsTypedWildcard returns true if the string 's' is a typed wildcard. A typed wildcard
// has the form 'type:*'.
func IsTypedWildcard(s string) bool {
	if IsValidObject(s) {
		_, id := SplitObject(s)
		if id == Wildcard {
			return true
		}
	}

	return false
}
