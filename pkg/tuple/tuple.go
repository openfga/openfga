package tuple

import (
	"fmt"
	"regexp"
	"strings"

	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type UserType string

const (
	User    UserType = "user"
	UserSet UserType = "userset"
)

var (
	userIDRegex   = regexp.MustCompile(`^[^:#\s]+$`)
	objectRegex   = regexp.MustCompile(`^[^:#\s]+:[^#:\s]+$`)
	userSetRegex  = regexp.MustCompile(`^[^:#\s]+:[^#\s]+#[^:#\s]+$`)
	relationRegex = regexp.MustCompile(`^[^:#@\s]+$`)
)

func NewTupleKey(object, relation, user string) *openfgapb.TupleKey {
	return &openfgapb.TupleKey{
		Object:   object,
		Relation: relation,
		User:     user,
	}
}

// ObjectKey returns the canonical key for the provided Object. The ObjectKey of an object
// is the string 'objectType:objectId'.
func ObjectKey(obj *openfgapb.Object) string {
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

// GetRelation returns the 'relation' portion of an object relation string, which may be empty if the input is malformed
// (or does not contain a relation specifier).
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
	userObj, _ := SplitObjectRelation(user)
	if IsValidObject(userObj) || user == "*" {
		return UserSet
	}
	return User
}

// TupleKeyToString converts a tuple key into its string representation. It assumes the tupleKey is valid
// (i.e. no forbidden characters)
func TupleKeyToString(tk *openfgapb.TupleKey) string {
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
