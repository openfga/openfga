package listuserstest

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
)

type Assertion struct {
	Request          *TestListUsersRequest
	ContextualTuples []*openfgav1.TupleKey `json:"contextualTuples"`
	Context          *structpb.Struct
	Expectation      []string
	ErrorCode        int `json:"errorCode"` // If ErrorCode is non-zero then we expect that the ListUsers call failed.
}

type TestListUsersRequest struct {
	Object   string
	Relation string
	Filters  []string `json:"filters"`
}

func (t *TestListUsersRequest) ToString() string {
	return fmt.Sprintf("object=%s, relation=%s, filters=%v", t.Object, t.Relation, strings.Join(t.Filters, ", "))
}

func FromUsersProto(r []*openfgav1.User) []string {
	var users []string
	for _, user := range r {
		users = append(users, tuple.UserProtoToString(user))
	}
	return users
}

func (t *TestListUsersRequest) ToProtoRequest() *openfgav1.ListUsersRequest {
	var userTypeFilters []*openfgav1.UserTypeFilter

	for _, filterString := range t.Filters {
		userTypeFilters = append(userTypeFilters, toProtoUserTypeFilter(filterString))
	}

	objectType, objectID := tuple.SplitObject(t.Object)
	return &openfgav1.ListUsersRequest{
		Object: &openfgav1.Object{
			Type: objectType,
			Id:   objectID,
		},
		Relation:    t.Relation,
		UserFilters: userTypeFilters,
	}
}

// toProtoUserTypeFilter returns the protobuf representation of a UserFilter. It is
// a helper to convert a string-represented UserFilter to a protobuf.
func toProtoUserTypeFilter(userFilter string) *openfgav1.UserTypeFilter {
	userObjType, userRel := tuple.SplitObjectRelation(userFilter)

	sourceUserRef := openfgav1.UserTypeFilter{
		Type: userObjType,
	}

	if userRel != "" {
		sourceUserRef.Relation = userRel
	}
	return &sourceUserRef
}
