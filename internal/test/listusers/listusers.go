package listuserstest

import (
	"fmt"
	"strings"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/openfga/openfga/pkg/tuple"
)

type Assertion struct {
	Request               *TestListUsersRequest
	TemporarilySkipReason string                // Temporarily skip test until functionality is fixed
	ContextualTuples      []*openfgav1.TupleKey `json:"contextualTuples"`
	Context               *structpb.Struct
	Expectation           []string
	ErrorCode             int `json:"errorCode"` // If ErrorCode is non-zero then we expect that the ListUsers call failed.
}

type TestListUsersRequest struct {
	Object   string
	Relation string
	Filters  []string `json:"filters"`
}

func (t *TestListUsersRequest) ToString() string {
	return fmt.Sprintf("object=%s, relation=%s, filters=%v", t.Object, t.Relation, strings.Join(t.Filters, ", "))
}

func FromProtoResponse(r *openfgav1.ListUsersResponse) []string {
	var users []string
	for _, user := range r.GetUsers() {
		users = append(users, tuple.UserProtoToString(user))
	}
	return users
}

func (t *TestListUsersRequest) ToProtoRequest() *openfgav1.ListUsersRequest {
	var protoFilters []*openfgav1.UserTypeFilter

	for _, filterString := range t.Filters {
		protoFilters = append(protoFilters, toProtoFilter(filterString))
	}

	objectType, objectID := tuple.SplitObject(t.Object)
	return &openfgav1.ListUsersRequest{
		Object: &openfgav1.Object{
			Type: objectType,
			Id:   objectID,
		},
		Relation:    t.Relation,
		UserFilters: protoFilters,
	}
}

func toProtoFilter(user string) *openfgav1.UserTypeFilter {
	userObjType, userRel := tuple.SplitObjectRelation(user)

	sourceUserRef := openfgav1.UserTypeFilter{
		Type: userObjType,
	}

	if userRel != "" {
		sourceUserRef.Relation = userRel
	}
	return &sourceUserRef
}
