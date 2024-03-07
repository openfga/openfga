package listuserstest

import (
	"strings"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/protobuf/types/known/structpb"

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
	Filters  []*string `json:"filters"`
}

func (t *TestListUsersRequest) ConvertTestListUsersRequest() *openfgav1.ListUsersRequest {
	objectType, objectID := tuple.SplitObject(t.Object)

	var convertedFilters []*openfgav1.RelationReference

	for _, filterString := range t.Filters {
		parts := strings.Split(*filterString, " ")
		if len(parts) != 1 {
			panic("unexpected filter")
		}

		userDef := parts[0]
		convertedFilters = append(convertedFilters, parseRelationReference(userDef))
	}

	return &openfgav1.ListUsersRequest{
		Object: &openfgav1.Object{
			Type: objectType,
			Id:   objectID,
		},
		Relation: t.Relation,
		Filters:  convertedFilters,
	}
}

func parseRelationReference(user string) *openfgav1.RelationReference {
	userObjType, userRel := tuple.SplitObjectRelation(user)

	sourceUserRef := openfgav1.RelationReference{
		Type: userObjType,
	}

	if userRel != "" {
		sourceUserRef.RelationOrWildcard = &openfgav1.RelationReference_Relation{
			Relation: userRel,
		}
	}
	return &sourceUserRef
}
