package checktest

import (
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

type Assertion struct {
	Name             string
	ContextualTuples []*openfgav1.TupleKey `json:"contextualTuples"`
	Context          *structpb.Struct
	// For Check API
	Tuple       *openfgav1.TupleKey
	Expectation bool
	ErrorCode   int `json:"errorCode"`
	Error       error
	// For other APIs
	ListObjectsError error
	ListUsersError   error
}
