package checktest

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

type Assertion struct {
	Name             string
	ContextualTuples []*openfgav1.TupleKey `json:"contextualTuples"`
	Context          *structpb.Struct
	// For Check API
	Tuple       *openfgav1.TupleKey
	Expectation bool
	ErrorCode   int `json:"errorCode"`
	// For other APIs
	ListObjectsErrorCode int
	ListUsersErrorCode   int
}
