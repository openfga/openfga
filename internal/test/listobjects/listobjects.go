package listobjectstest

import (
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

type Assertion struct {
	Request          *openfgav1.ListObjectsRequest
	ContextualTuples []*openfgav1.TupleKey `json:"contextualTuples"`
	Context          *structpb.Struct
	Expectation      []string
	ErrorCode        int `json:"errorCode"` // If ErrorCode is non-zero then we expect that the ListObjects call failed.
}
