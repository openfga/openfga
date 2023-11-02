package listobjectstest

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

type Assertion struct {
	Request          *openfgav1.ListObjectsRequest
	ContextualTuples []*openfgav1.TupleKey `yaml:"contextualTuples"`
	Context          *structpb.Struct
	Expectation      []string
	ErrorCode        int `yaml:"errorCode"` // If ErrorCode is non-zero then we expect that the ListObjects call failed.
}
