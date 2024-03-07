package checktest

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

type Assertion struct {
	Tuple            *openfgav1.TupleKey
	ContextualTuples []*openfgav1.TupleKey `json:"contextualTuples"`
	Context          *structpb.Struct
	Expectation      bool
	ErrorCode        int `json:"errorCode"` // If ErrorCode is non-zero then we expect that the check call failed.
}
