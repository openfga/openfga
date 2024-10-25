package batchchecktest

import (
	"fmt"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

type Assertion struct {
	Request *TestBatchCheckRequest
}

type IndividualCheck struct {
	ContextualTuples []*openfgav1.TupleKey
	Context          *structpb.Struct
	TupleKey         *openfgav1.TupleKey
	CorrelationID    string
}

type TestBatchCheckRequest struct {
	Checks []*IndividualCheck
}

func (t *TestBatchCheckRequest) ToProtoRequest() *openfgav1.BatchCheckRequest {
	return &openfgav1.BatchCheckRequest{}
}

func (t *TestBatchCheckRequest) ToString() string {
	return fmt.Sprintf("Justin test ToString for now")
}
