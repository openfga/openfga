package batchchecktest

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/protobuf/types/known/structpb"

	checktest "github.com/openfga/openfga/internal/test/check"
)

type BatchCheckTests struct {
	Tests []IndividualTest
}

type IndividualTest struct {
	Name       string
	Model      string
	Tuples     []*openfgav1.TupleKey
	Assertions []*Assertion
}

type Assertion struct {
	Request     *TestBatchCheckRequest
	Expectation []Expectation
}

type Expectation struct {
	CorrelationID string `json:"correlationID"`
	Allowed       bool
	InputError    int `json:"inputError"`
	InternalError int `json:"internalError"`
}

type IndividualCheck struct {
	ContextualTuples []*openfgav1.TupleKey
	Context          *structpb.Struct
	TupleKey         *openfgav1.TupleKey `json:"tupleKey"`
	CorrelationID    string              `json:"correlationID"`
}

type TestBatchCheckRequest struct {
	Checks []*IndividualCheck
}

func BatchCheckItemFromCheckAssertion(assertion *checktest.Assertion, correlationID string) *openfgav1.BatchCheckItem {
	item := &openfgav1.BatchCheckItem{
		TupleKey: &openfgav1.CheckRequestTupleKey{
			Object:   assertion.Tuple.GetObject(),
			Relation: assertion.Tuple.GetRelation(),
			User:     assertion.Tuple.GetUser(),
		},
		Context: assertion.Context,
		ContextualTuples: &openfgav1.ContextualTupleKeys{
			TupleKeys: assertion.ContextualTuples,
		},
		CorrelationId: correlationID,
	}

	return item
}
