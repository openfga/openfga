package batchchecktest

import (
	"fmt"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	checktest "github.com/openfga/openfga/internal/test/check"
	"google.golang.org/protobuf/types/known/structpb"
)

// TODO: justin use this file

type Assertion struct {
	Request     *TestBatchCheckRequest
	Expectation []Expectation
}

type Expectation struct {
	CorrelationID string `json:"correlationID"`
	Allowed       bool
	//Err
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

func (t *TestBatchCheckRequest) ToProtoRequest() *openfgav1.BatchCheckRequest {
	protoChecks := make([]*openfgav1.BatchCheckItem, 0, len(t.Checks))

	for _, check := range t.Checks {
		item := &openfgav1.BatchCheckItem{
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     check.TupleKey.User,
				Relation: check.TupleKey.Relation,
				Object:   check.TupleKey.Object,
			},
			CorrelationId: check.CorrelationID,
			Context:       check.Context,
			ContextualTuples: &openfgav1.ContextualTupleKeys{
				TupleKeys: check.ContextualTuples,
			},
		}

		protoChecks = append(protoChecks, item)
	}

	return &openfgav1.BatchCheckRequest{
		Checks: protoChecks,
	}
}

func (t *TestBatchCheckRequest) ToString() string {
	return fmt.Sprintf("Justin test ToString for now")
}

// TODO: this should also return the results map
func BatchChecksFromCheckAssertion(assertions []*checktest.Assertion) []*openfgav1.BatchCheckItem {
	checks := make([]*openfgav1.BatchCheckItem, 0, len(assertions))
	for _, assertion := range assertions {
		item := &openfgav1.BatchCheckItem{
			TupleKey: &openfgav1.CheckRequestTupleKey{
				Object:   assertion.Tuple.Object,
				Relation: assertion.Tuple.Relation,
				User:     assertion.Tuple.User,
			},
			Context: assertion.Context,
			ContextualTuples: &openfgav1.ContextualTupleKeys{
				TupleKeys: assertion.ContextualTuples,
			},
		}
		checks = append(checks, item)
	}

	return checks
}

func BatchCheckItemFromAssertion(assertion *checktest.Assertion, correlationId string) *openfgav1.BatchCheckItem {
	item := &openfgav1.BatchCheckItem{
		TupleKey: &openfgav1.CheckRequestTupleKey{
			Object:   assertion.Tuple.Object,
			Relation: assertion.Tuple.Relation,
			User:     assertion.Tuple.User,
		},
		Context: assertion.Context,
		ContextualTuples: &openfgav1.ContextualTupleKeys{
			TupleKeys: assertion.ContextualTuples,
		},
		CorrelationId: correlationId,
	}

	return item
}

func ExpectedResultFromAssertion(assertion *checktest.Assertion) *openfgav1.BatchCheckSingleResult_Allowed {

}
