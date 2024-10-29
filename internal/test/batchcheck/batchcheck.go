package batchchecktest

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	checktest "github.com/openfga/openfga/internal/test/check"
)

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
