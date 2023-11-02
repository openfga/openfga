package listobjectstest

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

type Assertion struct {
	Request          *openfgav1.ListObjectsRequest
	ContextualTuples []*openfgav1.TupleKey `yaml:"contextualTuples"`
	Context          map[string]interface{}
	Expectation      []string
	ErrorCode        int `yaml:"errorCode"` // If ErrorCode is non-zero then we expect that the ListObjects call failed.
}
