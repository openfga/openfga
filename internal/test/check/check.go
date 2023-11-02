package checktest

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

type Assertion struct {
	Tuple            *openfgav1.TupleKey
	ContextualTuples []*openfgav1.TupleKey `yaml:"contextualTuples"`
	Context          map[string]interface{}
	Expectation      bool
	ErrorCode        int `yaml:"errorCode"` // If ErrorCode is non-zero then we expect that the check call failed.
}
