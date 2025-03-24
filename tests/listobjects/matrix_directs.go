package listobjects

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	listobjectstest "github.com/openfga/openfga/internal/test/listobjects"
)

var directs = []matrixTest{
	{
		Name: "directs_direct_assignment",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs:direct_1_1", Relation: "direct", User: "user:direct_1"},
			{Object: "directs:direct_1_2", Relation: "direct", User: "user:direct_1"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:direct_1",
					Type:     "directs",
					Relation: "direct",
				},

				Expectation: []string{"directs:direct_1_1", "directs:direct_1_2"},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:direct_no_such_user",
					Type:     "directs",
					Relation: "direct",
				},

				Expectation: []string{},
			},
		},
	},
}
