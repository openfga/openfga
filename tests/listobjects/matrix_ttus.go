package listobjects

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	listobjectstest "github.com/openfga/openfga/internal/test/listobjects"
)

var ttus = []matrixTest{
	{
		Name: "ttus_alg_combined",
		Tuples: []*openfgav1.TupleKey{
			// Easy happy path
			{Object: "directs:ttu_alg_1", Relation: "direct", User: "user:ttu_anne"},
			{Object: "directs:ttu_alg_1", Relation: "other_rel", User: "user:ttu_anne"},
			{Object: "ttus:ttu_alg_1", Relation: "direct_parent", User: "directs:ttu_alg_1"},
			{Object: "ttus:ttu_alg_1", Relation: "mult_parent_types", User: "directs:ttu_alg_1"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:ttu_anne",
					Type:     "ttus",
					Relation: "alg_combined",
				},
				Expectation: nil, // TODO: anne should be able to see ttu_alg_1, check setup
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "directs:ttu_alg_1",
					Type:     "ttus",
					Relation: "mult_parent_types",
				},
				Expectation: []string{"ttus:ttu_alg_1"},
			},
		},
	},
}
