package listobjects

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	listobjectstest "github.com/openfga/openfga/internal/test/listobjects"
)

var ttus = []matrixTest{
	{
		Name: "ttus_alg_combined",
		Tuples: []*openfgav1.TupleKey{
			// Satisfies left side `and_ttu`
			{Object: "directs:ttu_alg_1", Relation: "direct_mult_types", User: "user:ttu_anne"},
			{Object: "directs:ttu_alg_1", Relation: "other_rel", User: "user:*"},

			{Object: "ttus:ttu_alg_1", Relation: "mult_parent_types", User: "directs:ttu_alg_1"},
			{Object: "ttus:ttu_alg_1", Relation: "direct_parent", User: "directs:ttu_alg_1"},

			// Satisfies right side of BUT NOT, but relies on condition in the `mult_parent_types` relation
			{Object: "directs:ttu_alg_2", Relation: "other_rel", User: "user:ttu_anne"},
			{Object: "directs:ttu_alg_2", Relation: "direct", User: "user:ttu_anne"},

			// Will exclude due to BUT NOT if condition is passed
			{Object: "ttus:ttu_alg_1", Relation: "mult_parent_types", User: "directs:ttu_alg_2", Condition: xCond},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:ttu_anne",
					Type:     "ttus",
					Relation: "alg_combined",
				},
				Context:     invalidConditionContext,
				Expectation: []string{"ttus:ttu_alg_1"},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:ttu_anne",
					Type:     "ttus",
					Relation: "alg_combined",
				},
				Context:     validConditionContext,
				Expectation: nil,
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
	{
		Name: "ttus_recursive",
		Tuples: []*openfgav1.TupleKey{
			{Object: "ttus:recursive_1", Relation: "ttu_recursive", User: "user:recursive_anne"},
			{Object: "ttus:recursive_2", Relation: "ttu_parent", User: "ttus:recursive_1"},
			{Object: "ttus:recursive_3", Relation: "ttu_parent", User: "ttus:recursive_2"},

			// Connect anne twice, recursive_3 should only return once
			{Object: "ttus:recursive_3", Relation: "ttu_recursive", User: "user:recursive_anne"},

			{Object: "ttus:recursive_2", Relation: "ttu_recursive_public", User: "user:*"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "ttus:recursive_2",
					Type:     "ttus",
					Relation: "ttu_parent",
				},
				Expectation: []string{"ttus:recursive_3"},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:public",
					Type:     "ttus",
					Relation: "ttu_recursive_public",
				},
				Expectation: []string{
					"ttus:recursive_2",
					"ttus:recursive_3",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:recursive_anne",
					Type:     "ttus",
					Relation: "ttu_recursive",
				},
				Expectation: []string{
					"ttus:recursive_1",
					"ttus:recursive_2",
					"ttus:recursive_3",
				},
			},
		},
	},
}
