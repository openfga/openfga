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

			// Satisfies left side of BUT NOT "AND ttu_other_rel" for bob
			{Object: "directs:ttu_alg_b", Relation: "other_rel", User: "user:ttu_bob"},
			{Object: "ttus:ttu_alg_2", Relation: "mult_parent_types", User: "directs:ttu_alg_b"},

			// Satisfies "OR alg_combined from mult_parent_types" iff condition is valid
			{Object: "directs:ttu_alg_c", Relation: "other_rel", User: "user:ttu_bob"},
			{Object: "directs:ttu_alg_c", Relation: "direct_mult_types", User: "user:ttu_bob"},
			{Object: "ttus:ttu_alg_2", Relation: "mult_parent_types", User: "directs:ttu_alg_c", Condition: xCond},
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
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:ttu_bob",
					Type:     "ttus",
					Relation: "alg_combined",
				},
				Context:     validConditionContext,
				Expectation: []string{"ttus:ttu_alg_2"},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:ttu_bob",
					Type:     "ttus",
					Relation: "alg_combined",
				},
				Context:     invalidConditionContext,
				Expectation: nil,
			},
		},
	},
}
