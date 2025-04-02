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
	{
		Name: "ttus_recursive_alg_combined_w2",
		Tuples: []*openfgav1.TupleKey{
			// Direct, should always return
			{Object: "ttus:recursive_w2_1", Relation: "ttu_recursive_alg_combined_w2", User: "user:w2_anne"},

			// Satisfies the ttu_recursive_alg_combined relation needed in ttu_parent links below iff condition valid
			{Object: "ttus:recursive_w2_1c", Relation: "user_rel2", User: "user:w2_anne", Condition: xCond},
			{Object: "ttus:recursive_w2_1c", Relation: "user_rel3", User: "user:w2_anne"},

			// Recursive chain, will return as well if condition above is valid
			{Object: "ttus:recursive_w2_2", Relation: "ttu_parent", User: "ttus:recursive_w2_1c"},
			{Object: "ttus:recursive_w2_3", Relation: "ttu_parent", User: "ttus:recursive_w2_2"},

			// This satisfies the rightmost AND iff the condition is valid
			{Object: "ttus:recursive_w2_a", Relation: "user_rel2", User: "user:w2_bob", Condition: xCond},
			{Object: "directs:recursive_w2_a", Relation: "direct", User: "user:w2_bob"},
			{Object: "ttus:recursive_w2_a", Relation: "direct_parent", User: "directs:recursive_w2_a"},

			// Satisfies the ttu_recursive_alg_combined relation also, testing wildcard with condition
			{Object: "ttus:recursive_w2_ab", Relation: "user_rel2", User: "user:w2_charlie"},
			{Object: "ttus:recursive_w2_ab", Relation: "user_rel3", User: "user:*", Condition: xCond},
			{Object: "ttus:recursive_w2_abc", Relation: "ttu_parent", User: "ttus:recursive_w2_ab"},
			{Object: "ttus:recursive_w2_abcd", Relation: "ttu_parent", User: "ttus:recursive_w2_abc"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:w2_anne",
					Type:     "ttus",
					Relation: "ttu_recursive_alg_combined_w2",
				},
				Expectation: []string{
					"ttus:recursive_w2_1",
					"ttus:recursive_w2_2",
					"ttus:recursive_w2_3",
				},
				Context: validConditionContext,
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:w2_anne",
					Type:     "ttus",
					Relation: "ttu_recursive_alg_combined_w2",
				},
				// only the direct [user] relation returns when condition fails
				Expectation: []string{"ttus:recursive_w2_1"},
				Context:     invalidConditionContext,
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:w2_bob",
					Type:     "ttus",
					Relation: "ttu_recursive_alg_combined_w2",
				},
				Context:     invalidConditionContext,
				Expectation: nil, // nil due to failed condition
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:w2_bob",
					Type:     "ttus",
					Relation: "ttu_recursive_alg_combined_w2",
				},
				Context:     validConditionContext,
				Expectation: []string{"ttus:recursive_w2_a"},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:w2_charlie",
					Type:     "ttus",
					Relation: "ttu_recursive_alg_combined_w2",
				},
				Context:     validConditionContext,
				Expectation: []string{"ttus:recursive_w2_abc", "ttus:recursive_w2_abcd"},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:w2_charlie",
					Type:     "ttus",
					Relation: "ttu_recursive_alg_combined_w2",
				},
				Context:     invalidConditionContext,
				Expectation: nil,
			},
		},
	},
}
