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
	{
		Name: "ttus_multiple_parent_exclusion_intersection",
		Tuples: []*openfgav1.TupleKey{

			// direct assignation to alg_inline
			{Object: "ttus:ttu1", Relation: "alg_inline", User: "user:bob"},
			{Object: "ttus:ttu3", Relation: "alg_inline", User: "user:bob"},
			{Object: "ttus:ttu2", Relation: "alg_inline", User: "user:*"},
			{Object: "ttus:ttu3", Relation: "alg_inline", User: "user:*"},
			{Object: "ttus:ttu4", Relation: "alg_inline", User: "user:*"},

			// user_rel4 is an or of user_rel1 direct assignation

			{Object: "ttus:ttu2", Relation: "user_rel1", User: "user:bob"},
			{Object: "ttus:ttu3", Relation: "user_rel1", User: "user:bob"},
			{Object: "ttus:ttu2", Relation: "user_rel1", User: "user:*"},
			{Object: "ttus:ttu1", Relation: "user_rel1", User: "user:*"},

			// sets the ttu parents
			{Object: "ttus:ttu1", Relation: "direct_parent", User: "directs:d1"},
			{Object: "ttus:ttu2", Relation: "direct_parent", User: "directs:d2"},
			{Object: "ttus:ttu3", Relation: "direct_parent", User: "directs:d3"},
			{Object: "ttus:ttu4", Relation: "direct_parent", User: "directs:d4"},

			// set assignation for bob
			{Object: "directs:d1", Relation: "direct", User: "user:bob"},
			{Object: "directs:d2", Relation: "direct", User: "user:bob"},
			{Object: "directs:d3", Relation: "direct", User: "user:bob"},
			{Object: "directs:d4", Relation: "direct", User: "user:bob"},

			// sets the ttu parents
			{Object: "ttus:ttu3", Relation: "mult_parent_types", User: "directs:d3"},
			{Object: "ttus:ttu4", Relation: "mult_parent_types", User: "directs:d4"},
			{Object: "ttus:ttu1", Relation: "mult_parent_types", User: "directs-employee:mdd1"},
			{Object: "ttus:ttu2", Relation: "mult_parent_types", User: "directs-employee:mdd2"},

			{Object: "directs:d4", Relation: "other_rel", User: "user:bob"},
			{Object: "directs:d3", Relation: "other_rel", User: "user:*"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:bob",
					Type:     "ttus",
					Relation: "alg_inline",
				},
				Expectation: []string{
					"ttus:ttu1",
					"ttus:ttu2",
				},
				Context: validConditionContext,
			},
		},
	},
	{
		Name: "ttus_recursive_alg_combined_oneline",
		Tuples: []*openfgav1.TupleKey{
			// This satisfies rel2 AND rel3 iff condition passes
			{Object: "ttus:oneline_public_1", Relation: "user_rel2", User: "user:oneline_anne"},
			{Object: "ttus:oneline_public_1", Relation: "user_rel3", User: "user:*", Condition: xCond},
			{Object: "ttus:oneline_public_2", Relation: "ttu_parent", User: "ttus:oneline_public_1"},

			// oneline_public_3 should not return below
			{Object: "ttus:oneline_public_2", Relation: "ttu_parent", User: "ttus:oneline_public_3"},

			// This also satisfies rel2 and rel3 but puts the condition on the other rel
			{Object: "ttus:oneline_public_1", Relation: "user_rel2", User: "user:oneline_bob", Condition: xCond},
			{Object: "ttus:oneline_public_1", Relation: "user_rel3", User: "user:oneline_bob"},

			// Testing user_rel1 part of OR
			{Object: "ttus:oneline_rel1_1", Relation: "ttu_parent", User: "ttus:oneline_rel1_2"},
			{Object: "ttus:oneline_rel1_2", Relation: "ttu_parent", User: "ttus:oneline_rel1_3"},
			{Object: "ttus:oneline_rel1_3", Relation: "ttu_parent", User: "ttus:oneline_rel1_4"},

			// Attach user on _3, which is a parent of _2 and _1 (but not _4)
			{Object: "ttus:oneline_rel1_3", Relation: "user_rel1", User: "user:oneline_charlie"},

			// simplest, direct relation
			{Object: "ttus:oneline_direct", Relation: "ttu_recursive_alg_combined_oneline", User: "user:oneline_direct"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:oneline_anne",
					Type:     "ttus",
					Relation: "ttu_recursive_alg_combined_oneline",
				},
				Context: validConditionContext,
				// With valid condition, should get public_1 and public_2
				Expectation: []string{"ttus:oneline_public_1", "ttus:oneline_public_2"},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:oneline_anne",
					Type:     "ttus",
					Relation: "ttu_recursive_alg_combined_oneline",
				},
				Context: invalidConditionContext,
				// With invalid condition, should get neither
				Expectation: nil,
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:oneline_bob",
					Type:     "ttus",
					Relation: "ttu_recursive_alg_combined_oneline",
				},
				Context: validConditionContext,
				// With valid condition, should get both
				Expectation: []string{"ttus:oneline_public_1", "ttus:oneline_public_2"},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:oneline_bob",
					Type:     "ttus",
					Relation: "ttu_recursive_alg_combined_oneline",
				},
				Context: invalidConditionContext,
				// With invalid condition, should get neither
				Expectation: nil,
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:oneline_charlie",
					Type:     "ttus",
					Relation: "ttu_recursive_alg_combined_oneline",
				},
				// ttus:oneline_rel1_4 should not appear here
				Expectation: []string{
					"ttus:oneline_rel1_1",
					"ttus:oneline_rel1_2",
					"ttus:oneline_rel1_3",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:oneline_direct",
					Type:     "ttus",
					Relation: "ttu_recursive_alg_combined_oneline",
				},
				Expectation: []string{"ttus:oneline_direct"},
			},
		},
	},
	{
		Name: "duplicate_ttu_parents",
		Tuples: []*openfgav1.TupleKey{
			{Object: "ttus:duplicate_parent", Relation: "mult_parent_types", User: "directs:duplicate_parent"},
			{Object: "directs:duplicate_parent", Relation: "direct", User: "user:duplicate_parent_anne"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:duplicate_parent_anne",
					Type:     "ttus",
					Relation: "duplicate_ttu",
				},
				Expectation: []string{"ttus:duplicate_parent"},
			},
		},
	},
	{
		Name: "ttus_tuple_cycle_len2_ttu",
		Tuples: []*openfgav1.TupleKey{
			// Create a cycle of ttus -> directs
			{Object: "ttus:cycle_1", Relation: "mult_parent_types", User: "directs:cycle_1"},
			{Object: "directs:cycle_1", Relation: "cycle_len2_parent", User: "ttus:cycle_2"},
			{Object: "ttus:cycle_2", Relation: "mult_parent_types", User: "directs:cycle_2"},
			{Object: "directs:cycle_2", Relation: "cycle_len2_parent", User: "ttus:cycle_3"},

			// Conditionally relate directs
			{Object: "ttus:cycle_3", Relation: "mult_parent_types", User: "directs:cycle_3", Condition: xCond},
			{Object: "directs:cycle_3", Relation: "tuple_cycle_len2_ttu", User: "user:ttu_cycle_bob"},

			// Conditionally relate directs-employee in the same place
			{Object: "ttus:cycle_3", Relation: "mult_parent_types", User: "directs-employee:cycle_3", Condition: xCond},
			{Object: "directs-employee:cycle_3", Relation: "tuple_cycle_len2_ttu", User: "user:ttu_cycle_employee"},

			// User attached unconditionally in the middle of the chain, charlie should not receive ttus:cycle_3
			{Object: "directs:cycle_2", Relation: "tuple_cycle_len2_ttu", User: "user:ttu_charlie"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:ttu_cycle_bob",
					Type:     "ttus",
					Relation: "tuple_cycle_len2_ttu",
				},
				Context: validConditionContext,
				Expectation: []string{
					"ttus:cycle_1",
					"ttus:cycle_2",
					"ttus:cycle_3",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:ttu_cycle_bob",
					Type:     "ttus",
					Relation: "tuple_cycle_len2_ttu",
				},
				Context:     invalidConditionContext,
				Expectation: nil,
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:ttu_cycle_employee",
					Type:     "ttus",
					Relation: "tuple_cycle_len2_ttu",
				},
				Context: validConditionContext,
				Expectation: []string{
					"ttus:cycle_1",
					"ttus:cycle_2",
					"ttus:cycle_3",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:ttu_cycle_employee",
					Type:     "ttus",
					Relation: "tuple_cycle_len2_ttu",
				},
				Context:     invalidConditionContext,
				Expectation: nil,
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:ttu_charlie",
					Type:     "ttus",
					Relation: "tuple_cycle_len2_ttu",
				},
				Expectation: []string{"ttus:cycle_1", "ttus:cycle_2"},
			},
		},
	},
	{
		Name: "ttus_recursive_combined_w3",
		Tuples: []*openfgav1.TupleKey{
			// Create a recursive chain
			{Object: "ttus:rc_2", Relation: "ttu_parent", User: "ttus:rc_1"},
			{Object: "ttus:rc_3", Relation: "ttu_parent", User: "ttus:rc_2"},

			// anne is the only user who should see rc_1, everyone else relates to one of its children
			{Object: "ttus:rc_1", Relation: "ttu_recursive_combined_w3", User: "user:rc_anne"},

			// Bob relates to rc_2 and its children (rc_3). He should not see ttus:rc_1
			{Object: "directs:rc_2", Relation: "direct", User: "user:rc_bob"},
			{Object: "ttus:rc_2", Relation: "direct_parent", User: "directs:rc_2"},

			// Create a fork in the recursive chain, rc_2 now has multiple ttu_parent chains
			{Object: "ttus:rc_2", Relation: "ttu_parent", User: "ttus:rc_a"},
			{Object: "ttus:rc_a", Relation: "ttu_parent", User: "ttus:rc_b"},

			// employee can see rc_a and its children (rc_2, rc_3)
			{Object: "ttus:rc_a", Relation: "ttu_recursive_combined_w3", User: "employee:rc_a"},

			// Returns for any user
			{Object: "ttus:rc_wild_parent", Relation: "ttu_recursive_combined_w3", User: "user:*"},
			{Object: "ttus:rc_wild_child", Relation: "ttu_parent", User: "ttus:rc_wild_parent"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:rc_anne",
					Type:     "ttus",
					Relation: "ttu_recursive_combined_w3",
				},
				Expectation: []string{
					"ttus:rc_1",
					"ttus:rc_2",
					"ttus:rc_3",
					"ttus:rc_wild_parent",
					"ttus:rc_wild_child",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:rc_bob",
					Type:     "ttus",
					Relation: "ttu_recursive_combined_w3",
				},
				Expectation: []string{
					"ttus:rc_2",
					"ttus:rc_3",
					"ttus:rc_wild_parent",
					"ttus:rc_wild_child",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "employee:rc_a",
					Type:     "ttus",
					Relation: "ttu_recursive_combined_w3",
				},
				Expectation: []string{
					"ttus:rc_a",
					"ttus:rc_2",
					"ttus:rc_3",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:public",
					Type:     "ttus",
					Relation: "ttu_recursive_combined_w3",
				},
				Expectation: []string{
					"ttus:rc_wild_parent",
					"ttus:rc_wild_child",
				},
			},
		},
	},
}
