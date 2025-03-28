package listobjects

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	listobjectstest "github.com/openfga/openfga/internal/test/listobjects"
)

var usersets = []matrixTest{
	{
		Name: "usersets_user_alg_combined",
		Tuples: []*openfgav1.TupleKey{
			{Object: "usersets-user:user_alg_1", Relation: "userset_alg_combined", User: "directs:user_alg_1#alg_combined"},
			{Object: "usersets-user:user_alg_2", Relation: "userset_alg_combined", User: "directs:user_alg_2#alg_combined"},

			{Object: "usersets-user:user_alg_3", Relation: "userset_alg_combined", User: "directs-employee:user_alg_1#alg_combined"},
			{Object: "usersets-user:user_alg_4", Relation: "userset_alg_combined", User: "directs-employee:user_alg_2#alg_combined"},

			// This satisfies directs#alg_combined
			{Object: "directs:user_alg_1", Relation: "direct_mult_types", User: "user:user_alg_1"},
			{Object: "directs:user_alg_1", Relation: "other_rel", User: "user:user_alg_1"},
			// This does not
			{Object: "directs:user_alg_2", Relation: "other_rel", User: "user:user_alg_1"},

			// This satisfies directs-employee#alg_combined
			{Object: "directs-employee:user_alg_1", Relation: "direct", User: "employee:user_alg_1"},
			{Object: "directs-employee:user_alg_1", Relation: "direct_wild", User: "employee:*"},
			// This does not
			{Object: "directs-employee:user_alg_2", Relation: "other_rel", User: "employee:user_alg_1"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "directs:user_alg_1#alg_combined",
					Type:     "usersets-user",
					Relation: "userset_alg_combined",
				},
				Expectation: []string{"usersets-user:user_alg_1"},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:user_alg_1",
					Type:     "usersets-user",
					Relation: "userset_alg_combined",
				},

				Expectation: []string{"usersets-user:user_alg_1"},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "employee:user_alg_1",
					Type:     "usersets-user",
					Relation: "userset_alg_combined",
				},

				Expectation: []string{"usersets-user:user_alg_3"},
			},
		},
	},
	{
		Name: "usersets_user_alg_combined_oneline",
		Tuples: []*openfgav1.TupleKey{
			{Object: "usersets-user:oneline_1", Relation: "userset_alg_combined_oneline", User: "directs:oneline_1#alg_combined_oneline"},
			{Object: "usersets-user:oneline_2", Relation: "userset_alg_combined_oneline", User: "directs:oneline_2#alg_combined_oneline"},

			{Object: "usersets-user:oneline_3", Relation: "userset_alg_combined_oneline", User: "directs-employee:oneline_1#alg_combined_oneline"},
			{Object: "usersets-user:oneline_4", Relation: "userset_alg_combined_oneline", User: "directs-employee:oneline_2#alg_combined_oneline"},

			// This satisfies directs#alg_combined_oneline
			{Object: "directs:oneline_1", Relation: "direct", User: "user:oneline_1"},
			{Object: "directs:oneline_1", Relation: "other_rel", User: "user:oneline_1"},
			// This does not
			{Object: "directs:oneline_2", Relation: "other_rel", User: "user:oneline_1"},

			// This satisfies directs-employee#alg_combined_oneline
			{Object: "directs-employee:oneline_1", Relation: "direct", User: "employee:oneline_1"},
			// This does not
			{Object: "directs-employee:oneline_2", Relation: "other_rel", User: "employee:oneline_1"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "directs:oneline_2#alg_combined_oneline",
					Type:     "usersets-user",
					Relation: "userset_alg_combined_oneline",
				},
				Expectation: []string{"usersets-user:oneline_2"},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "directs:oneline_1#alg_combined_oneline",
					Type:     "usersets-user",
					Relation: "userset_alg_combined_oneline",
				},
				Expectation: []string{"usersets-user:oneline_1"},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:oneline_1",
					Type:     "usersets-user",
					Relation: "userset_alg_combined_oneline",
				},

				Expectation: []string{"usersets-user:oneline_1"},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "employee:oneline_1",
					Type:     "usersets-user",
					Relation: "userset_alg_combined_oneline",
				},

				Expectation: []string{"usersets-user:oneline_3"},
			},
		},
	},
	{
		Name: "usersets_user_userset_recursive",
		Tuples: []*openfgav1.TupleKey{
			// Create a recursive chain
			{Object: "usersets-user:recursive_level_1", Relation: "userset_recursive", User: "user:recursive_1"},
			{Object: "usersets-user:recursive_level_2", Relation: "userset_recursive", User: "usersets-user:recursive_level_1#userset_recursive"},
			{Object: "usersets-user:recursive_level_3", Relation: "userset_recursive", User: "usersets-user:recursive_level_2#userset_recursive"},
			{Object: "usersets-user:recursive_level_4", Relation: "userset_recursive", User: "usersets-user:recursive_level_3#userset_recursive"},

			// Attach another user in the middle of the chain
			{Object: "usersets-user:recursive_level_3", Relation: "userset_recursive", User: "user:recursive_2"},

			// Add another branch
			{Object: "usersets-user:branch_2_level_1", Relation: "userset_recursive", User: "user:other_branch"},
			{Object: "usersets-user:branch_2_level_2", Relation: "userset_recursive", User: "usersets-user:branch_2_level_1#userset_recursive"},
			// Now tie it to the first branch created above
			{Object: "usersets-user:recursive_level_3", Relation: "userset_recursive", User: "usersets-user:branch_2_level_2#userset_recursive"},

			{Object: "usersets-user:branch_with_cycle_1", Relation: "userset_recursive", User: "user:cycle_1"},
			{Object: "usersets-user:branch_with_cycle_2", Relation: "userset_recursive", User: "usersets-user:branch_with_cycle_1#userset_recursive"},
			{Object: "usersets-user:branch_with_cycle_3", Relation: "userset_recursive", User: "usersets-user:branch_with_cycle_2#userset_recursive"},
			{Object: "usersets-user:branch_with_cycle_4", Relation: "userset_recursive", User: "usersets-user:branch_with_cycle_3#userset_recursive"},
			// Create cycle
			{Object: "usersets-user:branch_with_cycle_2", Relation: "userset_recursive", User: "usersets-user:branch_with_cycle_4#userset_recursive"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "usersets-user:recursive_level_3#userset_recursive",
					Type:     "usersets-user",
					Relation: "userset_recursive",
				},
				Expectation: []string{
					"usersets-user:recursive_level_3",
					"usersets-user:recursive_level_4",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:recursive_1",
					Type:     "usersets-user",
					Relation: "userset_recursive",
				},
				Expectation: []string{
					"usersets-user:recursive_level_1",
					"usersets-user:recursive_level_2",
					"usersets-user:recursive_level_3",
					"usersets-user:recursive_level_4",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:recursive_2",
					Type:     "usersets-user",
					Relation: "userset_recursive",
				},
				Expectation: []string{
					"usersets-user:recursive_level_3",
					"usersets-user:recursive_level_4",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:other_branch",
					Type:     "usersets-user",
					Relation: "userset_recursive",
				},
				Expectation: []string{
					"usersets-user:branch_2_level_2",
					"usersets-user:branch_2_level_1",
					"usersets-user:recursive_level_3",
					"usersets-user:recursive_level_4",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:cycle_1",
					Type:     "usersets-user",
					Relation: "userset_recursive",
				},
				Expectation: []string{
					"usersets-user:branch_with_cycle_1",
					"usersets-user:branch_with_cycle_2",
					"usersets-user:branch_with_cycle_3",
					"usersets-user:branch_with_cycle_4",
				},
			},
		},
	},
	{
		Name: "usersets_tuple_cycle_len2_userset",
		Tuples: []*openfgav1.TupleKey{
			// cycle
			{Object: "usersets-user:len2_1", Relation: "tuple_cycle_len2_userset", User: "directs:len2_1#tuple_cycle_len2_userset"},
			{Object: "directs:len2_1", Relation: "tuple_cycle_len2_userset", User: "usersets-user:len2_2#tuple_cycle_len2_userset"},
			{Object: "usersets-user:len2_2", Relation: "tuple_cycle_len2_userset", User: "user:len2_anne"},
			{Object: "directs:len2_2", Relation: "tuple_cycle_len2_userset", User: "usersets-user:len2_1#tuple_cycle_len2_userset"},

			// Evaluate the condition path
			{Object: "usersets-user:len2_2", Relation: "tuple_cycle_len2_userset", User: "directs-employee:len2_1#tuple_cycle_len2_userset", Condition: xCond},
			{Object: "directs-employee:len2_1", Relation: "tuple_cycle_len2_userset", User: "employee:len2_bob"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:len2_anne",
					Type:     "usersets-user",
					Relation: "tuple_cycle_len2_userset",
				},
				Expectation: []string{
					"usersets-user:len2_1",
					"usersets-user:len2_2",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:len2_anne",
					Type:     "directs",
					Relation: "tuple_cycle_len2_userset",
				},
				Expectation: []string{
					"directs:len2_1",
					"directs:len2_2",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "employee:len2_bob",
					Type:     "directs",
					Relation: "tuple_cycle_len2_userset",
				},
				Context: validConditionContext,
				Expectation: []string{
					"directs:len2_1",
					"directs:len2_2",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "employee:len2_bob",
					Type:     "directs",
					Relation: "tuple_cycle_len2_userset",
				},
				Context:     invalidConditionContext,
				Expectation: nil,
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "employee:len2_bob",
					Type:     "usersets-user",
					Relation: "tuple_cycle_len2_userset",
				},
				Context: validConditionContext,
				Expectation: []string{
					"usersets-user:len2_1",
					"usersets-user:len2_2",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "directs-employee:len2_1#tuple_cycle_len2_userset",
					Type:     "usersets-user",
					Relation: "tuple_cycle_len2_userset",
				},
				Context: validConditionContext,
				Expectation: []string{
					"usersets-user:len2_1",
					"usersets-user:len2_2",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "directs-employee:len2_1#tuple_cycle_len2_userset",
					Type:     "directs",
					Relation: "tuple_cycle_len2_userset",
				},
				Context: validConditionContext,
				Expectation: []string{
					"directs:len2_1",
					"directs:len2_2",
				},
			},
		},
	},
}
