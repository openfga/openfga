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
		Name: "userset_recursive_public",
		Tuples: []*openfgav1.TupleKey{
			// Create a recursive chain
			{Object: "usersets-user:recursive_public_level_1", Relation: "userset_recursive_public", User: "user:*"},
			{Object: "usersets-user:recursive_public_level_2", Relation: "userset_recursive_public", User: "usersets-user:recursive_public_level_1#userset_recursive_public"},
			{Object: "usersets-user:recursive_public_level_3", Relation: "userset_recursive_public", User: "usersets-user:recursive_public_level_2#userset_recursive_public"},
			{Object: "usersets-user:recursive_public_level_4", Relation: "userset_recursive_public", User: "usersets-user:recursive_public_level_3#userset_recursive_public"},

			// Attach another user in the middle of the chain
			{Object: "usersets-user:recursive_public_level_3", Relation: "userset_recursive_public", User: "user:*"},

			// Another branch-chain
			{Object: "usersets-user:recursive_public_branch_level_1", Relation: "userset_recursive_public", User: "user:*"},
			{Object: "usersets-user:recursive_public_branch_level_2", Relation: "userset_recursive_public", User: "usersets-user:recursive_public_branch_level_1#userset_recursive_public"},
			{Object: "usersets-user:recursive_public_branch_level_3", Relation: "userset_recursive_public", User: "usersets-user:recursive_public_branch_level_2#userset_recursive_public"},
			{Object: "usersets-user:recursive_public_branch_level_4", Relation: "userset_recursive_public", User: "usersets-user:recursive_public_branch_level_3#userset_recursive_public"},

			// Attach another user in the middle of the chain
			{Object: "usersets-user:recursive_public_branch_level_2", Relation: "userset_recursive_public", User: "user:*"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "usersets-user:recursive_public_level_3#userset_recursive_public",
					Type:     "usersets-user",
					Relation: "userset_recursive_public",
				},
				Expectation: []string{
					"usersets-user:recursive_public_level_3",
					"usersets-user:recursive_public_level_4",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:public",
					Type:     "usersets-user",
					Relation: "userset_recursive_public",
				},
				Expectation: []string{
					"usersets-user:recursive_public_level_1",
					"usersets-user:recursive_public_level_2",
					"usersets-user:recursive_public_level_3",
					"usersets-user:recursive_public_level_4",
					"usersets-user:recursive_public_branch_level_1",
					"usersets-user:recursive_public_branch_level_2",
					"usersets-user:recursive_public_branch_level_3",
					"usersets-user:recursive_public_branch_level_4",
				},
			},
		},
	},
	{
		Name: "userset_recursive_combined_w3",
		Tuples: []*openfgav1.TupleKey{
			// direct assign
			{Object: "usersets-user:userset_recursive_combined_w3_direct", Relation: "userset_recursive_combined_w3", User: "user:userset_recursive_combined_w3_direct"},
			// public wildcard direct assign
			{Object: "usersets-user:userset_recursive_combined_w3_public", Relation: "userset_recursive_combined_w3", User: "user:*"},
			// recursive via usersets-user#userset_recursive_combined_w3
			{Object: "usersets-user:userset_recursive_combined_w3_direct_recursive_1", Relation: "userset_recursive_combined_w3", User: "usersets-user:userset_recursive_combined_w3_direct#userset_recursive_combined_w3"},
			{Object: "usersets-user:userset_recursive_combined_w3_direct_recursive_2", Relation: "userset_recursive_combined_w3", User: "usersets-user:userset_recursive_combined_w3_direct_recursive_1#userset_recursive_combined_w3"},
			{Object: "usersets-user:userset_recursive_combined_w3_public_recursive_1", Relation: "userset_recursive_combined_w3", User: "usersets-user:userset_recursive_combined_w3_public#userset_recursive_combined_w3"},
			{Object: "usersets-user:userset_recursive_combined_w3_public_recursive_2", Relation: "userset_recursive_combined_w3", User: "usersets-user:userset_recursive_combined_w3_public_recursive_1#userset_recursive_combined_w3"},
			// usersets-user#userset
			{Object: "usersets-user:userset_recursive_combined_w3_userset_entry", Relation: "userset", User: "directs:userset_recursive_combined_w3_userset#direct_comb"},
			{Object: "usersets-user:userset_recursive_combined_w3_userset", Relation: "userset_recursive_combined_w3", User: "usersets-user:userset_recursive_combined_w3_userset_entry#userset"},
			{Object: "directs:userset_recursive_combined_w3_userset", Relation: "direct_comb", User: "user:userset_recursive_combined_w3_userset_direct"},
			{Object: "directs:userset_recursive_combined_w3_userset", Relation: "direct_comb", User: "user:userset_recursive_combined_w3_userset_direct_cond", Condition: xCond},
			{Object: "usersets-user:userset_recursive_combined_w3_userset_recursive_1", Relation: "userset_recursive_combined_w3", User: "usersets-user:userset_recursive_combined_w3_userset#userset_recursive_combined_w3"},
			// employee direct
			{Object: "usersets-user:userset_recursive_combined_w3_employee_direct", Relation: "userset_recursive_combined_w3", User: "employee:userset_recursive_combined_w3_employee_direct"},
			{Object: "usersets-user:userset_recursive_combined_w3_employee_direct_recursive_1", Relation: "userset_recursive_combined_w3", User: "usersets-user:userset_recursive_combined_w3_employee_direct#userset_recursive_combined_w3"},
			// usersets-user#userset via employee (public and public cond)
			{Object: "usersets-user:userset_recursive_combined_w3_userset_employee_entry", Relation: "userset", User: "directs-employee:userset_recursive_combined_w3_userset_employee#direct"},
			{Object: "usersets-user:userset_recursive_combined_w3_userset_employee", Relation: "userset_recursive_combined_w3", User: "usersets-user:userset_recursive_combined_w3_userset_employee_entry#userset"},
			{Object: "usersets-user:userset_recursive_combined_w3_userset_employee_recursive_1", Relation: "userset_recursive_combined_w3", User: "usersets-user:userset_recursive_combined_w3_userset_employee#userset_recursive_combined_w3"},
			{Object: "directs-employee:userset_recursive_combined_w3_userset_employee", Relation: "direct", User: "employee:*"},
			{Object: "usersets-user:userset_recursive_combined_w3_userset_employee_entry_cond", Relation: "userset", User: "directs-employee:userset_recursive_combined_w3_userset_employee_cond#direct"},
			{Object: "usersets-user:userset_recursive_combined_w3_userset_employee_cond", Relation: "userset_recursive_combined_w3", User: "usersets-user:userset_recursive_combined_w3_userset_employee_entry_cond#userset"},
			{Object: "usersets-user:userset_recursive_combined_w3_userset_employee_cond_recursive_1", Relation: "userset_recursive_combined_w3", User: "usersets-user:userset_recursive_combined_w3_userset_employee_cond#userset_recursive_combined_w3"},
			{Object: "directs-employee:userset_recursive_combined_w3_userset_employee_cond", Relation: "direct", User: "employee:*", Condition: xCond},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_combined_w3_direct",
					Type:     "usersets-user",
					Relation: "userset_recursive_combined_w3",
				},
				Expectation: []string{
					"usersets-user:userset_recursive_combined_w3_direct",
					"usersets-user:userset_recursive_combined_w3_public",
					"usersets-user:userset_recursive_combined_w3_direct_recursive_1",
					"usersets-user:userset_recursive_combined_w3_direct_recursive_2",
					"usersets-user:userset_recursive_combined_w3_public_recursive_1",
					"usersets-user:userset_recursive_combined_w3_public_recursive_2",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:public",
					Type:     "usersets-user",
					Relation: "userset_recursive_combined_w3",
				},
				Expectation: []string{
					"usersets-user:userset_recursive_combined_w3_public",
					"usersets-user:userset_recursive_combined_w3_public_recursive_1",
					"usersets-user:userset_recursive_combined_w3_public_recursive_2",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "usersets-user:userset_recursive_combined_w3_userset_entry#userset",
					Type:     "usersets-user",
					Relation: "userset_recursive_combined_w3",
				},
				Expectation: []string{
					"usersets-user:userset_recursive_combined_w3_userset",
					"usersets-user:userset_recursive_combined_w3_userset_recursive_1",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_combined_w3_userset_direct",
					Type:     "usersets-user",
					Relation: "userset_recursive_combined_w3",
				},
				Expectation: []string{
					"usersets-user:userset_recursive_combined_w3_public",
					"usersets-user:userset_recursive_combined_w3_public_recursive_1",
					"usersets-user:userset_recursive_combined_w3_public_recursive_2",
					"usersets-user:userset_recursive_combined_w3_userset",
					"usersets-user:userset_recursive_combined_w3_userset_recursive_1",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_combined_w3_userset_direct_cond",
					Type:     "usersets-user",
					Relation: "userset_recursive_combined_w3",
				},
				Context: validConditionContext,
				Expectation: []string{
					"usersets-user:userset_recursive_combined_w3_public",
					"usersets-user:userset_recursive_combined_w3_public_recursive_1",
					"usersets-user:userset_recursive_combined_w3_public_recursive_2",
					"usersets-user:userset_recursive_combined_w3_userset",
					"usersets-user:userset_recursive_combined_w3_userset_recursive_1",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_combined_w3_userset_direct_cond",
					Type:     "usersets-user",
					Relation: "userset_recursive_combined_w3",
				},
				Context: invalidConditionContext,
				Expectation: []string{
					"usersets-user:userset_recursive_combined_w3_public",
					"usersets-user:userset_recursive_combined_w3_public_recursive_1",
					"usersets-user:userset_recursive_combined_w3_public_recursive_2",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "employee:public",
					Type:     "usersets-user",
					Relation: "userset_recursive_combined_w3",
				},
				Context: validConditionContext,
				Expectation: []string{
					"usersets-user:userset_recursive_combined_w3_userset_employee",
					"usersets-user:userset_recursive_combined_w3_userset_employee_recursive_1",
					"usersets-user:userset_recursive_combined_w3_userset_employee_cond",
					"usersets-user:userset_recursive_combined_w3_userset_employee_cond_recursive_1",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "employee:public",
					Type:     "usersets-user",
					Relation: "userset_recursive_combined_w3",
				},
				Context: invalidConditionContext,
				Expectation: []string{
					"usersets-user:userset_recursive_combined_w3_userset_employee",
					"usersets-user:userset_recursive_combined_w3_userset_employee_recursive_1",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "employee:userset_recursive_combined_w3_employee_direct",
					Type:     "usersets-user",
					Relation: "userset_recursive_combined_w3",
				},
				Context: validConditionContext,
				Expectation: []string{
					"usersets-user:userset_recursive_combined_w3_userset_employee",
					"usersets-user:userset_recursive_combined_w3_userset_employee_recursive_1",
					"usersets-user:userset_recursive_combined_w3_userset_employee_cond",
					"usersets-user:userset_recursive_combined_w3_userset_employee_cond_recursive_1",
					"usersets-user:userset_recursive_combined_w3_employee_direct",
					"usersets-user:userset_recursive_combined_w3_employee_direct_recursive_1",
				},
			},
		},
	},
	{
		Name: "userset_recursive_alg_combined_oneline",
		Tuples: []*openfgav1.TupleKey{
			// only rel2 / rel3 (but not both).  Thus, they should not result in any match
			{Object: "usersets-user:userset_recursive_alg_combined_oneline_rel2_only", Relation: "user_rel2", User: "user:userset_recursive_alg_combined_oneline_rel2_only"},
			{Object: "usersets-user:userset_recursive_alg_combined_oneline_rel3_only", Relation: "user_rel3", User: "user:userset_recursive_alg_combined_oneline_rel3_only"},

			// user_rel2 and user_rel3
			{Object: "usersets-user:userset_recursive_alg_combined_oneline_rel2_rel3", Relation: "user_rel2", User: "user:userset_recursive_alg_combined_oneline_rel2_rel3"},
			{Object: "usersets-user:userset_recursive_alg_combined_oneline_rel2_rel3", Relation: "user_rel3", User: "user:userset_recursive_alg_combined_oneline_rel2_rel3"},
			{Object: "usersets-user:userset_recursive_alg_combined_oneline_rel2_rel3_recursive_1", Relation: "userset_recursive_alg_combined_oneline", User: "usersets-user:userset_recursive_alg_combined_oneline_rel2_rel3#userset_recursive_alg_combined_oneline"},

			// user_rel1
			{Object: "usersets-user:userset_recursive_alg_combined_oneline_rel1_only", Relation: "user_rel1", User: "user:userset_recursive_alg_combined_oneline_rel1_only"},
			{Object: "usersets-user:userset_recursive_alg_combined_oneline_rel1_only_recursive_1", Relation: "userset_recursive_alg_combined_oneline", User: "usersets-user:userset_recursive_alg_combined_oneline_rel1_only#userset_recursive_alg_combined_oneline"},

			// direct
			{Object: "usersets-user:userset_recursive_alg_combined_oneline_direct", Relation: "userset_recursive_alg_combined_oneline", User: "user:userset_recursive_alg_combined_oneline_direct"},
			{Object: "usersets-user:userset_recursive_alg_combined_oneline_direct_recursive_1", Relation: "userset_recursive_alg_combined_oneline", User: "usersets-user:userset_recursive_alg_combined_oneline_direct#userset_recursive_alg_combined_oneline"},

			// user_rel2 and user_rel3 with cond
			{Object: "usersets-user:userset_recursive_alg_combined_oneline_rel2_rel3_cond", Relation: "user_rel2", User: "user:userset_recursive_alg_combined_oneline_rel2_rel3_cond", Condition: xCond},
			{Object: "usersets-user:userset_recursive_alg_combined_oneline_rel2_rel3_cond", Relation: "user_rel3", User: "user:*", Condition: xCond},
			{Object: "usersets-user:userset_recursive_alg_combined_oneline_rel2_rel3_cond_recursive_1", Relation: "userset_recursive_alg_combined_oneline", User: "usersets-user:userset_recursive_alg_combined_oneline_rel2_rel3_cond#userset_recursive_alg_combined_oneline"},

			// user_rel2 no cond and user_rel3 with cond
			{Object: "usersets-user:userset_recursive_alg_combined_oneline_rel2_nocond_rel3_cond", Relation: "user_rel2", User: "user:userset_recursive_alg_combined_oneline_rel2_nocond_rel3_cond"},
			{Object: "usersets-user:userset_recursive_alg_combined_oneline_rel2_nocond_rel3_cond", Relation: "user_rel3", User: "user:*", Condition: xCond},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_alg_combined_oneline_rel2_only",
					Type:     "usersets-user",
					Relation: "userset_recursive_alg_combined_oneline",
				},
				Context:     invalidConditionContext,
				Expectation: []string{},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_alg_combined_oneline_rel3_only",
					Type:     "usersets-user",
					Relation: "userset_recursive_alg_combined_oneline",
				},
				Context:     invalidConditionContext,
				Expectation: []string{},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_alg_combined_oneline_rel2_rel3",
					Type:     "usersets-user",
					Relation: "userset_recursive_alg_combined_oneline",
				},
				Context: invalidConditionContext,
				Expectation: []string{
					"usersets-user:userset_recursive_alg_combined_oneline_rel2_rel3",
					"usersets-user:userset_recursive_alg_combined_oneline_rel2_rel3_recursive_1",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_alg_combined_oneline_rel1_only",
					Type:     "usersets-user",
					Relation: "userset_recursive_alg_combined_oneline",
				},
				Context: invalidConditionContext,
				Expectation: []string{
					"usersets-user:userset_recursive_alg_combined_oneline_rel1_only",
					"usersets-user:userset_recursive_alg_combined_oneline_rel1_only_recursive_1",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_alg_combined_oneline_rel1_only",
					Type:     "usersets-user",
					Relation: "userset_recursive_alg_combined_oneline",
				},
				Context: invalidConditionContext,
				Expectation: []string{
					"usersets-user:userset_recursive_alg_combined_oneline_rel1_only",
					"usersets-user:userset_recursive_alg_combined_oneline_rel1_only_recursive_1",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_alg_combined_oneline_direct",
					Type:     "usersets-user",
					Relation: "userset_recursive_alg_combined_oneline",
				},
				Context: invalidConditionContext,
				Expectation: []string{
					"usersets-user:userset_recursive_alg_combined_oneline_direct",
					"usersets-user:userset_recursive_alg_combined_oneline_direct_recursive_1",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_alg_combined_oneline_rel2_rel3_cond",
					Type:     "usersets-user",
					Relation: "userset_recursive_alg_combined_oneline",
				},
				Context: validConditionContext,
				Expectation: []string{
					"usersets-user:userset_recursive_alg_combined_oneline_rel2_rel3_cond",
					"usersets-user:userset_recursive_alg_combined_oneline_rel2_rel3_cond_recursive_1",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_alg_combined_oneline_rel2_rel3_cond",
					Type:     "usersets-user",
					Relation: "userset_recursive_alg_combined_oneline",
				},
				Context:     invalidConditionContext,
				Expectation: []string{},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_alg_combined_oneline_rel2_nocond_rel3_cond",
					Type:     "usersets-user",
					Relation: "userset_recursive_alg_combined_oneline",
				},
				// if it is valid, there should be public wildcard that matches
				Context: validConditionContext,
				Expectation: []string{
					"usersets-user:userset_recursive_alg_combined_oneline_rel2_nocond_rel3_cond",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_alg_combined_oneline_rel2_nocond_rel3_cond",
					Type:     "usersets-user",
					Relation: "userset_recursive_alg_combined_oneline",
				},
				Context:     invalidConditionContext,
				Expectation: []string{},
			},
		},
	},
	{
		Name: "alg_combined_computed",
		Tuples: []*openfgav1.TupleKey{
			// directs#alg_combined -> (((direct or direct_comb or direct_mult_types) and other_rel) but not direct_comb) but not direct
			// directs#computed_mult_types -> direct_mult_types
			// directs#alg_combined_oneline -> (direct or direct_comb) and (direct_mult_types or other_rel)

			// (([directs#direct_comb, directs-employee#direct] or [directs#alg_combined, directs-employee#alg_combined]) and
			//     [directs#computed_mult_types with xcond, directs-employee#computed_3_times])
			//     but not [directs#alg_combined_oneline, directs-employee#alg_combined_oneline]

			// (userset or userset_alg_combined) and userset_combined_cond but not userset_alg_combined_oneline

			// first case: user is in both userset and userset_combined_cond via directs
			{Object: "usersets-user:alg_combined_computed_direct_mult_types_only", Relation: "userset", User: "directs:alg_combined_computed_direct_mult_types_only#direct_comb"},
			{Object: "usersets-user:alg_combined_computed_direct_mult_types_only", Relation: "userset_combined_cond", User: "directs:alg_combined_computed_direct_mult_types_only#computed_mult_types", Condition: xCond},
			{Object: "directs:alg_combined_computed_direct_mult_types_only", Relation: "direct_comb", User: "user:alg_combined_computed_direct_mult_types_only"},
			{Object: "directs:alg_combined_computed_direct_mult_types_only", Relation: "direct_mult_types", User: "user:alg_combined_computed_direct_mult_types_only"},

			// second case: user is in both userset and userset_combined_cond via directs, but they are actually different directs object
			{Object: "usersets-user:alg_combined_computed_direct_mult_types_only_1", Relation: "userset", User: "directs:alg_combined_computed_direct_mult_types_only_1a#direct_comb"},
			{Object: "usersets-user:alg_combined_computed_direct_mult_types_only_1", Relation: "userset_combined_cond", User: "directs:alg_combined_computed_direct_mult_types_only_1b#computed_mult_types", Condition: xCond},
			{Object: "directs:alg_combined_computed_direct_mult_types_only_1a", Relation: "direct_comb", User: "user:alg_combined_computed_direct_mult_types_only_1"},
			{Object: "directs:alg_combined_computed_direct_mult_types_only_1b", Relation: "direct_mult_types", User: "user:alg_combined_computed_direct_mult_types_only_1"},

			// third case: user is in userset_alg_combined and userset_combined_cond via directs, but userset_alg_combined yield to false because direct_comb is in but not
			{Object: "usersets-user:alg_combined_computed_direct_mult_types_alg_combined", Relation: "userset_alg_combined", User: "directs:alg_combined_computed_direct_mult_types_alg_combined#alg_combined"},
			{Object: "usersets-user:alg_combined_computed_direct_mult_types_alg_combined", Relation: "userset_combined_cond", User: "directs:alg_combined_computed_direct_mult_types_alg_combined#computed_mult_types", Condition: xCond},
			{Object: "directs:alg_combined_computed_direct_mult_types_alg_combined", Relation: "direct_comb", User: "user:alg_combined_computed_direct_mult_types_alg_combined"},
			{Object: "directs:alg_combined_computed_direct_mult_types_alg_combined", Relation: "other_rel", User: "user:alg_combined_computed_direct_mult_types_alg_combined"},
			{Object: "directs:alg_combined_computed_direct_mult_types_alg_combined", Relation: "direct_mult_types", User: "user:alg_combined_computed_direct_mult_types_alg_combined"},
			// fourth case, user is in userset_alg_combined and userset_combined_cond via directs, but userset_alg_combined yield to true
			{Object: "directs:alg_combined_computed_direct_mult_types_alg_combined", Relation: "direct_mult_types", User: "user:alg_combined_computed_direct_mult_types_alg_combined_direct_mult_types"},
			{Object: "directs:alg_combined_computed_direct_mult_types_alg_combined", Relation: "other_rel", User: "user:alg_combined_computed_direct_mult_types_alg_combined_direct_mult_types"},
			// fifth case, user is in userset_alg_combined and userset_combined_cond via directs, but userset_alg_combined yield to false because direct is in but not
			{Object: "directs:alg_combined_computed_direct_mult_types_alg_combined", Relation: "direct", User: "user:alg_combined_computed_direct_mult_types_alg_combined_direct"},
			{Object: "directs:alg_combined_computed_direct_mult_types_alg_combined", Relation: "other_rel", User: "user:alg_combined_computed_direct_mult_types_alg_combined_direct"},
			{Object: "directs:alg_combined_computed_direct_mult_types_alg_combined", Relation: "direct_mult_types", User: "user:alg_combined_computed_direct_mult_types_alg_combined_direct"},
			// sixth case.  Similar to fourth case except it is false because missing other_rel
			{Object: "directs:alg_combined_computed_direct_mult_types_alg_combined", Relation: "direct_mult_types", User: "user:alg_combined_computed_direct_mult_types_alg_combined_direct_mult_types_missing_other_rel"},
			// seventh case - similar to second case except that userset_alg_combined_oneline is true in the top level (which makes the whole thing false)
			{Object: "usersets-user:alg_combined_computed_direct_mult_types_only_2", Relation: "userset", User: "directs:alg_combined_computed_direct_mult_types_only_2a#direct_comb"},
			{Object: "usersets-user:alg_combined_computed_direct_mult_types_only_2", Relation: "userset_combined_cond", User: "directs:alg_combined_computed_direct_mult_types_only_2b#computed_mult_types", Condition: xCond},
			{Object: "usersets-user:alg_combined_computed_direct_mult_types_only_2", Relation: "userset_alg_combined_oneline", User: "directs:alg_combined_computed_direct_mult_types_only_2c#alg_combined_oneline"},
			{Object: "directs:alg_combined_computed_direct_mult_types_only_2a", Relation: "direct_comb", User: "user:alg_combined_computed_direct_mult_types_only_2"},
			{Object: "directs:alg_combined_computed_direct_mult_types_only_2b", Relation: "direct_mult_types", User: "user:alg_combined_computed_direct_mult_types_only_2"},
			{Object: "directs:alg_combined_computed_direct_mult_types_only_2c", Relation: "direct", User: "user:alg_combined_computed_direct_mult_types_only_2"},
			{Object: "directs:alg_combined_computed_direct_mult_types_only_2c", Relation: "other_rel", User: "user:alg_combined_computed_direct_mult_types_only_2"},
			// eight case - similar to first case except userset_combined_cond missing
			{Object: "usersets-user:alg_combined_computed_direct_mult_types_only_missing_combined", Relation: "userset", User: "directs:alg_combined_computed_direct_mult_types_only_missing_combined#direct_comb"},
			{Object: "directs:alg_combined_computed_direct_mult_types_only_missing_combined", Relation: "direct_comb", User: "user:alg_combined_computed_direct_mult_types_only_missing_combined"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:alg_combined_computed_direct_mult_types_only",
					Type:     "usersets-user",
					Relation: "alg_combined_computed",
				},
				Context: validConditionContext,
				Expectation: []string{
					"usersets-user:alg_combined_computed_direct_mult_types_only",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:alg_combined_computed_direct_mult_types_only",
					Type:     "usersets-user",
					Relation: "alg_combined_computed",
				},
				Context:     invalidConditionContext,
				Expectation: []string{},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:alg_combined_computed_direct_mult_types_only_1",
					Type:     "usersets-user",
					Relation: "alg_combined_computed",
				},
				Context: validConditionContext,
				Expectation: []string{
					"usersets-user:alg_combined_computed_direct_mult_types_only_1",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:alg_combined_computed_direct_mult_types_only_1",
					Type:     "usersets-user",
					Relation: "alg_combined_computed",
				},
				Context:     invalidConditionContext,
				Expectation: []string{},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:alg_combined_computed_direct_mult_types_alg_combined",
					Type:     "usersets-user",
					Relation: "alg_combined_computed",
				},
				Context:     validConditionContext,
				Expectation: []string{},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:alg_combined_computed_direct_mult_types_alg_combined_direct_mult_types",
					Type:     "usersets-user",
					Relation: "alg_combined_computed",
				},
				Context: validConditionContext,
				Expectation: []string{
					"usersets-user:alg_combined_computed_direct_mult_types_alg_combined",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:alg_combined_computed_direct_mult_types_alg_combined_direct",
					Type:     "usersets-user",
					Relation: "alg_combined_computed",
				},
				Context:     validConditionContext,
				Expectation: []string{},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:alg_combined_computed_direct_mult_types_alg_combined_direct_mult_types_missing_other_rel",
					Type:     "usersets-user",
					Relation: "alg_combined_computed",
				},
				Context:     validConditionContext,
				Expectation: []string{},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:alg_combined_computed_direct_mult_types_only_2",
					Type:     "usersets-user",
					Relation: "alg_combined_computed",
				},
				Context:     validConditionContext,
				Expectation: []string{},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:alg_combined_computed_direct_mult_types_only_missing_combined",
					Type:     "usersets-user",
					Relation: "alg_combined_computed",
				},
				Context:     validConditionContext,
				Expectation: []string{},
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
	{
		Name: "userset_recursive_alg_combined_w2",
		Tuples: []*openfgav1.TupleKey{
			// ([user, usersets-user#userset_recursive_alg_combined_w2] or user_rel1) or (user_rel2 and userset)

			// recursive
			{Object: "usersets-user:userset_recursive_alg_combined_w2_recursive_1", Relation: "userset_recursive_alg_combined_w2", User: "usersets-user:userset_recursive_alg_combined_w2#userset_recursive_alg_combined_w2"},
			{Object: "usersets-user:userset_recursive_alg_combined_w2_recursive_2", Relation: "userset_recursive_alg_combined_w2", User: "usersets-user:userset_recursive_alg_combined_w2_recursive_1#userset_recursive_alg_combined_w2"},
			{Object: "usersets-user:userset_recursive_alg_combined_w2_recursive_3", Relation: "userset_recursive_alg_combined_w2", User: "usersets-user:userset_recursive_alg_combined_w2_recursive_2#userset_recursive_alg_combined_w2"},

			{Object: "usersets-user:userset_recursive_alg_combined_w2", Relation: "userset_recursive_alg_combined_w2", User: "user:userset_recursive_alg_combined_w2_direct"},
			{Object: "usersets-user:userset_recursive_alg_combined_w2", Relation: "user_rel1", User: "user:userset_recursive_alg_combined_w2_rel1"},
			{Object: "usersets-user:userset_recursive_alg_combined_w2", Relation: "user_rel2", User: "user:userset_recursive_alg_combined_w2_rel2"},
			{Object: "usersets-user:userset_recursive_alg_combined_w2", Relation: "userset", User: "directs:userset_recursive_alg_combined_w2_userset#direct_comb"},
			{Object: "directs:userset_recursive_alg_combined_w2_userset", Relation: "direct_comb", User: "user:userset_recursive_alg_combined_w2_userset"},

			{Object: "usersets-user:userset_recursive_alg_combined_w2", Relation: "user_rel2", User: "user:userset_recursive_alg_combined_w2_rel2_userset"},
			{Object: "usersets-user:userset_recursive_alg_combined_w2", Relation: "userset", User: "directs:userset_recursive_alg_combined_w2_rel2_userset#direct_comb"},
			{Object: "directs:userset_recursive_alg_combined_w2_rel2_userset", Relation: "direct_comb", User: "user:userset_recursive_alg_combined_w2_rel2_userset"},

			{Object: "usersets-user:userset_recursive_alg_combined_w2", Relation: "userset", User: "directs:userset_recursive_alg_combined_w2_rel2_userset_cond#direct_comb"},
			{Object: "directs:userset_recursive_alg_combined_w2_rel2_userset_cond", Relation: "direct_comb", User: "user:*", Condition: xCond},

			{Object: "usersets-user:userset_recursive_alg_combined_w2_public", Relation: "user_rel1", User: "user:*"},
			{Object: "usersets-user:userset_recursive_alg_combined_w2_public_recursive_1", Relation: "userset_recursive_alg_combined_w2", User: "usersets-user:userset_recursive_alg_combined_w2_public#userset_recursive_alg_combined_w2"},
			{Object: "usersets-user:userset_recursive_alg_combined_w2_public_recursive_2", Relation: "userset_recursive_alg_combined_w2", User: "usersets-user:userset_recursive_alg_combined_w2_public_recursive_1#userset_recursive_alg_combined_w2"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_alg_combined_w2_direct",
					Type:     "usersets-user",
					Relation: "userset_recursive_alg_combined_w2",
				},
				Context: validConditionContext,
				Expectation: []string{
					"usersets-user:userset_recursive_alg_combined_w2",
					"usersets-user:userset_recursive_alg_combined_w2_recursive_1",
					"usersets-user:userset_recursive_alg_combined_w2_recursive_2",
					"usersets-user:userset_recursive_alg_combined_w2_recursive_3",
					"usersets-user:userset_recursive_alg_combined_w2_public",
					"usersets-user:userset_recursive_alg_combined_w2_public_recursive_1",
					"usersets-user:userset_recursive_alg_combined_w2_public_recursive_2",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_alg_combined_w2_rel1",
					Type:     "usersets-user",
					Relation: "userset_recursive_alg_combined_w2",
				},
				Context: validConditionContext,
				Expectation: []string{
					"usersets-user:userset_recursive_alg_combined_w2",
					"usersets-user:userset_recursive_alg_combined_w2_recursive_1",
					"usersets-user:userset_recursive_alg_combined_w2_recursive_2",
					"usersets-user:userset_recursive_alg_combined_w2_recursive_3",
					"usersets-user:userset_recursive_alg_combined_w2_public",
					"usersets-user:userset_recursive_alg_combined_w2_public_recursive_1",
					"usersets-user:userset_recursive_alg_combined_w2_public_recursive_2",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_alg_combined_w2_rel2",
					Type:     "usersets-user",
					Relation: "userset_recursive_alg_combined_w2",
				},
				Context: validConditionContext,
				Expectation: []string{
					"usersets-user:userset_recursive_alg_combined_w2",
					"usersets-user:userset_recursive_alg_combined_w2_recursive_1",
					"usersets-user:userset_recursive_alg_combined_w2_recursive_2",
					"usersets-user:userset_recursive_alg_combined_w2_recursive_3",
					"usersets-user:userset_recursive_alg_combined_w2_public",
					"usersets-user:userset_recursive_alg_combined_w2_public_recursive_1",
					"usersets-user:userset_recursive_alg_combined_w2_public_recursive_2",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_alg_combined_w2_rel2",
					Type:     "usersets-user",
					Relation: "userset_recursive_alg_combined_w2",
				},
				Context: invalidConditionContext,
				Expectation: []string{
					"usersets-user:userset_recursive_alg_combined_w2_public",
					"usersets-user:userset_recursive_alg_combined_w2_public_recursive_1",
					"usersets-user:userset_recursive_alg_combined_w2_public_recursive_2",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_alg_combined_w2_userset",
					Type:     "usersets-user",
					Relation: "userset_recursive_alg_combined_w2",
				},
				Context: validConditionContext,
				Expectation: []string{
					"usersets-user:userset_recursive_alg_combined_w2_public",
					"usersets-user:userset_recursive_alg_combined_w2_public_recursive_1",
					"usersets-user:userset_recursive_alg_combined_w2_public_recursive_2",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_alg_combined_w2_rel2_userset",
					Type:     "usersets-user",
					Relation: "userset_recursive_alg_combined_w2",
				},
				Context: validConditionContext,
				Expectation: []string{
					"usersets-user:userset_recursive_alg_combined_w2",
					"usersets-user:userset_recursive_alg_combined_w2_recursive_1",
					"usersets-user:userset_recursive_alg_combined_w2_recursive_2",
					"usersets-user:userset_recursive_alg_combined_w2_recursive_3",
					"usersets-user:userset_recursive_alg_combined_w2_public",
					"usersets-user:userset_recursive_alg_combined_w2_public_recursive_1",
					"usersets-user:userset_recursive_alg_combined_w2_public_recursive_2",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:userset_recursive_alg_combined_w2_rel2_userset",
					Type:     "usersets-user",
					Relation: "userset_recursive_alg_combined_w2",
				},
				Context: validConditionContext,
				Expectation: []string{
					"usersets-user:userset_recursive_alg_combined_w2",
					"usersets-user:userset_recursive_alg_combined_w2_recursive_1",
					"usersets-user:userset_recursive_alg_combined_w2_recursive_2",
					"usersets-user:userset_recursive_alg_combined_w2_recursive_3",
					"usersets-user:userset_recursive_alg_combined_w2_public",
					"usersets-user:userset_recursive_alg_combined_w2_public_recursive_1",
					"usersets-user:userset_recursive_alg_combined_w2_public_recursive_2",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:public",
					Type:     "usersets-user",
					Relation: "userset_recursive_alg_combined_w2",
				},
				Context: validConditionContext,
				Expectation: []string{
					"usersets-user:userset_recursive_alg_combined_w2_public",
					"usersets-user:userset_recursive_alg_combined_w2_public_recursive_1",
					"usersets-user:userset_recursive_alg_combined_w2_public_recursive_2",
				},
			},
		},
	},
}
