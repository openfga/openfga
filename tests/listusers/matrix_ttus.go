package listusers

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

var ttus = []matrixTest{
	{
		// The intersection is evaluated per parent. p1 and p2 each satisfy one side of
		// directs#alg_combined_oneline, with the users swapped: merging the parents first would wrongly grant a and b.
		Name: "ttus_intersection_is_per_parent",
		Tuples: []*openfgav1.TupleKey{
			t3("ttus:lu5", "mult_parent_types", "directs:p1"),
			t3("ttus:lu5", "mult_parent_types", "directs:p2"),
			t3("ttus:lu5", "mult_parent_types", "directs:p3"),
			t3("directs:p1", "direct", "user:a"),
			t3("directs:p1", "other_rel", "user:b"),
			t3("directs:p2", "direct", "user:b"),
			t3("directs:p2", "other_rel", "user:a"),
			t3("directs:p3", "direct", "user:c"),
			t3("directs:p3", "other_rel", "user:c"),
		},
		Assertions: []matrixAssertion{
			assertion("ttus:lu5", "ttu_alg_combined_oneline", "user", nil, "user:c"),
			assertion("ttus:lu5", "ttu_other_rel", "user", nil, "user:a", "user:b", "user:c"),
		},
	},
	{
		// The exclusion is evaluated per parent: a is excluded in p1 but granted in p2.
		Name: "ttus_exclusion_is_per_parent",
		Tuples: []*openfgav1.TupleKey{
			t3("ttus:ex1", "mult_parent_types", "directs:x1"),
			t3("ttus:ex1", "mult_parent_types", "directs:x2"),
			// x1: alg_combined_oneline = (computed_3_times or computed_comb) and (computed_mult_types or other_rel)
			t3("directs:x1", "direct", "user:a"),
			t3("directs:x1", "other_rel", "user:a"),
			t3("directs:x1", "direct", "user:b"),
			// x2: only other_rel, so the left side is empty
			t3("directs:x2", "other_rel", "user:a"),
			t3("directs:x2", "other_rel", "user:c"),
		},
		Assertions: []matrixAssertion{
			assertion("ttus:ex1", "ttu_alg_combined_oneline", "user", nil, "user:a"),
			assertion("ttus:ex1", "ttu_alg_combined", "user", nil),
		},
	},
	{
		Name: "ttus_conditioned_tupleset",
		Tuples: []*openfgav1.TupleKey{
			t3c("ttus:lu11", "mult_parent_types", "directs:q1"),
			t3("directs:q1", "other_rel", "user:a"),
		},
		Assertions: []matrixAssertion{
			assertion("ttus:lu11", "ttu_other_rel", "user", validConditionContext, "user:a"),
			assertion("ttus:lu11", "ttu_other_rel", "user", invalidConditionContext),
		},
	},
	{
		// The same user through two TTU branches is returned once.
		Name: "ttus_duplicate_parents",
		Tuples: []*openfgav1.TupleKey{
			t3("ttus:dp1", "direct_parent", "directs:dq1"),
			t3("ttus:dp1", "mult_parent_types", "directs:dq1"),
			t3("ttus:dp1", "mult_parent_types", "directs-employee:dq2"),
			t3("directs:dq1", "direct", "user:a"),
			t3("directs-employee:dq2", "direct", "employee:e"),
		},
		Assertions: []matrixAssertion{
			assertion("ttus:dp1", "duplicate_ttu", "user", nil, "user:a"),
			assertion("ttus:dp1", "duplicate_ttu", "employee", nil, "employee:e"),
		},
	},
	{
		// [user, user:*] and user_rel4 and (A but not B): an inline algebraic expression over TTUs.
		Name: "ttus_alg_inline",
		Tuples: []*openfgav1.TupleKey{
			t3("ttus:ai1", "alg_inline", "user:a"),
			t3("ttus:ai1", "alg_inline", "user:b"),
			t3("ttus:ai1", "alg_inline", "user:c"),
			t3("ttus:ai1", "user_rel1", "user:a"),
			t3("ttus:ai1", "user_rel2", "user:b"),
			t3("ttus:ai1", "user_rel3", "user:b"),
			t3("ttus:ai1", "direct_parent", "directs:ad"),
			t3("ttus:ai1", "mult_parent_types", "directs:am"),
			t3("directs:ad", "direct", "user:a"),
			t3("directs:ad", "direct", "user:b"),
			t3("directs:ad", "direct", "user:c"),
			t3("directs:am", "other_rel", "user:b"),
		},
		Assertions: []matrixAssertion{
			// a: in all three; b: excluded by the third; c: fails user_rel4
			assertion("ttus:ai1", "alg_inline", "user", nil, "user:a"),
			assertion("ttus:ai1", "user_rel4", "user", nil, "user:a", "user:b"),
		},
	},
	{
		// A data cycle through a recursive TTU.
		Name: "ttus_recursive_with_data_cycle",
		Tuples: []*openfgav1.TupleKey{
			t3("ttus:r1", "ttu_parent", "ttus:r2"),
			t3("ttus:r2", "ttu_parent", "ttus:r3"),
			t3("ttus:r3", "ttu_parent", "ttus:r1"),
			t3("ttus:r2", "ttu_recursive", "user:a"),
			t3("ttus:r3", "ttu_recursive", "user:b"),
		},
		Assertions: []matrixAssertion{
			assertion("ttus:r1", "ttu_recursive", "user", nil, "user:a", "user:b"),
			assertion("ttus:r2", "ttu_recursive", "user", nil, "user:a", "user:b"),
		},
	},
	{
		Name: "ttus_recursive_public",
		Tuples: []*openfgav1.TupleKey{
			t3("ttus:rp1", "ttu_parent", "ttus:rp2"),
			t3("ttus:rp2", "ttu_recursive_public", "user:*"),
			t3("ttus:rp3", "ttu_parent", "ttus:rp4"),
		},
		Assertions: []matrixAssertion{
			assertion("ttus:rp1", "ttu_recursive_public", "user", nil, "user:*"),
			assertion("ttus:rp3", "ttu_recursive_public", "user", nil),
		},
	},
	{
		// ttu_recursive_combined_w3: three terminal types, itself, and a second TTU.
		Name: "ttus_recursive_combined_w3",
		Tuples: []*openfgav1.TupleKey{
			t3("ttus:cw1", "ttu_parent", "ttus:cw2"),
			t3("ttus:cw1", "direct_parent", "directs:cwd"),
			t3("ttus:cw2", "ttu_recursive_combined_w3", "user:a"),
			t3("ttus:cw2", "ttu_recursive_combined_w3", "employee:e"),
			t3("directs:cwd", "direct", "user:b"),
			t3("ttus:cw3", "ttu_recursive_combined_w3", "user:*"),
		},
		Assertions: []matrixAssertion{
			assertion("ttus:cw1", "ttu_recursive_combined_w3", "user", nil, "user:a", "user:b"),
			assertion("ttus:cw1", "ttu_recursive_combined_w3", "employee", nil, "employee:e"),
			assertion("ttus:cw3", "ttu_recursive_combined_w3", "user", nil, "user:*"),
		},
	},
	{
		// A tuple cycle through a TTU across types: ttus -> directs -> ttus.
		Name: "ttus_tuple_cycle_len2_ttu",
		Tuples: []*openfgav1.TupleKey{
			t3("ttus:tc1", "mult_parent_types", "directs:tcd1"),
			t3("directs:tcd1", "cycle_len2_parent", "ttus:tc1"),
			t3("directs:tcd1", "tuple_cycle_len2_ttu", "user:a"),
			t3("ttus:tc1", "tuple_cycle_len2_ttu", "employee:e"),
		},
		Assertions: []matrixAssertion{
			assertion("ttus:tc1", "tuple_cycle_len2_ttu", "user", nil, "user:a"),
			assertion("ttus:tc1", "tuple_cycle_len2_ttu", "employee", nil, "employee:e"),
			assertion("directs:tcd1", "tuple_cycle_len2_ttu", "user", nil, "user:a"),
			assertion("directs:tcd1", "tuple_cycle_len2_ttu", "employee", nil, "employee:e"),
		},
	},
}
