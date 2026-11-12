package listusers

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

const legacyUsersetBelow = "ListUsers expands below the userset tuple and fails on the missing condition context of tuples there; Check never looks below a userset filter match"

const legacyUsersetFilter = "Check expands a userset filter only through recursive or tuple-cycle edges and never returns the queried object itself; ListUsers also descends through non-recursive edges and returns the object itself"

var usersets = []matrixTest{
	{
		// A userset assigned to a relation is expanded for user filters, and returned as is for userset filters.
		Name: "usersets_expansion_and_filters",
		Tuples: []*openfgav1.TupleKey{
			t3("usersets-user:uu1", "userset", "directs:d1#direct_comb"),
			t3("usersets-user:uu1", "userset", "directs-employee:e1#direct"),
			t3("directs:d1", "direct_comb", "user:a"),
			t3c("directs:d1", "direct_comb", "user:b"),
			t3("directs:d2", "direct_comb", "user:z"), // not linked to uu1
			t3("directs-employee:e1", "direct", "employee:x"),
		},
		Assertions: []matrixAssertion{
			assertion("usersets-user:uu1", "userset_computed", "user", validConditionContext, "user:a", "user:b"),
			assertion("usersets-user:uu1", "userset_computed", "user", invalidConditionContext, "user:a"),
			assertion("usersets-user:uu1", "userset_computed", "employee", validConditionContext, "employee:x"),
			// Check does not look below the userset tuple, so it never needs the context of d1's conditioned tuple.
			diverges(assertion("usersets-user:uu1", "userset", "directs#direct_comb", nil, "directs:d1#direct_comb"), legacyUsersetBelow),
			diverges(assertion("usersets-user:uu1", "userset", "directs-employee#direct", nil, "directs-employee:e1#direct"), legacyUsersetBelow),
			// the wrong userset type for this relation
			assertion("usersets-user:uu1", "userset", "directs#other_rel", nil),
		},
	},
	{
		// A condition on the userset tuple itself.
		Name: "usersets_condition_on_userset_tuple",
		Tuples: []*openfgav1.TupleKey{
			t3c("usersets-user:uc1", "userset_combined_cond", "directs:ud#computed_mult_types"),
			t3("usersets-user:uc1", "userset_combined_cond", "directs-employee:ue#computed_3_times"),
			t3("directs:ud", "direct_mult_types", "user:a"),
			t3("directs-employee:ue", "direct", "employee:e"),
		},
		Assertions: []matrixAssertion{
			assertion("usersets-user:uc1", "userset_combined_cond", "user", validConditionContext, "user:a"),
			assertion("usersets-user:uc1", "userset_combined_cond", "user", invalidConditionContext),
			assertion("usersets-user:uc1", "userset_combined_cond", "employee", validConditionContext, "employee:e"),
			assertion("usersets-user:uc1", "userset_combined_cond", "directs#computed_mult_types", validConditionContext, "directs:ud#computed_mult_types"),
			assertion("usersets-user:uc1", "userset_combined_cond", "directs#computed_mult_types", invalidConditionContext),
		},
	},
	{
		// userset_alg_combined and userset_alg_combined_oneline: the userset target is itself an algebraic relation.
		Name: "usersets_alg_combined",
		Tuples: []*openfgav1.TupleKey{
			t3("usersets-user:ua1", "userset_alg_combined", "directs:ua_d1#alg_combined"),
			t3("usersets-user:ua1", "userset_alg_combined", "directs:ua_d2#alg_combined"),
			t3("usersets-user:ua1", "userset_alg_combined", "directs-employee:ua_e1#alg_combined"),
			// satisfies directs#alg_combined
			t3("directs:ua_d1", "direct_mult_types", "user:a"),
			t3("directs:ua_d1", "other_rel", "user:a"),
			// does not: no direct_mult_types
			t3("directs:ua_d2", "other_rel", "user:b"),
			// satisfies directs-employee#alg_combined
			t3("directs-employee:ua_e1", "direct", "employee:e"),
			t3("directs-employee:ua_e1", "direct_wild", "employee:*"),
		},
		Assertions: []matrixAssertion{
			assertion("usersets-user:ua1", "userset_alg_combined", "user", nil, "user:a"),
			assertion("usersets-user:ua1", "userset_alg_combined", "employee", nil, "employee:e"),
			assertion("usersets-user:ua1", "userset_alg_combined", "directs#alg_combined", nil, "directs:ua_d1#alg_combined", "directs:ua_d2#alg_combined"),
		},
	},
	{
		// Wildcard minus intersection: b is a member only through the wildcard, a is excluded.
		Name: "usersets_wildcard_minus_intersection",
		Tuples: []*openfgav1.TupleKey{
			t3("usersets-user:lu7", "userset_exclude_mixed", "user:*"),
			t3("usersets-user:lu7", "userset_intersect_mixed", "user:a"),
			t3("usersets-user:lu7", "userset_intersect_mixed", "user:b"),
			t3("usersets-user:lu7", "user_rel1", "user:a"),
			t3("usersets-user:lu7", "user_rel2", "user:b"),
		},
		Assertions: []matrixAssertion{
			assertion("usersets-user:lu7", "userset_intersect_mixed", "user", nil, "user:a"),
			assertion("usersets-user:lu7", "userset_exclude_mixed", "user", nil, "user:*"),
		},
	},
	{
		// Wildcard intersected with a wildcard keeps the explicit user.
		Name: "usersets_wildcard_intersection",
		Tuples: []*openfgav1.TupleKey{
			t3("usersets-user:lu12", "userset_intersect_mixed", "user:*"),
			t3("usersets-user:lu12", "user_rel1", "user:*"),
			t3("usersets-user:lu12", "user_rel1", "user:a"),
		},
		Assertions: []matrixAssertion{
			assertion("usersets-user:lu12", "userset_intersect_mixed", "user", nil, "user:*", "user:a"),
		},
	},
	{
		// Intersection with conditions on the right-hand side: user_rel2 and user_rel3.
		Name: "usersets_intersection_conditions",
		Tuples: []*openfgav1.TupleKey{
			t3("usersets-user:ic1", "userset_intersect_mixed", "user:a"),
			t3("usersets-user:ic1", "userset_intersect_mixed", "user:b"),
			t3c("usersets-user:ic1", "user_rel2", "user:a"),
			t3c("usersets-user:ic1", "user_rel3", "user:*"),
			t3("usersets-user:ic1", "user_rel2", "user:b"),
			t3("usersets-user:ic1", "user_rel3", "user:b"),
		},
		Assertions: []matrixAssertion{
			assertion("usersets-user:ic1", "userset_intersect_mixed", "user", validConditionContext, "user:a", "user:b"),
			assertion("usersets-user:ic1", "userset_intersect_mixed", "user", invalidConditionContext, "user:b"),
		},
	},
	{
		// Recursive userset on the same relation, and the userset filter rules of Check:
		//  - a recursive edge keeps expanding: lu8b and lu8c are returned;
		//  - the queried object itself is never returned;
		//  - a non-recursive entry into the recursive userset stops at the first level.
		Name: "usersets_recursive_userset_filter",
		Tuples: []*openfgav1.TupleKey{
			t3("usersets-user:lu8", "userset_recursive", "usersets-user:lu8b#userset_recursive"),
			t3("usersets-user:lu8b", "userset_recursive", "usersets-user:lu8c#userset_recursive"),
			t3("usersets-user:lu8c", "userset_recursive", "user:z"),
			t3("usersets-user:lu10", "userset_to_recursive", "usersets-user:lu8#userset_recursive"),
		},
		Assertions: []matrixAssertion{
			diverges(assertion("usersets-user:lu8", "userset_recursive", "usersets-user#userset_recursive", nil,
				"usersets-user:lu8b#userset_recursive", "usersets-user:lu8c#userset_recursive"), legacyUsersetFilter),
			assertion("usersets-user:lu8", "userset_recursive", "user", nil, "user:z"),
			diverges(assertion("usersets-user:lu10", "userset_to_recursive", "usersets-user#userset_recursive", nil,
				"usersets-user:lu8#userset_recursive"), legacyUsersetFilter),
			assertion("usersets-user:lu10", "userset_to_recursive", "user", nil, "user:z"),
		},
	},
	{
		// A cycle in the data (not only in the model): the recursion terminates and each user is returned once.
		Name: "usersets_recursive_with_data_cycle",
		Tuples: []*openfgav1.TupleKey{
			t3("usersets-user:rc1", "userset_recursive", "usersets-user:rc2#userset_recursive"),
			t3("usersets-user:rc2", "userset_recursive", "usersets-user:rc3#userset_recursive"),
			t3("usersets-user:rc3", "userset_recursive", "usersets-user:rc1#userset_recursive"),
			t3("usersets-user:rc1", "userset_recursive", "user:a"),
			t3("usersets-user:rc3", "userset_recursive", "user:c"),
		},
		Assertions: []matrixAssertion{
			assertion("usersets-user:rc1", "userset_recursive", "user", nil, "user:a", "user:c"),
			assertion("usersets-user:rc2", "userset_recursive", "user", nil, "user:a", "user:c"),
			// rc3 holds rc1#userset_recursive as a subject, so through the data cycle rc1 is legitimately a member of itself.
			assertion("usersets-user:rc1", "userset_recursive", "usersets-user#userset_recursive", nil,
				"usersets-user:rc1#userset_recursive", "usersets-user:rc2#userset_recursive", "usersets-user:rc3#userset_recursive"),
		},
	},
	{
		// The public wildcard found only at the end of a recursive chain.
		Name: "usersets_recursive_public",
		Tuples: []*openfgav1.TupleKey{
			t3("usersets-user:urp1", "userset_recursive_public", "usersets-user:urp2#userset_recursive_public"),
			t3("usersets-user:urp2", "userset_recursive_public", "user:*"),
			t3("usersets-user:urp3", "userset_recursive_public", "usersets-user:urp4#userset_recursive_public"),
		},
		Assertions: []matrixAssertion{
			assertion("usersets-user:urp1", "userset_recursive_public", "user", nil, "user:*"),
			assertion("usersets-user:urp2", "userset_recursive_public", "user", nil, "user:*"),
			assertion("usersets-user:urp3", "userset_recursive_public", "user", nil),
		},
	},
	{
		// userset_recursive_combined_w3: user, wildcard, employee, itself, and another userset relation, all in one.
		Name: "usersets_recursive_combined_w3",
		Tuples: []*openfgav1.TupleKey{
			t3("usersets-user:w1", "userset_recursive_combined_w3", "usersets-user:w2#userset_recursive_combined_w3"),
			t3("usersets-user:w1", "userset_recursive_combined_w3", "usersets-user:w3#userset"),
			t3("usersets-user:w1", "userset_recursive_combined_w3", "user:c"),
			t3("usersets-user:w2", "userset_recursive_combined_w3", "user:a"),
			t3("usersets-user:w2", "userset_recursive_combined_w3", "employee:e"),
			t3("usersets-user:w3", "userset", "directs:dw#direct_comb"),
			t3("directs:dw", "direct_comb", "user:b"),
			t3("usersets-user:w4", "userset_recursive_combined_w3", "user:*"),
			t3("usersets-user:w4", "userset_recursive_combined_w3", "user:d"),
		},
		Assertions: []matrixAssertion{
			assertion("usersets-user:w1", "userset_recursive_combined_w3", "user", nil, "user:a", "user:b", "user:c"),
			assertion("usersets-user:w1", "userset_recursive_combined_w3", "employee", nil, "employee:e"),
			assertion("usersets-user:w4", "userset_recursive_combined_w3", "user", nil, "user:*", "user:d"),
			assertion("usersets-user:w4", "userset_recursive_combined_w3", "employee", nil),
		},
	},
	{
		// Tuple cycle across two types, through usersets.
		Name: "usersets_tuple_cycle_len2",
		Tuples: []*openfgav1.TupleKey{
			t3("directs:c1", "tuple_cycle_len2_userset", "usersets-user:c2#tuple_cycle_len2_userset"),
			t3("usersets-user:c2", "tuple_cycle_len2_userset", "directs:c1#tuple_cycle_len2_userset"),
			t3("usersets-user:c2", "tuple_cycle_len2_userset", "user:a"),
			t3("directs:c1", "tuple_cycle_len2_userset", "employee:e"),
		},
		Assertions: []matrixAssertion{
			assertion("directs:c1", "tuple_cycle_len2_userset", "user", nil, "user:a"),
			assertion("directs:c1", "tuple_cycle_len2_userset", "employee", nil, "employee:e"),
			assertion("directs:c1", "tuple_cycle_len2_userset", "usersets-user#tuple_cycle_len2_userset", nil, "usersets-user:c2#tuple_cycle_len2_userset"),
		},
	},
	{
		// Tuple cycle across three types: usersets-user -> directs -> complexity3 -> (ttu) usersets-user.
		Name: "usersets_tuple_cycle_len3",
		Tuples: []*openfgav1.TupleKey{
			t3("usersets-user:t1", "tuple_cycle_len3", "directs:t2#tuple_cycle_len3"),
			t3("directs:t2", "tuple_cycle_len3", "complexity3:t3#tuple_cycle_len3"),
			t3("complexity3:t3", "userset_parent", "usersets-user:t1"),
			t3("complexity3:t3", "tuple_cycle_len3", "user:a"),
			t3("directs:t2", "tuple_cycle_len3", "user:b"),
			t3("directs:t2", "tuple_cycle_len3", "employee:e"),
		},
		Assertions: []matrixAssertion{
			assertion("usersets-user:t1", "tuple_cycle_len3", "user", nil, "user:a", "user:b"),
			assertion("usersets-user:t1", "tuple_cycle_len3", "employee", nil, "employee:e"),
			assertion("directs:t2", "tuple_cycle_len3", "user", nil, "user:a", "user:b"),
			assertion("usersets-user:t1", "tuple_cycle_len3", "directs#tuple_cycle_len3", nil, "directs:t2#tuple_cycle_len3"),
			assertion("usersets-user:t1", "tuple_cycle_len3", "complexity3#tuple_cycle_len3", nil, "complexity3:t3#tuple_cycle_len3"),
		},
	},
}
