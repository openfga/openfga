package listusers

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

var complexity = []matrixTest{
	{
		// complexity3: a TTU whose target is a userset relation.
		Name: "complexity3_ttu_userset",
		Tuples: []*openfgav1.TupleKey{
			t3("complexity3:c3a", "userset_parent", "usersets-user:u3a"),
			t3("usersets-user:u3a", "userset", "directs:d3a#direct_comb"),
			t3("directs:d3a", "direct_comb", "user:a"),
			t3("complexity3:c3a", "ttu_userset_public", "user:*"),
			// a second object that has no parent
			t3("complexity3:c3b", "ttu_userset_public", "user:*"),
		},
		Assertions: []matrixAssertion{
			assertion("complexity3:c3a", "ttu_userset", "user", nil, "user:a"),
			assertion("complexity3:c3a", "ttu_userset_public", "user", nil, "user:*", "user:a"),
			assertion("complexity3:c3b", "ttu_userset", "user", nil),
			assertion("complexity3:c3b", "ttu_userset_public", "user", nil, "user:*"),
		},
	},
	{
		// An intersection nested in a TTU of a userset, with several parents: it is evaluated per parent.
		// p1 grants a (direct and user_rel1); p2 has c directly and b only on the other side.
		Name: "complexity3_ttu_userset_intersection_is_per_parent",
		Tuples: []*openfgav1.TupleKey{
			t3("complexity3:ci1", "userset_parent", "usersets-user:cip1"),
			t3("complexity3:ci1", "userset_parent", "usersets-user:cip2"),
			t3("usersets-user:cip1", "userset_intersect_mixed", "user:a"),
			t3("usersets-user:cip1", "userset_intersect_mixed", "user:b"),
			t3("usersets-user:cip1", "user_rel1", "user:a"),
			t3("usersets-user:cip2", "userset_intersect_mixed", "user:c"),
			t3("usersets-user:cip2", "user_rel1", "user:b"),
		},
		Assertions: []matrixAssertion{
			assertion("complexity3:ci1", "ttu_userset_intersect_mixed", "user", nil, "user:a"),
		},
	},
	{
		// An exclusion nested in a TTU of a userset, with several parents: a is subtracted in p1 only.
		Name: "complexity3_ttu_userset_exclusion_is_per_parent",
		Tuples: []*openfgav1.TupleKey{
			t3("complexity3:ce1", "userset_parent", "usersets-user:cep1"),
			t3("complexity3:ce1", "userset_parent", "usersets-user:cep2"),
			// userset_exclude_mixed = [user, user:*, directs#alg_combined_oneline] but not userset_intersect_mixed
			t3("usersets-user:cep1", "userset_exclude_mixed", "user:a"),
			t3("usersets-user:cep1", "userset_exclude_mixed", "user:b"),
			t3("usersets-user:cep1", "userset_intersect_mixed", "user:a"),
			t3("usersets-user:cep1", "user_rel1", "user:a"),
			t3("usersets-user:cep2", "userset_exclude_mixed", "user:a"),
		},
		Assertions: []matrixAssertion{
			assertion("complexity3:ce1", "ttu_userset_exclude_mixed", "user", nil, "user:a", "user:b"),
		},
	},
	{
		// complexity4: a TTU of a TTU of a userset.
		Name: "complexity4_ttu_userset_ttu",
		Tuples: []*openfgav1.TupleKey{
			t3("complexity4:c4a", "parent", "complexity3:c4p"),
			t3("complexity3:c4p", "userset_ttu", "ttus:c4t#ttu_direct"),
			t3("ttus:c4t", "direct_parent", "directs:c4d"),
			t3("directs:c4d", "direct", "user:a"),
		},
		Assertions: []matrixAssertion{
			assertion("complexity4:c4a", "ttu_userset_ttu", "user", nil, "user:a"),
			assertion("complexity4:c4a", "or_complex4", "user", nil, "user:a"),
		},
	},
}
