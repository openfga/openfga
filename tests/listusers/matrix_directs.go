package listusers

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

var directs = []matrixTest{
	{
		// Concrete users, conditioned users, a conditioned public wildcard, a computed chain, and filter isolation.
		Name: "directs_mixed_conditions_wildcard",
		Tuples: []*openfgav1.TupleKey{
			t3("directs:lu1", "direct_comb", "user:a"),
			t3c("directs:lu1", "direct_comb", "user:b"),
			t3c("directs:lu1", "direct_comb", "user:*"),
			t3("directs:lu1", "direct", "user:c"),
			t3("directs:lu1", "other_rel", "employee:e"),
		},
		Assertions: []matrixAssertion{
			assertion("directs:lu1", "direct_comb", "user", validConditionContext, "user:a", "user:b", "user:*"),
			assertion("directs:lu1", "direct_comb", "user", invalidConditionContext, "user:a"),
			assertion("directs:lu1", "computed_3_times", "user", nil, "user:c"),
			assertion("directs:lu1", "computed_3_times", "employee", nil),
			assertion("directs:lu1", "other_rel", "employee", nil, "employee:e"),
			assertion("directs:lu1", "other_rel", "user", nil),
			failing("directs:lu1", "direct_comb", "user"), // the condition needs `x`
		},
	},
	{
		// The same relation answers differently per filter type.
		Name: "directs_multiple_terminal_types",
		Tuples: []*openfgav1.TupleKey{
			t3("directs:lu2", "direct_mult_types", "user:a"),
			t3("directs:lu2", "direct_mult_types", "employee:e"),
			t3("directs:lu2", "direct_mult_types", "employee:*"),
		},
		Assertions: []matrixAssertion{
			assertion("directs:lu2", "computed_mult_types", "user", nil, "user:a"),
			assertion("directs:lu2", "computed_mult_types", "employee", nil, "employee:e", "employee:*"),
		},
	},
	{
		// Union, intersection with a wildcard operand, nested exclusions. user:b is a negative: it is in the union
		// but not in other_rel.
		Name: "directs_algebra_chain",
		Tuples: []*openfgav1.TupleKey{
			t3("directs:lu3", "direct", "user:a"),
			t3("directs:lu3", "direct_comb", "user:b"),
			t3("directs:lu3", "direct_mult_types", "user:*"),
			t3("directs:lu3", "other_rel", "user:a"),
			t3("directs:lu3", "other_rel", "user:c"),
			t3("directs:lu3", "other_rel", "employee:e"),
		},
		Assertions: []matrixAssertion{
			assertion("directs:lu3", "or_computed_mult_types", "user", nil, "user:a", "user:b", "user:*"),
			// c is in other_rel, and in the union only through the wildcard in direct_mult_types.
			assertion("directs:lu3", "and_computed_mult_types", "user", nil, "user:a", "user:c"),
			assertion("directs:lu3", "butnot_computed", "user", nil, "user:a", "user:c"),
			assertion("directs:lu3", "alg_combined", "user", nil, "user:c"),
			assertion("directs:lu3", "and_computed_mult_types", "employee", nil),
		},
	},
	{
		// Conditions decide which side of an intersection holds. employee is not conditionable on these relations.
		Name: "directs_algebra_conditions",
		Tuples: []*openfgav1.TupleKey{
			t3("directs:lu4", "direct", "user:a"),
			t3("directs:lu4", "other_rel", "user:a"),
			t3("directs:lu4", "direct", "user:b"),
			t3c("directs:lu4", "other_rel", "user:b"),
			t3("directs:lu4", "direct_mult_types", "employee:e"),
			t3("directs:lu4", "other_rel", "employee:e"),
			t3("directs:lu4", "direct_mult_types", "employee:f"),
		},
		Assertions: []matrixAssertion{
			assertion("directs:lu4", "and_computed_mult_types", "user", validConditionContext, "user:a", "user:b"),
			assertion("directs:lu4", "and_computed_mult_types", "user", invalidConditionContext, "user:a"),
			assertion("directs:lu4", "and_computed_mult_types", "employee", validConditionContext, "employee:e"),
			assertion("directs:lu4", "and_computed_mult_types", "employee", invalidConditionContext, "employee:e"),
		},
	},
	{
		// directs-employee: wildcard operands in an intersection, then an exclusion.
		Name: "directs_employee_exclusion_with_wildcard_operand",
		Tuples: []*openfgav1.TupleKey{
			t3("directs-employee:lu6", "direct", "employee:e1"),
			t3("directs-employee:lu6", "direct_wild", "employee:*"),
			t3("directs-employee:lu6", "other_rel", "employee:e2"),
		},
		Assertions: []matrixAssertion{
			assertion("directs-employee:lu6", "and_computed", "employee", nil, "employee:e1", "employee:e2"),
			assertion("directs-employee:lu6", "alg_combined", "employee", nil, "employee:e1"),
		},
	},
	{
		Name: "directs_employee_conditions_wildcard",
		Tuples: []*openfgav1.TupleKey{
			t3("directs-employee:de1", "direct", "employee:a"),
			t3c("directs-employee:de1", "direct", "employee:b"),
			t3c("directs-employee:de1", "direct", "employee:*"),
		},
		Assertions: []matrixAssertion{
			assertion("directs-employee:de1", "computed_3_times", "employee", validConditionContext, "employee:a", "employee:b", "employee:*"),
			assertion("directs-employee:de1", "computed_3_times", "employee", invalidConditionContext, "employee:a"),
			assertion("directs-employee:de1", "computed_3_times", "user", validConditionContext),
		},
	},
}
