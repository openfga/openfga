package listobjects

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	listobjectstest "github.com/openfga/openfga/internal/test/listobjects"
)

var xCond = &openfgav1.RelationshipCondition{Name: "xcond"}
var validConditionContext = MustNewStruct(map[string]any{
	"x": "1",
})
var invalidConditionContext = MustNewStruct(map[string]any{
	"x": "9",
})

var directs = []matrixTest{
	{
		Name: "directs_direct_assignment",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs:direct_1_1", Relation: "direct", User: "user:direct_1"},
			{Object: "directs:direct_1_2", Relation: "direct", User: "user:direct_1"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:direct_1",
					Type:     "directs",
					Relation: "direct",
				},

				Expectation: []string{"directs:direct_1_1", "directs:direct_1_2"},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:direct_no_such_user",
					Type:     "directs",
					Relation: "direct",
				},

				Expectation: []string{},
			},
		},
	},
	{
		Name: "directs_one_terminal_type_wildcard_and_conditions",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs:wildcard_and_condition_1", Relation: "direct_comb", User: "user:direct_comb_1"},
			{Object: "directs:wildcard_and_condition_2", Relation: "direct_comb", User: "user:*"},
			{Object: "directs:wildcard_and_condition_3", Relation: "direct_comb", User: "user:direct_comb_1", Condition: xCond},
			{Object: "directs:wildcard_and_condition_4", Relation: "direct_comb", User: "user:*", Condition: xCond},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:direct_comb_1",
					Type:     "directs",
					Relation: "direct_comb",
				},
				Context: validConditionContext,
				Expectation: []string{
					"directs:wildcard_and_condition_1",
					"directs:wildcard_and_condition_2",
					"directs:wildcard_and_condition_3",
					"directs:wildcard_and_condition_4",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:direct_comb_1",
					Type:     "directs",
					Relation: "direct_comb",
				},
				Context: invalidConditionContext,
				Expectation: []string{
					"directs:wildcard_and_condition_1",
					"directs:wildcard_and_condition_2",
				},
			},
		},
	},
	{
		Name: "directs_multiple_terminal_types",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs:mult_types_1", Relation: "direct_mult_types", User: "employee:mult_types_1"},
			{Object: "directs:mult_types_2", Relation: "direct_mult_types", User: "employee:*"},
			{Object: "directs:mult_types_3", Relation: "direct_mult_types", User: "user:mult_types_1"},
			{Object: "directs:mult_types_4", Relation: "direct_mult_types", User: "user:*"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:mult_types_1",
					Type:     "directs",
					Relation: "direct_mult_types",
				},
				Expectation: []string{
					"directs:mult_types_3",
					"directs:mult_types_4",
				},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "employee:mult_types_1",
					Type:     "directs",
					Relation: "direct_mult_types",
				},
				Expectation: []string{
					"directs:mult_types_1",
					"directs:mult_types_2",
				},
			},
		},
	},
	{
		Name: "directs_algebraic_expression_multiple_terminal_types",
		Tuples: []*openfgav1.TupleKey{
			// Both returned if valid condition context, otherwise neither
			{Object: "directs:alg_expr_1", Relation: "direct", User: "user:alg_expr_1"},
			{Object: "directs:alg_expr_1", Relation: "other_rel", User: "user:*", Condition: xCond},
			{Object: "directs:alg_expr_2", Relation: "direct_mult_types", User: "user:alg_expr_1"},
			{Object: "directs:alg_expr_2", Relation: "other_rel", User: "user:*", Condition: xCond},

			// returned for this employee
			{Object: "directs:alg_expr_2", Relation: "direct_mult_types", User: "employee:*"},
			{Object: "directs:alg_expr_2", Relation: "other_rel", User: "employee:alg_expr_1"},

			// User should have access to both of these with condition, but neither without condition
			{Object: "directs:alg_expr_3", Relation: "direct_comb", User: "user:*"},
			{Object: "directs:alg_expr_3", Relation: "other_rel", User: "user:alg_expr_1", Condition: xCond},
			{Object: "directs:alg_expr_4", Relation: "direct_comb", User: "user:*", Condition: xCond},
			{Object: "directs:alg_expr_4", Relation: "other_rel", User: "user:*"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:alg_expr_1",
					Type:     "directs",
					Relation: "and_computed_mult_types",
				},
				Expectation: []string{
					"directs:alg_expr_1",
					"directs:alg_expr_2",
					"directs:alg_expr_3",
					"directs:alg_expr_4",
				},
				Context: validConditionContext,
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:alg_expr_1",
					Type:     "directs",
					Relation: "and_computed_mult_types",
				},
				Expectation: []string{},
				Context:     invalidConditionContext,
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "employee:alg_expr_1",
					Type:     "directs",
					Relation: "and_computed_mult_types",
				},
				Expectation: []string{
					"directs:alg_expr_2",
				},
			},
		},
	},
	{
		Name: "directs_nested_algebraic_expressions",
		Tuples: []*openfgav1.TupleKey{
			// Will exclude due to "but not computed_3_times" in the resolution path
			{Object: "directs:nested_alg_1", Relation: "direct", User: "user:nested_alg_1"},
			{Object: "directs:nested_alg_1", Relation: "other_rel", User: "user:nested_alg_1"},

			// Will exclude due to "but not computed_comb" in the resolution path
			{Object: "directs:nested_alg_2", Relation: "direct_comb", User: "user:*"},
			{Object: "directs:nested_alg_2", Relation: "other_rel", User: "user:*", Condition: xCond},

			// Should return for both types
			{Object: "directs:nested_alg_3", Relation: "direct_mult_types", User: "user:nested_alg_1"},
			{Object: "directs:nested_alg_3", Relation: "direct_mult_types", User: "employee:*"},
			{Object: "directs:nested_alg_3", Relation: "other_rel", User: "user:*", Condition: xCond},
			{Object: "directs:nested_alg_3", Relation: "other_rel", User: "employee:nested_alg_1"},

			// Will exclude due to lack of other_rel
			{Object: "directs:nested_alg_4", Relation: "direct", User: "user:nested_alg_1"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:nested_alg_1",
					Type:     "directs",
					Relation: "alg_combined",
				},
				Expectation: []string{"directs:nested_alg_3"},
				Context:     validConditionContext,
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "employee:nested_alg_1",
					Type:     "directs",
					Relation: "alg_combined",
				},
				Expectation: []string{"directs:nested_alg_3"},
			},
		},
	},
	{
		Name: "directs_alg_combined_oneline",
		Tuples: []*openfgav1.TupleKey{
			// excluded, only satisfies left side of AND
			{Object: "directs:alg_combined_oneline_1", Relation: "direct", User: "user:alg_combined_oneline_1"},

			// satisfies both sides of AND relation for this user
			{Object: "directs:alg_combined_oneline_2", Relation: "direct_mult_types", User: "user:alg_combined_oneline_1"},
			{Object: "directs:alg_combined_oneline_2", Relation: "direct_comb", User: "user:alg_combined_oneline_1"},
			{Object: "directs:alg_combined_oneline_2", Relation: "other_rel", User: "user:alg_combined_oneline_1"},

			// excluded for this employee, these relations are both on one side of the AND
			{Object: "directs:alg_combined_oneline_2", Relation: "other_rel", User: "employee:alg_combined_oneline_1"},
			{Object: "directs:alg_combined_oneline_2", Relation: "direct_mult_types", User: "employee:alg_combined_oneline_1"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:alg_combined_oneline_1",
					Type:     "directs",
					Relation: "alg_combined_oneline",
				},
				Expectation: []string{"directs:alg_combined_oneline_2"},
				Context:     validConditionContext,
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "employee:alg_combined_oneline_1",
					Type:     "directs",
					Relation: "alg_combined_oneline",
				},
				Expectation: nil,
			},
		},
	},
}
