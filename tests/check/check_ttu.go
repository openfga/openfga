package check

import (
	"github.com/openfga/openfga/internal/condition"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	checktest "github.com/openfga/openfga/internal/test/check"
)

var ttuCompleteTestingModelTest = []*stage{
	{
		Name: "direct_pa_direct_ch",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:ttu_1", Relation: "direct", User: "user:valid"},
			{Object: "ttus:1", Relation: "mult_parent_types", User: "directs-user:ttu_1"},
			{Object: "directs-user:ttu_1_invalid", Relation: "direct", User: "user:ttu_invalid_group"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:1", Relation: "direct_pa_direct_ch", User: "user:valid"},
				Expectation: true,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:1", Relation: "direct_pa_direct_ch", User: "user:invalid"},
				Expectation: false,
			},
			{
				Name:        "invalid_group",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:1", Relation: "direct_pa_direct_ch", User: "user:ttu_invalid_group"},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:invalid", Relation: "direct_pa_direct_ch", User: "user:valid"},
				Expectation: false,
			},
		},
	},
	{
		Name: "or_comp_from_direct_parent",
		Tuples: []*openfgav1.TupleKey{
			// FIXME - add more cases
			{Object: "directs-user:ttu_2", Relation: "direct", User: "user:valid"},
			{Object: "ttus:2", Relation: "direct_parent", User: "directs-user:ttu_2"},
			{Object: "directs-user:ttu_2_invalid", Relation: "direct", User: "user:ttu_invalid_group"},
			{Object: "directs-user:ttu_2_all", Relation: "direct_wild", User: "user:*"},
			{Object: "ttus:2_all", Relation: "direct_parent", User: "directs-user:ttu_2_all"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "or_computed_valid",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:2", Relation: "or_comp_from_direct_parent", User: "user:valid"},
				Expectation: true,
			},
			{
				Name:        "or_computed_valid_second_child_match",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:2_all", Relation: "or_comp_from_direct_parent", User: "user:valid"},
				Expectation: true,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:2", Relation: "or_comp_from_direct_parent", User: "user:ttu_invalid_group"},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:invalid", Relation: "or_comp_from_direct_parent", User: "user:valid"},
				Expectation: false,
			},
		},
	},
	{
		Name: "and_comp_from_direct_parent",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:ttu_3_all", Relation: "direct_cond", User: "user:valid", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "directs-user:ttu_3_all", Relation: "direct_wild", User: "user:*"},
			{Object: "directs-user:ttu_3_partial", Relation: "direct_cond", User: "user:partial", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "ttus:3", Relation: "direct_cond_parent", User: "directs-user:ttu_3_all", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "ttus:3", Relation: "direct_cond_parent", User: "directs-user:ttu_3_partial", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "and_computed_valid",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:3", Relation: "and_comp_from_direct_parent", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:3", Relation: "and_comp_from_direct_parent", User: "user:ttu_invalid_group"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:        "invalid_user_partial",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:3", Relation: "and_comp_from_direct_parent", User: "user:partial"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:invalid", Relation: "and_comp_from_direct_parent", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
		},
	},
	{
		Name: "butnot_comp_from_direct_parent",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:ttu_4_only_wild", Relation: "direct_wild_cond", User: "user:*", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "directs-user:both_wild_computed", Relation: "direct", User: "user:both_wild_computed"},
			{Object: "directs-user:both_wild_computed", Relation: "direct_wild_cond", User: "user:*", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "directs-user:ttu_4_only_computed", Relation: "direct", User: "user:only_computed"},
			{Object: "ttus:4_only_wild", Relation: "direct_cond_parent", User: "directs-user:ttu_4_only_wild", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "ttus:4_both_wild_computed", Relation: "direct_cond_parent", User: "directs-user:both_wild_computed", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "ttus:4_only_computed", Relation: "direct_cond_parent", User: "directs-user:ttu_4_only_computed", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "butnot_ok",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:4_only_wild", Relation: "butnot_comp_from_direct_parent", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "butnot_both_match",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:4_both_wild_computed", Relation: "butnot_comp_from_direct_parent", User: "user:both_wild_computed"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:        "butnot_only_computed",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:4_only_computed", Relation: "butnot_comp_from_direct_parent", User: "user:only_computed"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:invalid", Relation: "butnot_comp_from_direct_parent", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
		},
	},
	{
		Name: "userset_pa_userset_ch",
		Tuples: []*openfgav1.TupleKey{
			{Object: "ttus:5", Relation: "userset_parent", User: "usersets-user:ttu_5"},
			{Object: "usersets-user:ttu_5", Relation: "userset", User: "directs-user:ttu_5#direct"},
			{Object: "directs-user:ttu_5", Relation: "direct", User: "user:valid"},
			{Object: "directs-user:ttu_5_unconnected", Relation: "direct", User: "user:ttu_5_unconnected_directuser"},
			{Object: "directs-user:ttu_5_unconnected_userset", Relation: "direct", User: "user:ttu_5_unconnected_userset"},
			{Object: "usersets-user:ttu_5_unconnected_userset", Relation: "userset", User: "directs-user:ttu_5_unconnected_userset#direct"},
			{Object: "ttus:5_empty_userset", Relation: "userset_parent", User: "usersets-user:ttu_5_empty_userset"},
			{Object: "ttus:5_empty_direct", Relation: "userset_parent", User: "usersets-user:ttu_5_empty_direct"},
			{Object: "usersets-user:ttu_5_empty_direct", Relation: "userset", User: "directs-user:ttu_5_empty_direct#direct"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "userset_parent_userset_ch",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:5", Relation: "userset_pa_userset_ch", User: "user:valid"},
				Expectation: true,
			},
			{
				Name:        "user_directuser_not_connected",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:5", Relation: "userset_pa_userset_ch", User: "user:unconnected_user_directuser"},
				Expectation: false,
			},
			{
				Name:        "directuser_userset_not_connected",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:5", Relation: "userset_pa_userset_ch", User: "user:ttu_5_unconnected_directuser"},
				Expectation: false,
			},
			{
				Name:        "userset_ttu_not_connected",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:5", Relation: "userset_pa_userset_ch", User: "user:ttu_5_unconnected_userset"},
				Expectation: false,
			},
			{
				Name:        "empty_userset",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:5_empty_userset", Relation: "userset_pa_userset_ch", User: "user:valid"},
				Expectation: false,
			},
			{
				Name:        "empty_direct",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:5_empty_direct", Relation: "userset_pa_userset_ch", User: "user:valid"},
				Expectation: false,
			},
		},
	},
	{
		Name: "userset_pa_userset_comp_ch",
		Tuples: []*openfgav1.TupleKey{
			{Object: "ttus:6", Relation: "userset_parent", User: "usersets-user:ttu_6"},
			{Object: "usersets-user:ttu_6", Relation: "userset_to_computed", User: "directs-user:ttu_6#computed"},
			{Object: "directs-user:ttu_6", Relation: "direct", User: "user:valid"},
			// user not part of userset
			{Object: "ttus:6_disconnected_1", Relation: "userset_parent", User: "usersets-user:ttu_6_unconnected"},
			{Object: "usersets-user:ttu_6_unconnected", Relation: "userset_to_computed", User: "directs-user:ttu_6_unconnected#computed"},
			// parent not linked to anything
			{Object: "ttus:6_disconnected_2", Relation: "userset_parent", User: "usersets-user:ttu_6_disconnected_2"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "path_to_user",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:6", Relation: "userset_pa_userset_comp_ch", User: "user:valid"},
				Expectation: true,
			},
			{
				Name:        "no_path_to_user",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:6", Relation: "userset_pa_userset_comp_ch", User: "user:invalid"},
				Expectation: false,
			},
			{
				Name:        "no_path_to_user_because_not_part_of_userset",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:6_disconnected_1", Relation: "userset_pa_userset_comp_ch", User: "user:valid"},
				Expectation: false,
			},
			{
				Name:        "no_path_to_user_because_parent_not_linked_to_anything",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:6_disconnected_2", Relation: "userset_pa_userset_comp_ch", User: "user:valid"},
				Expectation: false,
			},
		},
	},
	{
		Name: "userset_pa_userset_comp_cond_ch",
		Tuples: []*openfgav1.TupleKey{
			{Object: "ttus:7", Relation: "userset_parent", User: "usersets-user:ttu_7"},
			{Object: "usersets-user:ttu_7", Relation: "userset_to_computed_cond", User: "directs-user:ttu_7#computed_cond"},
			{Object: "directs-user:ttu_7", Relation: "direct_cond", User: "user:valid", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "directs-user:ttu_7_unconnected", Relation: "direct_cond", User: "user:7unconnected_from_parent", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "path_to_user_and_condition_evaluates_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:7", Relation: "userset_pa_userset_comp_cond_ch", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "no_path_to_user_and_condition_evaluates_false",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:7", Relation: "userset_pa_userset_comp_cond_ch", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "no_path_to_user_even_if_condition_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:7", Relation: "userset_pa_userset_comp_cond_ch", User: "user:7unconnected_from_parent"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:        "no_path_to_user_and_condition_false",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:7", Relation: "userset_pa_userset_comp_cond_ch", User: "user:7unconnected_from_parent"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
		},
	},
	{
		Name: "userset_pa_userset_comp_wild_ch", // condition doesn't impact the outcome
		Tuples: []*openfgav1.TupleKey{
			{Object: "ttus:8", Relation: "userset_parent", User: "usersets-user:ttu_8"},
			{Object: "usersets-user:ttu_8", Relation: "userset_to_computed_wild", User: "directs-user:ttu_8#computed_wild"},
			{Object: "directs-user:ttu_8", Relation: "direct_wild", User: "user:*"},
			// parent not linked to anything
			{Object: "ttus:8_unconnected_1", Relation: "userset_parent", User: "usersets-user:ttu_8_unconnected_1"},
			// user not part of userset
			{Object: "ttus:8_unconnected_2", Relation: "userset_parent", User: "usersets-user:ttu_8_unconnected_2"},
			{Object: "usersets-user:ttu_8_unconnected_2", Relation: "userset_to_computed_wild", User: "directs-user:ttu_8_unconnected_2#computed_wild"}},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "path_to_random_user_and_condition_is_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:8", Relation: "userset_pa_userset_comp_wild_ch", User: "user:user1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "path_to_random_user_and_condition_is_false",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:8", Relation: "userset_pa_userset_comp_wild_ch", User: "user:user1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: true,
			},
			{
				Name:        "path_to_another_random_user_and_condition_is_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:8", Relation: "userset_pa_userset_comp_wild_ch", User: "user:user2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "no_path_to_user_even_if_condition_true_because_parent_not_linked",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:8_unconnected_1", Relation: "userset_pa_userset_comp_wild_ch", User: "user:user1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:        "no_path_to_user_even_if_condition_true_because_user_not_part_of_userset",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:8_unconnected_2", Relation: "userset_pa_userset_comp_wild_ch", User: "user:user1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
		},
	},
	{
		Name: "userset_pa_userset_comp_wild_cond_ch",
		Tuples: []*openfgav1.TupleKey{
			{Object: "ttus:9", Relation: "userset_parent", User: "usersets-user:ttu_9"},
			{Object: "usersets-user:ttu_9", Relation: "userset_to_computed_wild_cond", User: "directs-user:ttu_9#direct_wild_cond"},
			{Object: "directs-user:ttu_9", Relation: "direct_wild_cond", User: "user:*", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			// parent not linked to anything
			{Object: "ttus:9_unconnected_1", Relation: "userset_parent", User: "usersets-user:9_unconnected_1"},
			// user not part of userset
			{Object: "ttus:9_unconnected_2", Relation: "userset_parent", User: "usersets-user:9_unconnected_2"},
			{Object: "usersets-user:9_unconnected_2", Relation: "userset_to_computed_wild_cond", User: "directs-user:9_unconnected_2#direct_wild_cond"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "path_to_random_user_and_condition_is_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:9", Relation: "userset_pa_userset_comp_wild_cond_ch", User: "user:user1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "path_to_another_random_user_and_condition_is_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:9", Relation: "userset_pa_userset_comp_wild_cond_ch", User: "user:user2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "no_path_to_random_user_because_condition_evaluates_false",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:9", Relation: "userset_pa_userset_comp_wild_cond_ch", User: "user:user1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "no_path_to_another_random_user_because_condition_evaluates_false",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:9", Relation: "userset_pa_userset_comp_wild_cond_ch", User: "user:user2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "no_path_to_user_even_if_condition_true_because_parent_not_linked",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:9_unconnected_1", Relation: "userset_pa_userset_comp_wild_cond_ch", User: "user:user1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:        "no_path_to_user_even_if_condition_true_because_user_not_part_of_userset",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:9_unconnected_2", Relation: "userset_pa_userset_comp_wild_cond_ch", User: "user:user1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
		},
	},
	{
		Name: "userset_cond_userset_ch",
		Tuples: []*openfgav1.TupleKey{
			{Object: "ttus:10", Relation: "userset_cond_parent", User: "usersets-user:ttu_10", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			// path to user
			{Object: "usersets-user:ttu_10", Relation: "userset", User: "directs-user:ttu_10#direct"},
			{Object: "directs-user:ttu_10", Relation: "direct", User: "user:valid"},
			// path to employee
			{Object: "usersets-user:ttu_10", Relation: "userset", User: "directs-employee:ttu_10#direct"},
			{Object: "directs-employee:ttu_10", Relation: "direct", User: "employee:valid"},
			// user not member of userset
			{Object: "ttus:10_disconnected_1", Relation: "userset_cond_parent", User: "usersets-user:10_disconnected_1", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "usersets-user:10_disconnected_1", Relation: "userset", User: "directs-user:10_disconnected#direct"},
			// parent not linked to anything
			{Object: "ttus:10_disconnected_2", Relation: "userset_cond_parent", User: "usersets-user:10_disconnected_2", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "path_to_user_and_cond_is_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:10", Relation: "userset_cond_userset_ch", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "no_path_to_user_because_cond_is_false",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:10", Relation: "userset_cond_userset_ch", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "path_to_employee_and_cond_is_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:10", Relation: "userset_cond_userset_ch", User: "employee:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "no_path_to_employee_because_cond_is_false",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:10", Relation: "userset_cond_userset_ch", User: "employee:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "no_path_to_user_because_user_not_in_userset",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:10_disconnected_1", Relation: "userset_cond_userset_ch", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:        "no_path_to_user_because_parent_not_linked_to_object",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:10_disconnected_2", Relation: "userset_cond_userset_ch", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
		},
	},
	{
		Name: "userset_cond_userset_comp_ch",
		Tuples: []*openfgav1.TupleKey{
			{Object: "ttus:11", Relation: "userset_cond_parent", User: "usersets-user:ttu_11", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			// path to user
			{Object: "usersets-user:ttu_11", Relation: "userset_to_computed", User: "directs-user:ttu_11#computed"},
			{Object: "directs-user:ttu_11", Relation: "direct", User: "user:valid"},
			// path to employee
			{Object: "usersets-user:ttu_11", Relation: "userset_to_computed", User: "directs-employee:ttu_11#computed"},
			{Object: "directs-employee:ttu_11", Relation: "direct", User: "employee:valid"},
			// user not member of userset
			{Object: "ttus:11_disconnected_1", Relation: "userset_cond_parent", User: "usersets-user:ttu_11_disconnected_1", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "usersets-user:ttu_11_disconnected_1", Relation: "userset_to_computed", User: "directs-user:11_disconnected#computed"},
			// parent not linked to anything
			{Object: "ttus:11_disconnected_2", Relation: "userset_cond_parent", User: "usersets-user:ttu_11_disconnected_2", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "path_to_user_and_cond_is_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:11", Relation: "userset_cond_userset_comp_ch", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "no_path_to_user_because_cond_is_false",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:11", Relation: "userset_cond_userset_comp_ch", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "path_to_employee_and_cond_is_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:11", Relation: "userset_cond_userset_comp_ch", User: "employee:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "no_path_to_employee_because_cond_is_false",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:11", Relation: "userset_cond_userset_comp_ch", User: "employee:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "no_path_to_user_because_user_not_in_userset",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:11_disconnected_1", Relation: "userset_cond_userset_comp_ch", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:        "no_path_to_user_because_parent_not_linked_to_object",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:11_disconnected_2", Relation: "userset_cond_userset_comp_ch", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
		},
	},
	{
		Name: "userset_cond_userset_comp_cond_ch",
		Tuples: []*openfgav1.TupleKey{
			{Object: "ttus:12", Relation: "userset_cond_parent", User: "usersets-user:ttu_12", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			// path to user
			{Object: "usersets-user:ttu_12", Relation: "userset_to_computed_cond", User: "directs-user:ttu_12#computed_cond"},
			{Object: "directs-user:ttu_12", Relation: "direct_cond", User: "user:valid", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			// path to employee
			{Object: "usersets-user:ttu_12", Relation: "userset_to_computed_cond", User: "directs-employee:ttu_12#direct_cond"},
			{Object: "directs-employee:ttu_12", Relation: "direct_cond", User: "employee:valid", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			// disconnected
			{Object: "ttus:12_disconnected", Relation: "userset_cond_parent", User: "usersets-user:ttu_12_disconnected", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "usersets-user:ttu_12_udisconnected", Relation: "userset_to_computed_cond", User: "directs-user:ttu_12_disconnected#computed_cond"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "path_to_user_and_cond_is_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:12", Relation: "userset_cond_userset_comp_cond_ch", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "no_path_to_user_because_cond_is_false",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:12", Relation: "userset_cond_userset_comp_cond_ch", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "path_to_employee_and_cond_is_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:12", Relation: "userset_cond_userset_comp_cond_ch", User: "employee:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "no_path_to_employee_because_cond_is_false",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:12", Relation: "userset_cond_userset_comp_cond_ch", User: "employee:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "disconnected",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:12_disconnected", Relation: "userset_cond_userset_comp_cond_ch", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
		},
	},
	{
		Name: "userset_cond_userset_comp_wild_ch",
		Tuples: []*openfgav1.TupleKey{
			{Object: "ttus:13", Relation: "userset_cond_parent", User: "usersets-user:ttu_13", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			// path to user
			{Object: "usersets-user:ttu_13", Relation: "userset_to_computed_wild", User: "directs-user:ttu_13#computed_wild"},
			{Object: "directs-user:ttu_13", Relation: "direct_wild", User: "user:*"},
			// path to employee
			{Object: "usersets-user:ttu_13", Relation: "userset_to_computed_wild", User: "directs-employee:ttu_13#direct_wild"},
			{Object: "directs-employee:ttu_13", Relation: "direct_wild", User: "employee:*"},
			// unconnected
			{Object: "ttus:13_unconnected", Relation: "userset_cond_parent", User: "usersets-user:ttu_13_unconnected", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "usersets-user:ttu_13_unconnected", Relation: "userset_to_computed_wild", User: "directs-user:ttu_13_unconnected#computed_wild"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "path_to_random_user_and_cond_is_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:13", Relation: "userset_cond_userset_comp_wild_ch", User: "user:1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "path_to_another_random_user_and_cond_is_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:13", Relation: "userset_cond_userset_comp_wild_ch", User: "user:2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "no_path_to_random_user_because_cond_is_false",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:13", Relation: "userset_cond_userset_comp_wild_ch", User: "user:1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "no_path_to_another_random_user_because_cond_is_false",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:13", Relation: "userset_cond_userset_comp_wild_ch", User: "user:2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "path_to_random_employee_and_cond_is_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:13", Relation: "userset_cond_userset_comp_wild_ch", User: "employee:1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "path_to_another_random_employee_and_cond_is_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:13", Relation: "userset_cond_userset_comp_wild_ch", User: "employee:2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "unconnected_from_wildcard_even_if_cond_is_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:13_unconnected", Relation: "userset_cond_userset_comp_wild_ch", User: "user:1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
		},
	},
	{
		Name: "userset_cond_userset_comp_wild_cond_ch",
		Tuples: []*openfgav1.TupleKey{
			{Object: "ttus:14", Relation: "userset_cond_parent", User: "usersets-user:ttu_14", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "usersets-user:ttu_14", Relation: "userset_to_computed_wild_cond", User: "directs-user:ttu_14#direct_wild_cond"},
			{Object: "directs-user:ttu_14", Relation: "direct_wild_cond", User: "user:*", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			// unconnected
			{Object: "ttus:14_unconnected", Relation: "userset_cond_parent", User: "usersets-user:ttu_14_unconnected", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:  "missing_context_for_condition",
				Tuple: &openfgav1.TupleKey{Object: "ttus:14", Relation: "userset_cond_userset_comp_wild_cond_ch", User: "user:1"},
				Error: condition.ErrEvaluationFailed,
			},
			{
				Name:        "path_and_cond_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:14", Relation: "userset_cond_userset_comp_wild_cond_ch", User: "user:1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "path_for_diff_user_and_cond_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:14", Relation: "userset_cond_userset_comp_wild_cond_ch", User: "user:2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "path_but_cond_false",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:14", Relation: "userset_cond_userset_comp_wild_cond_ch", User: "user:1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "unconnected",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:14_unconnected", Relation: "userset_cond_userset_comp_wild_cond_ch", User: "user:1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
		},
	},
	{
		Name: "or_ttu",
		Tuples: []*openfgav1.TupleKey{
			// path 1
			{Object: "ttus:15_path_1", Relation: "mult_parent_types", User: "directs-user:ttu_15_path_1"},
			{Object: "directs-user:ttu_15_path_1", Relation: "direct", User: "user:valid_path_1"},
			// path 2
			{Object: "ttus:15_path_2", Relation: "mult_parent_types_cond", User: "directs-user:ttu_15_path_2", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "directs-user:ttu_15_path_2", Relation: "direct", User: "user:valid_path_2"},
			// both paths are true
			{Object: "ttus:15_path_both", Relation: "mult_parent_types", User: "directs-user:ttu_15_path_both_1"},
			{Object: "ttus:15_path_both", Relation: "mult_parent_types_cond", User: "directs-user:15_path_both_1", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "directs-user:ttu_15_path_both_1", Relation: "direct", User: "user:valid_both"},
			{Object: "directs-user:15_path_both_1", Relation: "direct", User: "user:valid_both"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_path_1",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:15_path_1", Relation: "or_ttu", User: "user:valid_path_1"},
				Expectation: true,
			},
			{
				Name:        "valid_path_2",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:15_path_2", Relation: "or_ttu", User: "user:valid_path_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_path_2_but_cond_is_false",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:15_path_2", Relation: "or_ttu", User: "user:valid_path_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "valid_both",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:15_path_both", Relation: "or_ttu", User: "user:valid_both"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
		},
	},
	{
		Name: "and_ttu",
		Tuples: []*openfgav1.TupleKey{
			// path 1
			{Object: "ttus:16_path_1", Relation: "direct_parent", User: "directs-user:ttu_16_parent_path_1"},
			{Object: "directs-user:ttu_16_parent_path_1", Relation: "direct_wild", User: "user:*"},
			{Object: "ttus:16_path_1", Relation: "mult_parent_types", User: "directs-user:ttu_16_parent_path_12"},
			{Object: "directs-user:ttu_16_parent_path_12", Relation: "direct", User: "user:valid1"},
			// path 2
			{Object: "ttus:16_path_2", Relation: "direct_parent", User: "directs-user:ttu_16_parent_path_2"},
			{Object: "directs-user:ttu_16_parent_path_2", Relation: "direct", User: "user:valid2"},
			{Object: "ttus:16_path_2", Relation: "mult_parent_types", User: "directs-user:ttu_16_parent_path_22"},
			{Object: "directs-user:ttu_16_parent_path_22", Relation: "direct", User: "user:valid2"},
			// path 3
			{Object: "ttus:16_path_3", Relation: "direct_parent", User: "directs-user:ttu_16_parent_path_3"},
			{Object: "directs-user:ttu_16_parent_path_3", Relation: "direct_cond", User: "user:valid3", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "ttus:16_path_3", Relation: "mult_parent_types", User: "directs-user:ttu_16_parent_path_32"},
			{Object: "directs-user:ttu_16_parent_path_32", Relation: "direct", User: "user:valid3"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_path_1",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:16_path_1", Relation: "and_ttu", User: "user:valid1"},
				Expectation: true,
			},
			{
				Name:        "no_path_1",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:16_path_1", Relation: "and_ttu", User: "user:valid2"},
				Expectation: false,
			},
			{
				Name:        "valid_path_2",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:16_path_2", Relation: "and_ttu", User: "user:valid2"},
				Expectation: true,
			},
			{
				Name:        "no_path_2",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:16_path_2", Relation: "and_ttu", User: "user:valid1"},
				Expectation: false,
			},
			{
				Name:        "valid_path_3",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:16_path_3", Relation: "and_ttu", User: "user:valid3"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "no_path_3_because_condition_missing",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:16_path_3", Relation: "and_ttu", User: "user:valid3"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
		},
	},
	{
		Name: "nested_butnot_ttu",
		Tuples: []*openfgav1.TupleKey{
			// case 1: base is true for user:1
			{Object: "ttus:17", Relation: "direct_parent", User: "directs-user:ttu_17"},
			{Object: "directs-user:ttu_17", Relation: "direct", User: "user:1"},
			// case 1: diff is true for everyone
			{Object: "ttus:17", Relation: "userset_parent", User: "usersets-user:ttu_17"},
			{Object: "usersets-user:ttu_17", Relation: "userset_to_computed_wild", User: "directs-user:ttu_17#computed_wild"},
			{Object: "directs-user:ttu_17", Relation: "direct_wild", User: "user:*"},
			// case 2: base is true for user:1
			{Object: "ttus:17_1", Relation: "direct_parent", User: "directs-user:ttu_17_1"},
			{Object: "directs-user:ttu_17_1", Relation: "direct", User: "user:1"},
			// case 2: diff is false for everyone
			{Object: "ttus:17_1", Relation: "userset_parent", User: "usersets-user:ttu_17_1"},
			{Object: "usersets-user:ttu_17_1", Relation: "userset_to_computed_wild", User: "directs-employee:ttu_17_1#direct_wild"},
			{Object: "directs-employee:ttu_17_1", Relation: "direct_wild", User: "employee:*"},
		},
		CheckAssertions: []*checktest.Assertion{
			// case 1
			{
				Name:        "base_true_diff_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:17", Relation: "nested_butnot_ttu", User: "user:1"},
				Expectation: false,
			},
			{
				Name:        "base_false_diff_true",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:17", Relation: "nested_butnot_ttu", User: "user:2"},
				Expectation: false,
			},
			// case 2
			{
				Name:        "base_true_diff_false",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:17_1", Relation: "nested_butnot_ttu", User: "user:1"},
				Expectation: true,
			},
			{
				Name:        "base_false_diff_false",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:17_1", Relation: "nested_butnot_ttu", User: "user:2"},
				Expectation: false,
			},
		},
	},
	{
		Name: "nested_ttu",
		Tuples: []*openfgav1.TupleKey{
			{Object: "ttus:ttus_nested_ttu_direct_assign", Relation: "nested_ttu", User: "directs-user:ttus_nested_ttu_direct_assign"},
			{Object: "ttus:ttus_nested_ttu_parent_case_1_1", Relation: "nested_ttu_parent", User: "ttus:ttus_nested_ttu_direct_assign"},
			{Object: "ttus:ttus_nested_ttu_parent_case_1_2", Relation: "nested_ttu_parent", User: "ttus:ttus_nested_ttu_parent_case_1_1"},
			{Object: "ttus:ttus_nested_ttu_parent_case_1_3", Relation: "nested_ttu_parent", User: "ttus:ttus_nested_ttu_parent_case_1_2"},
			{Object: "ttus:ttus_nested_ttu_parent_case_1_4", Relation: "nested_ttu_parent", User: "ttus:ttus_nested_ttu_parent_case_1_3"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "nested_ttu_direct_assigned",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_direct_assign", Relation: "nested_ttu", User: "directs-user:ttus_nested_ttu_direct_assign"},
				Expectation: true,
			},
			{
				Name:        "nested_ttu_not_direct_assigned",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_direct_assign", Relation: "nested_ttu", User: "directs-user:ttus_nested_ttu_not_direct_assign"},
				Expectation: false,
			},
			{
				Name:        "nested_ttu_level_1",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_parent_case_1_1", Relation: "nested_ttu", User: "directs-user:ttus_nested_ttu_direct_assign"},
				Expectation: true,
			},
			{
				Name:        "nested_ttu_not_direct_assigned_level_1",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_parent_case_1_1", Relation: "nested_ttu", User: "directs-user:ttus_nested_ttu_not_direct_assign"},
				Expectation: false,
			},
			{
				Name:        "nested_ttu_level_2",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_parent_case_1_2", Relation: "nested_ttu", User: "directs-user:ttus_nested_ttu_direct_assign"},
				Expectation: true,
			},
			{
				Name:        "nested_ttu_not_direct_assigned_level_2",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_parent_case_1_2", Relation: "nested_ttu", User: "directs-user:ttus_nested_ttu_not_direct_assign"},
				Expectation: false,
			},
			{
				Name:        "nested_ttu_level_3",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_parent_case_1_3", Relation: "nested_ttu", User: "directs-user:ttus_nested_ttu_direct_assign"},
				Expectation: true,
			},
			{
				Name:        "nested_ttu_not_direct_assigned_level_3",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_parent_case_1_3", Relation: "nested_ttu", User: "directs-user:ttus_nested_ttu_not_direct_assign"},
				Expectation: false,
			},
			{
				Name:        "nested_ttu_level_4",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_parent_case_1_4", Relation: "nested_ttu", User: "directs-user:ttus_nested_ttu_direct_assign"},
				Expectation: true,
			},
			{
				Name:        "nested_ttu_not_direct_assigned_level_4",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_parent_case_1_4", Relation: "nested_ttu", User: "directs-user:ttus_nested_ttu_not_direct_assign"},
				Expectation: false,
			},
		},
	},
	{
		Name: "nested_ttu_public",
		Tuples: []*openfgav1.TupleKey{
			{Object: "ttus:ttus_nested_ttu_public_direct_assign", Relation: "nested_ttu_public", User: "directs-user:ttus_nested_ttu_public_direct_assign"},
			{Object: "ttus:ttus_nested_ttu_public_parent_case_1_1", Relation: "nested_ttu_parent", User: "ttus:ttus_nested_ttu_public_direct_assign"},
			{Object: "ttus:ttus_nested_ttu_public_parent_case_1_2", Relation: "nested_ttu_parent", User: "ttus:ttus_nested_ttu_public_parent_case_1_1"},
			{Object: "ttus:ttus_nested_ttu_public_parent_case_1_3", Relation: "nested_ttu_parent", User: "ttus:ttus_nested_ttu_public_parent_case_1_2"},
			{Object: "ttus:ttus_nested_ttu_public_parent_case_1_4", Relation: "nested_ttu_parent", User: "ttus:ttus_nested_ttu_public_parent_case_1_3"},
			{Object: "ttus:ttus_nested_ttu_public_wildcard", Relation: "nested_ttu_public", User: "directs-user:*"},
			{Object: "ttus:ttus_nested_ttu_public_wildcard_parent_case_1_1", Relation: "nested_ttu_parent", User: "ttus:ttus_nested_ttu_public_wildcard"},
			{Object: "ttus:ttus_nested_ttu_public_wildcard_parent_case_1_2", Relation: "nested_ttu_parent", User: "ttus:ttus_nested_ttu_public_wildcard_parent_case_1_1"},
			{Object: "ttus:ttus_nested_ttu_public_wildcard_parent_case_1_3", Relation: "nested_ttu_parent", User: "ttus:ttus_nested_ttu_public_wildcard_parent_case_1_2"},
			{Object: "ttus:ttus_nested_ttu_public_wildcard_parent_case_1_4", Relation: "nested_ttu_parent", User: "ttus:ttus_nested_ttu_public_wildcard_parent_case_1_3"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "nested_ttu_direct_assigned",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_public_direct_assign", Relation: "nested_ttu_public", User: "directs-user:ttus_nested_ttu_public_direct_assign"},
				Expectation: true,
			},
			{
				Name:        "nested_ttu_not_direct_assigned",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_public_direct_assign", Relation: "nested_ttu_public", User: "directs-user:ttus_nested_ttu_public_not_direct_assign"},
				Expectation: false,
			},
			{
				Name:        "nested_ttu_level_1",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_public_parent_case_1_1", Relation: "nested_ttu_public", User: "directs-user:ttus_nested_ttu_public_direct_assign"},
				Expectation: true,
			},
			{
				Name:        "nested_ttu_not_direct_assigned_level_1",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_public_parent_case_1_1", Relation: "nested_ttu_public", User: "directs-user:ttus_nested_ttu_public_not_direct_assign"},
				Expectation: false,
			},
			{
				Name:        "nested_ttu_level_2",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_public_parent_case_1_2", Relation: "nested_ttu_public", User: "directs-user:ttus_nested_ttu_public_direct_assign"},
				Expectation: true,
			},
			{
				Name:        "nested_ttu_not_direct_assigned_level_2",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_public_parent_case_1_2", Relation: "nested_ttu_public", User: "directs-user:ttus_nested_ttu_public_not_direct_assign"},
				Expectation: false,
			},
			{
				Name:        "nested_ttu_level_3",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_public_parent_case_1_3", Relation: "nested_ttu_public", User: "directs-user:ttus_nested_ttu_public_direct_assign"},
				Expectation: true,
			},
			{
				Name:        "nested_ttu_not_direct_assigned_level_3",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_public_parent_case_1_3", Relation: "nested_ttu_public", User: "directs-user:ttus_nested_ttu_public_not_direct_assign"},
				Expectation: false,
			},
			{
				Name:        "nested_ttu_level_4",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_public_parent_case_1_4", Relation: "nested_ttu_public", User: "directs-user:ttus_nested_ttu_public_direct_assign"},
				Expectation: true,
			},
			{
				Name:        "nested_ttu_not_direct_assigned_level_4",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_public_parent_case_1_4", Relation: "nested_ttu_public", User: "directs-user:ttus_nested_ttu_public_not_direct_assign"},
				Expectation: false,
			},
			{
				Name:        "nested_ttu_public_wildcard",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_public_wildcard", Relation: "nested_ttu_public", User: "directs-user:any"},
				Expectation: true,
			},
			{
				Name:        "nested_ttu_public_wildcard_level_1",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_public_wildcard_parent_case_1_1", Relation: "nested_ttu_public", User: "directs-user:any"},
				Expectation: true,
			},
			{
				Name:        "nested_ttu_public_wildcard_level_2",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_public_wildcard_parent_case_1_2", Relation: "nested_ttu_public", User: "directs-user:any"},
				Expectation: true,
			},
			{
				Name:        "nested_ttu_public_wildcard_level_3",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_public_wildcard_parent_case_1_3", Relation: "nested_ttu_public", User: "directs-user:any"},
				Expectation: true,
			},
			{
				Name:        "nested_ttu_public_wildcard_level_4",
				Tuple:       &openfgav1.TupleKey{Object: "ttus:ttus_nested_ttu_public_wildcard_parent_case_1_4", Relation: "nested_ttu_public", User: "directs-user:any"},
				Expectation: true,
			},
		},
	},
}
