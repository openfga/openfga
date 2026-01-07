package check

import (
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	checktest "github.com/openfga/openfga/internal/test/check"
)

var complexityFourTestingModelTest = []*stage{
	{
		Name: "complexity4_userset_ttu_userset_ttu",
		Tuples: []*openfgav1.TupleKey{
			{Object: "complexity4:xxx", Relation: "userset_ttu_userset_ttu", User: "complexity3:yyy#ttu_userset_ttu"},
			{Object: "complexity3:yyy", Relation: "userset_parent", User: "usersets-user:zzz"},
			{Object: "usersets-user:zzz", Relation: "ttu_direct_userset", User: "ttus:aaa#direct_pa_direct_ch"},
			{Object: "ttus:aaa", Relation: "mult_parent_types", User: "directs-employee:ccc"},
			{Object: "ttus:aaa", Relation: "mult_parent_types", User: "directs-user:bbb"},
			{Object: "directs-user:bbb", Relation: "direct", User: "user:valid"},
			{Object: "directs-employee:ccc", Relation: "direct", User: "employee:valid"},
			// missing tuples, 1 level deep
			{Object: "complexity4:xxx_1", Relation: "userset_ttu_userset_ttu", User: "complexity3:xxx_1#ttu_userset_ttu"},
			// missing tuples, 2 levels deep
			{Object: "complexity4:xxx_2", Relation: "userset_ttu_userset_ttu", User: "complexity3:xxx_2#ttu_userset_ttu"},
			{Object: "complexity3:xxx_2", Relation: "userset_parent", User: "usersets-user:xxx_2"},
			// missing tuples, 3 levels deep
			{Object: "complexity4:xxx_3", Relation: "userset_ttu_userset_ttu", User: "complexity3:xxx_3#ttu_userset_ttu"},
			{Object: "complexity3:xxx_3", Relation: "userset_parent", User: "usersets-user:xxx_3"},
			{Object: "usersets-user:xxx_3", Relation: "ttu_direct_userset", User: "ttus:xxx_3#direct_pa_direct_ch"},
			// missing tuples, 4 levels deep
			{Object: "complexity4:xxx_4", Relation: "userset_ttu_userset_ttu", User: "complexity3:xxx_4#ttu_userset_ttu"},
			{Object: "complexity3:xxx_4", Relation: "userset_parent", User: "usersets-user:xxx_4"},
			{Object: "usersets-user:xxx_4", Relation: "ttu_direct_userset", User: "ttus:xxx_4#direct_pa_direct_ch"},
			{Object: "ttus:xxx_4", Relation: "mult_parent_types", User: "directs-user:xxx_4"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "path_to_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:xxx", Relation: "userset_ttu_userset_ttu", User: "user:valid"},
				Expectation: true,
			},
			{
				Name:        "path_to_user_and_condition_evaluates_to_true_but_ignored",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:xxx", Relation: "userset_ttu_userset_ttu", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "path_to_user_and_condition_evaluates_to_false_but_ignored",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:xxx", Relation: "userset_ttu_userset_ttu", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: true,
			},
			{
				Name:        "no_path_to_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:xxx", Relation: "userset_ttu_userset_ttu", User: "user:invalid"},
				Expectation: false,
			},
			{
				Name:        "path_to_employee",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:xxx", Relation: "userset_ttu_userset_ttu", User: "employee:valid"},
				Expectation: true,
			},
			{
				Name:        "no_path_to_employee",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:xxx", Relation: "userset_ttu_userset_ttu", User: "employee:invalid"},
				Expectation: false,
			},
			{
				Name:        "no_path_to_user_missing_tuple_level_1",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:xxx_1", Relation: "userset_ttu_userset_ttu", User: "user:valid"},
				Expectation: false,
			},
			{
				Name:        "no_path_to_user_missing_tuple_level_2",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:xxx_2", Relation: "userset_ttu_userset_ttu", User: "user:valid"},
				Expectation: false,
			},
			{
				Name:        "no_path_to_user_missing_tuple_level_3",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:xxx_3", Relation: "userset_ttu_userset_ttu", User: "user:valid"},
				Expectation: false,
			},
			{
				Name:        "no_path_to_user_missing_tuple_level_4",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:xxx_4", Relation: "userset_ttu_userset_ttu", User: "user:valid"},
				Expectation: false,
			},
		},
	},
	{
		Name: "complexity4_ttu_ttu_ttu_userset",
		Tuples: []*openfgav1.TupleKey{
			{Object: "complexity4:car", Relation: "parent", User: "complexity3:carparent"},
			{Object: "complexity3:carparent", Relation: "ttu_parent", User: "ttus:truck"},
			{Object: "ttus:truck", Relation: "userset_parent", User: "usersets-user:van"},
			{Object: "usersets-user:van", Relation: "userset", User: "directs-user:vanusers#direct"},
			{Object: "directs-user:vanusers", Relation: "direct", User: "user:valid"},
			{Object: "usersets-user:van", Relation: "userset", User: "directs-employee:vanemployees#direct"},
			{Object: "directs-employee:vanemployees", Relation: "direct", User: "employee:valid"},
			// missing tuples, 1 level deep
			{Object: "complexity4:car_1", Relation: "parent", User: "complexity3:carparent1"},
			// missing tuples, 2 levels deep
			{Object: "complexity4:car_2", Relation: "parent", User: "complexity3:carparent2"},
			{Object: "complexity3:carparent2", Relation: "ttu_parent", User: "ttus:truck2"},
			// missing tuples, 3 levels deep
			{Object: "complexity4:car_3", Relation: "parent", User: "complexity3:carparent3"},
			{Object: "complexity3:carparent3", Relation: "ttu_parent", User: "ttus:truck3"},
			{Object: "ttus:truck3", Relation: "userset_parent", User: "usersets-user:van3"},
			// missing tuples, 4 levels deep
			{Object: "complexity4:car_4", Relation: "parent", User: "complexity3:carparent4"},
			{Object: "complexity3:carparent4", Relation: "ttu_parent", User: "ttus:truck4"},
			{Object: "ttus:truck4", Relation: "userset_parent", User: "usersets-user:van4"},
			{Object: "usersets-user:van4", Relation: "userset", User: "directs-user:vanusers4#direct"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "path_to_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:car", Relation: "ttu_ttu_ttu_userset", User: "user:valid"},
				Expectation: true,
			},
			{
				Name:        "no_path_to_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:car", Relation: "ttu_ttu_ttu_userset", User: "user:invalid"},
				Expectation: false,
			},
			{
				Name:        "path_to_user_and_condition_evaluates_to_true_but_ignored",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:car", Relation: "ttu_ttu_ttu_userset", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "path_to_user_and_condition_evaluates_to_false_but_ignored",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:car", Relation: "ttu_ttu_ttu_userset", User: "user:valid"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: true,
			},
			{
				Name:        "path_to_employee",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:car", Relation: "ttu_ttu_ttu_userset", User: "employee:valid"},
				Expectation: true,
			},
			{
				Name:        "no_path_to_employee",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:car", Relation: "ttu_ttu_ttu_userset", User: "employee:invalid"},
				Expectation: false,
			},
			{
				Name:        "no_path_to_user_missing_tuple_level_1",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:car_1", Relation: "ttu_ttu_ttu_userset", User: "user:valid"},
				Expectation: false,
			},
			{
				Name:        "no_path_to_user_missing_tuple_level_2",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:car_2", Relation: "ttu_ttu_ttu_userset", User: "user:valid"},
				Expectation: false,
			},
			{
				Name:        "no_path_to_user_missing_tuple_level_3",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:car_3", Relation: "ttu_ttu_ttu_userset", User: "user:valid"},
				Expectation: false,
			},
			{
				Name:        "no_path_to_user_missing_tuple_level_4",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:car_4", Relation: "ttu_ttu_ttu_userset", User: "user:valid"},
				Expectation: false,
			},
		},
	},
	{
		Name: "complexity4_userset_or_compute_complex3",
		Tuples: []*openfgav1.TupleKey{
			{Object: "complexity4:ps1", Relation: "userset_or_compute_complex3", User: "complexity3:ps2#or_compute_complex3"},
			// path 1 in the or
			{Object: "complexity3:ps2", Relation: "userset_parent", User: "usersets-user:ps2"},
			{Object: "usersets-user:ps2", Relation: "ttu_direct_userset", User: "ttus:ps3#direct_pa_direct_ch"},
			{Object: "ttus:ps3", Relation: "mult_parent_types", User: "directs-user:ps4"},
			{Object: "directs-user:ps4", Relation: "direct", User: "user:valid"},
			{Object: "ttus:ps3", Relation: "mult_parent_types", User: "directs-employee:ps4"},
			{Object: "directs-employee:ps4", Relation: "direct", User: "employee:valid"},
			// path 2 in the or
			{Object: "complexity3:ps2", Relation: "userset_ttu_userset", User: "ttus:ps3#userset_pa_userset_ch"},
			{Object: "ttus:ps3", Relation: "userset_parent", User: "usersets-user:ps2"},
			{Object: "usersets-user:ps2", Relation: "userset", User: "directs-user:ps4#direct"},
			{Object: "directs-user:ps4", Relation: "direct", User: "user:valid2"},
			{Object: "usersets-user:ps2", Relation: "userset", User: "directs-employee:ps4#direct"},
			{Object: "directs-employee:ps4", Relation: "direct", User: "employee:valid2"},
		},
		CheckAssertions: []*checktest.Assertion{
			// path 1
			{
				Name:        "path_to_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:ps1", Relation: "userset_or_compute_complex3", User: "user:valid"},
				Expectation: true,
			},
			{
				Name:        "no_path_to_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:ps1", Relation: "userset_or_compute_complex3", User: "user:invalid"},
				Expectation: false,
			},
			{
				Name:        "path_to_employee",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:ps1", Relation: "userset_or_compute_complex3", User: "employee:valid"},
				Expectation: true,
			},
			{
				Name:        "no_path_to_employee",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:ps1", Relation: "userset_or_compute_complex3", User: "employee:invalid"},
				Expectation: false,
			},
			// path 2
			{
				Name:        "path_to_user2",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:ps1", Relation: "userset_or_compute_complex3", User: "user:valid2"},
				Expectation: true,
			},
			{
				Name:        "no_path_to_user2",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:ps1", Relation: "userset_or_compute_complex3", User: "user:invalid2"},
				Expectation: false,
			},
			{
				Name:        "path_to_employee",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:ps1", Relation: "userset_or_compute_complex3", User: "employee:valid2"},
				Expectation: true,
			},
			{
				Name:        "no_path_to_employee",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:ps1", Relation: "userset_or_compute_complex3", User: "employee:invalid2"},
				Expectation: false,
			},
		},
	},
	{
		Name: "complexity4_ttu_and_nested_complex3",
		// not possible to reach an employee
		// 2 possible paths to reach a user
		Tuples: []*openfgav1.TupleKey{
			{Object: "complexity4:pe1", Relation: "parent", User: "complexity3:pe2"},
			{Object: "complexity3:pe2", Relation: "and_nested_complex3", User: "ttus:pe3#and_ttu"},
			{Object: "directs-user:pe4", Relation: "direct", User: "user:valid"},
			{Object: "complexity3:pe2", Relation: "userset_parent", User: "usersets-user:pe3"},
			{Object: "usersets-user:pe3", Relation: "ttu_direct_userset", User: "ttus:pe3#direct_pa_direct_ch"},
			{Object: "ttus:pe3", Relation: "mult_parent_types", User: "directs-user:pe4"},
			// path 1 in the union
			{Object: "ttus:pe3", Relation: "direct_parent", User: "directs-user:pe4"},
			// path 2 in the union that cannot lead to truthy because we are missing tuples on other parts of the graph
			{Object: "directs-user:pe4", Relation: "direct_cond", User: "user:validwithcond", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			// path 3 for employee cannot lead to truthy because we are missing tuples on other parts of the graph
			{Object: "ttus:pe3", Relation: "mult_parent_types", User: "directs-employee:pe4"},
			{Object: "directs-employee:pe4", Relation: "direct", User: "employee:invalid"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:               "path_to_user_1",
				Tuple:              &openfgav1.TupleKey{Object: "complexity4:pe1", Relation: "ttu_and_nested_complex3", User: "user:valid"},
				Expectation:        true,
				ListUsersErrorCode: 2000,
			},
			{
				Name:        "no_path_to_user_with_cond_truthy",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:pe1", Relation: "ttu_and_nested_complex3", User: "user:validwithcond"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:        "no_path_to_employee",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:pe1", Relation: "ttu_and_nested_complex3", User: "employee:invalid"},
				Expectation: false,
			},
		},
	},
	{
		Name: "complexity4_or_complex4",
		Tuples: []*openfgav1.TupleKey{
			{Object: "complexity4:pe1", Relation: "parent", User: "complexity3:pe2"},
			{Object: "complexity3:pe2", Relation: "and_nested_complex3", User: "ttus:pe3#and_ttu"},
			{Object: "directs-user:pe4", Relation: "direct", User: "user:valid"},
			{Object: "complexity3:pe2", Relation: "userset_parent", User: "usersets-user:pe3"},
			{Object: "usersets-user:pe3", Relation: "ttu_direct_userset", User: "ttus:pe3#direct_pa_direct_ch"},
			{Object: "ttus:pe3", Relation: "mult_parent_types", User: "directs-user:pe4"},
			{Object: "ttus:pe3", Relation: "direct_parent", User: "directs-user:pe4"},
			{Object: "directs-user:pe4", Relation: "direct_cond", User: "user:validwithcond", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "ttus:pe3", Relation: "mult_parent_types", User: "directs-employee:pe4"},
			{Object: "directs-employee:pe4", Relation: "direct", User: "employee:invalid"},
			{Object: "complexity4:ps1", Relation: "userset_or_compute_complex3", User: "complexity3:ps2#or_compute_complex3"},
			{Object: "complexity3:ps2", Relation: "userset_parent", User: "usersets-user:ps2"},
			{Object: "usersets-user:ps2", Relation: "ttu_direct_userset", User: "ttus:ps3#direct_pa_direct_ch"},
			{Object: "ttus:ps3", Relation: "mult_parent_types", User: "directs-user:ps4"},
			{Object: "directs-user:ps4", Relation: "direct", User: "user:valid"},
			{Object: "ttus:ps3", Relation: "mult_parent_types", User: "directs-employee:ps4"},
			{Object: "directs-employee:ps4", Relation: "direct", User: "employee:valid"},
			{Object: "complexity3:ps2", Relation: "userset_ttu_userset", User: "ttus:ps3#userset_pa_userset_ch"},
			{Object: "ttus:ps3", Relation: "userset_parent", User: "usersets-user:ps2"},
			{Object: "usersets-user:ps2", Relation: "userset", User: "directs-user:ps4#direct"},
			{Object: "directs-user:ps4", Relation: "direct", User: "user:valid2"},
			{Object: "usersets-user:ps2", Relation: "userset", User: "directs-employee:ps4#direct"},
			{Object: "directs-employee:ps4", Relation: "direct", User: "employee:valid2"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:               "path_to_user_through_ttu_and_nested_complex3",
				Tuple:              &openfgav1.TupleKey{Object: "complexity4:pe1", Relation: "or_complex4", User: "user:valid"},
				Expectation:        true,
				ListUsersErrorCode: 2000,
			},
			{
				Name:        "path_to_user_through_userset_or_compute_complex3",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:ps1", Relation: "or_complex4", User: "user:valid"},
				Expectation: true,
			},
			{
				Name:        "path_to_user_through_userset_or_compute_complex3_2",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:ps1", Relation: "or_complex4", User: "user:valid2"},
				Expectation: true,
			},
			{
				Name:               "no_path_1",
				Tuple:              &openfgav1.TupleKey{Object: "complexity4:pe1", Relation: "or_complex4", User: "user:valid2"},
				Expectation:        false,
				ListUsersErrorCode: 2000,
			},
			{
				Name:        "no_path_2",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:ps1", Relation: "or_complex4", User: "user:invalid"},
				Expectation: false,
			},
			{
				Name:        "no_path_3",
				Tuple:       &openfgav1.TupleKey{Object: "complexity4:pe1x", Relation: "or_complex4", User: "user:invalid"},
				Expectation: false,
			},
		},
	},
}
