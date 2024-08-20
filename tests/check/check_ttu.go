package check

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/protobuf/types/known/structpb"

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
				Tuple:       &openfgav1.TupleKey{Object: "ttus:1", Relation: "direct_pa_direct_ch", User: "user:invalid_group"},
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
		},
	},
}
