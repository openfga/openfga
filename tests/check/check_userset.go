package check

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/protobuf/types/known/structpb"

	checktest "github.com/openfga/openfga/internal/test/check"
)

var usersetCompleteTestingModelTest = []*stage{
	{
		Name: "usersets_userset",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:userset_1", Relation: "direct", User: "user:userset_valid"},
			{Object: "directs-employee:userset_1", Relation: "direct", User: "employee:userset_valid"},

			{Object: "usersets-user:userset_1", Relation: "userset", User: "directs-user:userset_1#direct"},
			{Object: "usersets-user:userset_1", Relation: "userset", User: "directs-employee:userset_1#direct"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "user_valid",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:userset_1", Relation: "userset", User: "user:userset_valid"},
				Expectation: true,
			},
			{
				Name:        "employee_valid",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:userset_1", Relation: "userset", User: "employee:userset_valid"},
				Expectation: true,
			},
			{
				Name:        "user_invalid",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:userset_1", Relation: "userset", User: "user:userset_invalid"},
				Expectation: false,
			},
			{
				Name:        "employee_invalid",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:userset_1", Relation: "userset", User: "employee:userset_invalid"},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:userset_2", Relation: "userset", User: "user:userset_invalid"},
				Expectation: false,
			},
		},
	},
	{
		Name: "usersets_userset_to_computed",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:utc_1", Relation: "direct", User: "user:utc_valid"},
			{Object: "directs-employee:utc_1", Relation: "direct", User: "employee:utc_valid"},
			{Object: "usersets-user:utc_1", Relation: "userset_to_computed", User: "directs-user:utc_1#computed"},
			{Object: "usersets-user:utc_1", Relation: "userset_to_computed", User: "directs-employee:utc_1#computed"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utc_1", Relation: "userset_to_computed", User: "user:utc_valid"},
				Expectation: true,
			},
			{
				Name:        "valid_employee",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utc_1", Relation: "userset_to_computed", User: "employee:utc_valid"},
				Expectation: true,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utc_1", Relation: "userset_to_computed", User: "user:utc_invalid"},
				Expectation: false,
			},
			{
				Name:        "invalid_employee",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utc_1", Relation: "userset_to_computed", User: "employee:utc_invalid"},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utc_2", Relation: "userset_to_computed", User: "user:utc_valid"},
				Expectation: false,
			},
		},
	},
	{
		Name: "usersets_userset_to_computed_wild",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:utcw_1", Relation: "direct_wild", User: "user:*"},
			{Object: "directs-employee:utcw_1", Relation: "direct_wild", User: "employee:*"},
			{Object: "usersets-user:utcw_1", Relation: "userset_to_computed_wild", User: "directs-user:utcw_1#computed_wild"},
			{Object: "usersets-user:utcw_1", Relation: "userset_to_computed_wild", User: "directs-employee:utcw_1#direct_wild"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utcw_1", Relation: "userset_to_computed_wild", User: "user:utcw_valid"},
				Expectation: true,
			},
			{
				Name:        "valid_employee",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utcw_1", Relation: "userset_to_computed_wild", User: "employee:utcw_valid"},
				Expectation: true,
			},
			{
				Name:        "invalid_user_type",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utcw_1", Relation: "userset_to_computed_wild", User: "ttus:utcw_invalid"},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utcw_2", Relation: "userset_to_computed_wild", User: "user:utcw_valid"},
				Expectation: false,
			},
		},
	},
	{
		Name: "usersets_userset_to_computed_wild_cond",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:utcwd_1", Relation: "direct_wild_cond", User: "user:*", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "directs-employee:utcwd_2", Relation: "direct_wild_cond", User: "employee:*", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},

			{Object: "usersets-user:utcwd_1", Relation: "userset_to_computed_wild_cond", User: "directs-user:utcwd_1#direct_wild_cond"},
			{Object: "usersets-user:utcwd_2", Relation: "userset_to_computed_wild_cond", User: "directs-employee:utcwd_2#direct_wild_cond"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utcwd_1", Relation: "userset_to_computed_wild_cond", User: "user:utwcd_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_employee",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utcwd_2", Relation: "userset_to_computed_wild_cond", User: "employee:utwcd_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_user_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utcwd_1", Relation: "userset_to_computed_wild_cond", User: "user:utwcd_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "valid_employee_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utcwd_2", Relation: "userset_to_computed_wild_cond", User: "employee:utwcd_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:      "user_no_cond",
				Tuple:     &openfgav1.TupleKey{Object: "usersets-user:utcwd_1", Relation: "userset_to_computed_wild_cond", User: "user:utwcd_1"},
				ErrorCode: 2000,
			},
			{
				Name:      "employee_no_cond",
				Tuple:     &openfgav1.TupleKey{Object: "usersets-user:utcwd_2", Relation: "userset_to_computed_wild_cond", User: "employee:utwcd_2"},
				ErrorCode: 2000,
			},
		},
	},
	{
		Name: "usersets_userset_cond",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:uuc_1", Relation: "direct", User: "user:uuc_1"},
			{Object: "usersets-user:uuc_1", Relation: "userset_cond", User: "directs-user:uuc_1#direct", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uuc_1", Relation: "userset_cond", User: "user:uuc_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_user_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uuc_1", Relation: "userset_cond", User: "user:uuc_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uuc_1", Relation: "userset_cond", User: "user:uuc_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uuc_2", Relation: "userset_cond", User: "user:uuc_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},

			{
				Name:      "no_cond",
				Tuple:     &openfgav1.TupleKey{Object: "usersets-user:uuc_1", Relation: "userset_cond", User: "user:uuc_1"},
				ErrorCode: 2000,
			},
		},
	},
	{
		Name: "usersets_userset_cond_to_computed",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:uuctc_1", Relation: "direct", User: "user:uuctc_1"},
			{Object: "usersets-user:uuctc_1", Relation: "userset_cond_to_computed", User: "directs-user:uuctc_1#computed", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uuctc_1", Relation: "userset_cond_to_computed", User: "user:uuctc_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_user_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uuctc_1", Relation: "userset_cond_to_computed", User: "user:uuctc_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uuctc_1", Relation: "userset_cond_to_computed", User: "user:uuctc_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uuctc_2", Relation: "userset_cond_to_computed", User: "user:uuctc_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},

			{
				Name:      "no_cond",
				Tuple:     &openfgav1.TupleKey{Object: "usersets-user:uuctc_1", Relation: "userset_cond_to_computed", User: "user:uuctc_1"},
				ErrorCode: 2000,
			},
		},
	},
	{
		Name: "usersets_userset_cond_to_computed_cond",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:uuctcc_1", Relation: "direct_cond", User: "user:uuctcc_1", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "usersets-user:uuctcc_1", Relation: "userset_cond_to_computed_cond", User: "directs-user:uuctcc_1#computed_cond", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uuctcc_1", Relation: "userset_cond_to_computed_cond", User: "user:uuctcc_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_user_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uuctcc_1", Relation: "userset_cond_to_computed_cond", User: "user:uuctcc_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uuctcc_1", Relation: "userset_cond_to_computed_cond", User: "user:uuctcc_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uuctcc_2", Relation: "userset_cond_to_computed_cond", User: "user:uuctcc_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},

			{
				Name:      "no_cond",
				Tuple:     &openfgav1.TupleKey{Object: "usersets-user:uuctcc_1", Relation: "userset_cond_to_computed_cond", User: "user:uuctcc_2"},
				ErrorCode: 2000,
			},
		},
	},
	{
		Name: "usersets_userset_cond_to_computed_wild",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:uuctcw_1", Relation: "direct_wild", User: "user:*"},
			{Object: "usersets-user:uuctcw_1", Relation: "userset_cond_to_computed_wild", User: "directs-user:uuctcw_1#computed_wild", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uuctcw_1", Relation: "userset_cond_to_computed_wild", User: "user:uuctcw_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_user_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uuctcw_1", Relation: "userset_cond_to_computed_wild", User: "user:uuctcw_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uuctcw_2", Relation: "userset_cond_to_computed_wild", User: "user:uuctcw_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
		},
	},
	{
		Name: "usersets_userset_cond_to_computed_wild_cond",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:uuctcwc_1", Relation: "direct_wild_cond", User: "user:*", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "usersets-user:uuctcwc_1", Relation: "userset_cond_to_computed_wild_cond", User: "directs-user:uuctcwc_1#computed_wild_cond", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uuctcwc_1", Relation: "userset_cond_to_computed_wild_cond", User: "user:uuctcwc_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_user_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uuctcwc_1", Relation: "userset_cond_to_computed_wild_cond", User: "user:uuctcwc_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uuctcwc_2", Relation: "userset_cond_to_computed_wild_cond", User: "user:uuctcwc_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},

			{
				Name:      "no_cond",
				Tuple:     &openfgav1.TupleKey{Object: "usersets-user:uuctcwc_1", Relation: "userset_cond_to_computed_wild_cond", User: "user:uuctcwc_1"},
				ErrorCode: 2000,
			},
		},
	},
	{
		Name: "usersets_userset_to_or_computed",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:utoc_1", Relation: "direct", User: "user:utoc_1"},                                                                  // covers computed
			{Object: "directs-user:utoc_2", Relation: "direct_wild", User: "user:*"},                                                                  // covers direct_wild
			{Object: "directs-user:utoc_3", Relation: "direct_cond", User: "user:utoc_3", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}}, // covers direct_cond

			{Object: "usersets-user:utoc_1", Relation: "userset_to_or_computed", User: "directs-user:utoc_1#or_computed"},
			{Object: "usersets-user:utoc_2", Relation: "userset_to_or_computed", User: "directs-user:utoc_2#or_computed"},
			{Object: "usersets-user:utoc_3", Relation: "userset_to_or_computed", User: "directs-user:utoc_3#or_computed"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utoc_1", Relation: "userset_to_or_computed", User: "user:utoc_1"},
				Expectation: true,
			},
			{
				Name:        "valid_wildcard",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utoc_2", Relation: "userset_to_or_computed", User: "user:utoc_2"},
				Expectation: true,
			},
			{
				Name:        "valid_user_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utoc_3", Relation: "userset_to_or_computed", User: "user:utoc_3"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_user_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utoc_3", Relation: "userset_to_or_computed", User: "user:utoc_3"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utoc_1", Relation: "userset_to_or_computed", User: "user:utoc_2"},
				Expectation: false,
			},
			{
				Name:        "invalid_user_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utoc_3", Relation: "userset_to_or_computed", User: "user:utoc_4"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:      "no_condition",
				Tuple:     &openfgav1.TupleKey{Object: "usersets-user:utoc_3", Relation: "userset_to_or_computed", User: "user:utoc_3"},
				ErrorCode: 2000,
			},
			{
				Name:               "invalid_object",
				Tuple:              &openfgav1.TupleKey{Object: "usersets-user:utoc_3", Relation: "userset_to_or_computed", User: "user:utoc_1"},
				Expectation:        false,
				ListUsersErrorCode: 2000,
			},
		},
	},
	{
		Name: "usersets_userset_to_butnot_computed",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:utbc_1", Relation: "direct_wild_cond", User: "user:*", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "directs-user:utbc_1", Relation: "direct", User: "user:utbc_2"},

			{Object: "usersets-user:utbc_1", Relation: "userset_to_butnot_computed", User: "directs-user:utbc_1#butnot_computed"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utbc_1", Relation: "userset_to_butnot_computed", User: "user:utbc_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_user_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utbc_1", Relation: "userset_to_butnot_computed", User: "user:utbc_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:      "no_condition",
				Tuple:     &openfgav1.TupleKey{Object: "usersets-user:utbc_1", Relation: "userset_to_butnot_computed", User: "user:utbc_1"},
				ErrorCode: 2000,
			},
			{
				Name:        "but_not_case",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utbc_1", Relation: "userset_to_butnot_computed", User: "user:utbc_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utbc_2", Relation: "userset_to_butnot_computed", User: "user:utbc_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
		},
	},
	{
		Name: "usersets_userset_to_and_computed",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:utac_1", Relation: "direct_cond", User: "user:utac_1", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "directs-user:utac_1", Relation: "direct_wild", User: "user:*"},

			{Object: "usersets-user:utac_1", Relation: "userset_to_and_computed", User: "directs-user:utac_1#and_computed"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utac_1", Relation: "userset_to_and_computed", User: "user:utac_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_user_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utac_1", Relation: "userset_to_and_computed", User: "user:utac_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:      "no_condition",
				Tuple:     &openfgav1.TupleKey{Object: "usersets-user:utac_1", Relation: "userset_to_and_computed", User: "user:utac_1"},
				ErrorCode: 2000,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utac_1", Relation: "userset_to_and_computed", User: "user:utac_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utac_2", Relation: "userset_to_and_computed", User: "user:utac_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
		},
	},
	{
		Name: "usersets_userset_recursive",
		Tuples: []*openfgav1.TupleKey{
			{Object: "usersets-user:userset_recursive_1", Relation: "userset_recursive", User: "user:userset_recursive_user_1"},
			{Object: "usersets-user:userset_recursive_1", Relation: "userset_recursive", User: "usersets-user:userset_recursive_2#userset_recursive"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_recursive",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:userset_recursive_1", Relation: "userset_recursive", User: "usersets-user:userset_recursive_2#userset_recursive"},
				Expectation: true,
			},
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:userset_recursive_1", Relation: "userset_recursive", User: "user:userset_recursive_user_1"},
				Expectation: true,
			},
			{
				Name:        "invalid_recursive",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:userset_1", Relation: "userset_recursive", User: "usersets-user:userset_3#userset_recursive"},
				Expectation: false,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:userset_1", Relation: "userset_recursive", User: "user:userset_user_2"},
				Expectation: false,
			},

			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:userset_2", Relation: "userset_recursive", User: "user:userset_user_2"},
				Expectation: false,
			},
		},
	},
	{
		Name: "usersets_or_userset",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:userset_or_1", Relation: "direct", User: "user:userset_or_userset_valid"},
			{Object: "directs-user:userset_or_2", Relation: "direct_cond", User: "user:uou_2", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "directs-employee:userset_or_3", Relation: "direct", User: "employee:uou_3"},
			{Object: "directs-employee:userset_or_4", Relation: "direct_cond", User: "employee:uou_4", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},

			{Object: "usersets-user:userset_or_1", Relation: "userset", User: "directs-user:userset_or_1#direct"},
			{Object: "usersets-user:userset_or_2", Relation: "userset_to_computed_cond", User: "directs-user:userset_or_2#computed_cond"},
			{Object: "usersets-user:userset_or_3", Relation: "userset", User: "directs-employee:userset_or_3#direct"},
			{Object: "usersets-user:userset_or_4", Relation: "userset_to_computed_cond", User: "directs-employee:userset_or_4#direct_cond"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_userset_directs-user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:userset_or_1", Relation: "or_userset", User: "user:userset_or_userset_valid"},
				Expectation: true,
			},
			{
				Name:        "valid_userset_directs-user_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:userset_or_2", Relation: "or_userset", User: "user:uou_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_userset_directs-employee",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:userset_or_3", Relation: "or_userset", User: "employee:uou_3"},
				Expectation: true,
			},
			{
				Name:        "valid_userset_directs-employee_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:userset_or_4", Relation: "or_userset", User: "employee:uou_4"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_userset_directs-user_cond_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:userset_or_2", Relation: "or_userset", User: "user:uou_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "valid_userset_directs-employee_cond_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:userset_or_4", Relation: "or_userset", User: "employee:uou_4"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:      "valid_userset_directs-user_cond_no_cond",
				Tuple:     &openfgav1.TupleKey{Object: "usersets-user:userset_or_2", Relation: "or_userset", User: "user:uou_2"},
				ErrorCode: 2000,
			},

			{
				Name:      "valid_userset_directs-employee_cond_ino_cond",
				Tuple:     &openfgav1.TupleKey{Object: "usersets-user:userset_or_4", Relation: "or_userset", User: "employee:uou_4"},
				ErrorCode: 2000,
			},
			{
				Name:        "invalid_userset",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:userset_or_1", Relation: "or_userset", User: "user:userset_or_userset_invalid"},
				Expectation: false,
			},
			{
				Name:               "invalid_object",
				Tuple:              &openfgav1.TupleKey{Object: "usersets-user:userset_or_2", Relation: "or_userset", User: "user:userset_or_userset_invalid"},
				Expectation:        false,
				ListUsersErrorCode: 2000,
			},
		},
	},
	{
		Name: "usersets_and_userset",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:uau_1", Relation: "direct_cond", User: "user:uau_1", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "directs-user:uau_1", Relation: "direct_wild", User: "user:*"},

			{Object: "usersets-user:uau_1", Relation: "userset_to_computed_cond", User: "directs-user:uau_1#computed_cond"},
			{Object: "usersets-user:uau_1", Relation: "userset_to_computed_wild", User: "directs-user:uau_1#computed_wild"},

			{Object: "directs-employee:uau_1", Relation: "direct_cond", User: "employee:uau_1", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "directs-employee:uau_1", Relation: "direct_wild", User: "employee:*"},

			{Object: "usersets-user:uau_2", Relation: "userset_to_computed_cond", User: "directs-employee:uau_1#direct_cond"},
			{Object: "usersets-user:uau_2", Relation: "userset_to_computed_wild", User: "directs-employee:uau_1#direct_wild"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uau_1", Relation: "and_userset", User: "user:uau_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_user_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uau_1", Relation: "and_userset", User: "user:uau_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uau_1", Relation: "and_userset", User: "user:uau_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:      "no_condition",
				Tuple:     &openfgav1.TupleKey{Object: "usersets-user:uau_1", Relation: "and_userset", User: "user:uau_1"},
				ErrorCode: 2000,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uau_3", Relation: "and_userset", User: "user:uau_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:        "valid_employee",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uau_2", Relation: "and_userset", User: "employee:uau_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_employee_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uau_2", Relation: "and_userset", User: "employee:uau_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "invalid_employee",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:uau_2", Relation: "and_userset", User: "employee:uau_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
		},
	},
	{
		Name: "usersets_butnot_userset",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:bnu_1", Relation: "direct_wild", User: "user:*"},
			{Object: "directs-user:bnu_1", Relation: "direct", User: "user:bnu_2"},

			{Object: "usersets-user:bnu_1", Relation: "userset_cond", User: "directs-user:bnu_1#direct", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "usersets-user:bnu_1", Relation: "userset_cond_to_computed_wild", User: "directs-user:bnu_1#computed_wild", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:bnu_1", Relation: "butnot_userset", User: "user:bnu_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_user_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:bnu_1", Relation: "butnot_userset", User: "user:bnu_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:bnu_1", Relation: "butnot_userset", User: "user:bnu_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:bnu_2", Relation: "butnot_userset", User: "user:bnu_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:      "no_condition",
				Tuple:     &openfgav1.TupleKey{Object: "usersets-user:bnu_1", Relation: "butnot_userset", User: "user:bnu_1"},
				ErrorCode: 2000,
			},
		},
	},
	{
		Name: "usersets_nested_or_userset",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:nou_1", Relation: "direct", User: "user:nou_1"},
			{Object: "directs-user:nou_2", Relation: "direct_cond", User: "user:nou_2", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "directs-user:nou_3", Relation: "direct_wild", User: "user:*"},

			{Object: "usersets-user:nou_1", Relation: "userset_to_or_computed", User: "directs-user:nou_1#or_computed"}, // direct
			{Object: "usersets-user:nou_2", Relation: "userset_to_or_computed", User: "directs-user:nou_2#or_computed"}, // direct_cond
			{Object: "usersets-user:nou_3", Relation: "userset_to_or_computed", User: "directs-user:nou_3#or_computed"}, // direct_wild

			{Object: "directs-user:nou_4", Relation: "direct_wild_cond", User: "user:*", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "directs-user:nou_4", Relation: "direct", User: "user:5"},

			{Object: "usersets-user:nou_4", Relation: "userset_to_butnot_computed", User: "directs-user:nou_4#butnot_computed"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:                 "valid_user_direct",
				Tuple:                &openfgav1.TupleKey{Object: "usersets-user:nou_1", Relation: "nested_or_userset", User: "user:nou_1"},
				Expectation:          true,
				ListObjectsErrorCode: 2000, // any tuple with user:* and a condition and missing context will be un-evaluable
			},
			{
				Name:        "valid_user_direct_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:nou_2", Relation: "nested_or_userset", User: "user:nou_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_user_direct_cond_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:nou_2", Relation: "nested_or_userset", User: "user:nou_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:                 "valid_user_direct_wild",
				Tuple:                &openfgav1.TupleKey{Object: "usersets-user:nou_3", Relation: "nested_or_userset", User: "user:nou_3"},
				Expectation:          true,
				ListObjectsErrorCode: 2000, // any tuple with user:* and a condition and missing context will be un-evaluable
			},
			{
				Name:                 "invalid_user_direct",
				Tuple:                &openfgav1.TupleKey{Object: "usersets-user:nou_1", Relation: "nested_or_userset", User: "user:nou_2"},
				Expectation:          false,
				ListObjectsErrorCode: 2000, // any tuple with user:* and a condition and missing context will be un-evaluable
			},
			{
				Name:        "invalid_user_direct_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:nou_2", Relation: "nested_or_userset", User: "user:nou_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:      "user_direct_cond_no_condition",
				Tuple:     &openfgav1.TupleKey{Object: "usersets-user:nou_2", Relation: "nested_or_userset", User: "user:nou_2"},
				ErrorCode: 2000,
			},
			{
				Name:        "valid_user_butnot_computed",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:nou_4", Relation: "nested_or_userset", User: "user:nou_4"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},

			{
				Name:        "valid_user_butnot_computed_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:nou_4", Relation: "nested_or_userset", User: "user:nou_4"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "invalid_user_butnot_computed",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:nou_4", Relation: "nested_or_userset", User: "user:nou_5"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:      "butnot_computed_no_condition",
				Tuple:     &openfgav1.TupleKey{Object: "usersets-user:nou_4", Relation: "nested_or_userset", User: "user:nou_4"},
				ErrorCode: 2000,
			},
		},
	},
	{
		Name: "usersets_nested_and_userset", // TODO: more cases to be covered?
		Tuples: []*openfgav1.TupleKey{

			{Object: "directs-user:nau_1", Relation: "direct_cond", User: "user:nau_1", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "directs-user:nau_1", Relation: "direct_wild", User: "user:*"},

			{Object: "usersets-user:nau_1", Relation: "userset_to_and_computed", User: "directs-user:nau_1#and_computed"},
			{Object: "usersets-user:nau_1", Relation: "userset_to_or_computed", User: "directs-user:nau_1#or_computed"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:nau_1", Relation: "nested_and_userset", User: "user:nau_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_user_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:nau_1", Relation: "nested_and_userset", User: "user:nau_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:nau_1", Relation: "nested_and_userset", User: "user:nau_5"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:      "no_condition",
				Tuple:     &openfgav1.TupleKey{Object: "usersets-user:nau_1", Relation: "nested_and_userset", User: "user:nau_1"},
				ErrorCode: 2000,
			},
		},
	},
	{
		Name: "ttu_direct_userset",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:ttudu_1", Relation: "direct", User: "user:ttudu_1"},
			{Object: "ttus:ttudu_1", Relation: "mult_parent_types", User: "directs-user:ttudu_1"},

			{Object: "usersets-user:ttudu_1", Relation: "ttu_direct_userset", User: "ttus:ttudu_1#direct_pa_direct_ch"},

			{Object: "directs-employee:ttudu_1", Relation: "direct", User: "employee:ttudu_1"},
			{Object: "ttus:ttudu_2", Relation: "mult_parent_types", User: "directs-employee:ttudu_1"},

			{Object: "usersets-user:ttudu_2", Relation: "ttu_direct_userset", User: "ttus:ttudu_2#direct_pa_direct_ch"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttudu_1", Relation: "ttu_direct_userset", User: "user:ttudu_1"},
				Expectation: true,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttudu_1", Relation: "ttu_direct_userset", User: "user:ttudu_2"},
				Expectation: false,
			},
			{
				Name:        "valid_employee",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttudu_2", Relation: "ttu_direct_userset", User: "employee:ttudu_1"},
				Expectation: true,
			},
			{
				Name:        "invalid_employee",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttudu_2", Relation: "ttu_direct_userset", User: "employee:ttudu_2"},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttudu_3", Relation: "ttu_direct_userset", User: "user:ttudu_1"},
				Expectation: false,
			},
		},
	},
	{
		Name: "usersets_ttu_direct_cond_userset",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:ttdcu_1", Relation: "direct", User: "user:ttdcu_1"},
			{Object: "ttus:ttdcu_1", Relation: "mult_parent_types_cond", User: "directs-user:ttdcu_1", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},

			{Object: "directs-employee:ttdcu_2", Relation: "direct", User: "employee:ttdcu_2"},
			{Object: "ttus:ttdcu_2", Relation: "mult_parent_types_cond", User: "directs-employee:ttdcu_2", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},

			{Object: "usersets-user:ttdcu_1", Relation: "ttu_direct_cond_userset", User: "ttus:ttdcu_1#direct_cond_pa_direct_ch"},
			{Object: "usersets-user:ttdcu_2", Relation: "ttu_direct_cond_userset", User: "ttus:ttdcu_2#direct_cond_pa_direct_ch"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttdcu_1", Relation: "ttu_direct_cond_userset", User: "user:ttdcu_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttdcu_2", Relation: "ttu_direct_cond_userset", User: "employee:ttdcu_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_user_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttdcu_1", Relation: "ttu_direct_cond_userset", User: "user:ttdcu_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "valid_employee_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttdcu_2", Relation: "ttu_direct_cond_userset", User: "employee:ttdcu_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttdcu_1", Relation: "ttu_direct_cond_userset", User: "user:ttdcu_5"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:      "no_condition",
				Tuple:     &openfgav1.TupleKey{Object: "usersets-user:ttdcu_1", Relation: "ttu_direct_cond_userset", User: "user:ttdcu_1"},
				ErrorCode: 2000,
			},

			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttdcu_2", Relation: "ttu_direct_cond_userset", User: "user:ttdcu_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
		},
	},
	{
		Name: "userset_ttu_or_direct_userset",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:ttuodu_1", Relation: "direct", User: "user:ttuodu_1"},                                                                  // covers direct
			{Object: "directs-user:ttuodu_2", Relation: "direct_wild", User: "user:*"},                                                                    // covers direct_wild
			{Object: "directs-user:ttuodu_3", Relation: "direct_cond", User: "user:ttuodu_3", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}}, // covers direct_wild

			{Object: "ttus:ttuodu_1", Relation: "direct_parent", User: "directs-user:ttuodu_1"},
			{Object: "ttus:ttuodu_2", Relation: "direct_parent", User: "directs-user:ttuodu_2"},
			{Object: "ttus:ttuodu_3", Relation: "direct_parent", User: "directs-user:ttuodu_3"},

			{Object: "usersets-user:ttuodu_1", Relation: "ttu_or_direct_userset", User: "ttus:ttuodu_1#or_comp_from_direct_parent"},
			{Object: "usersets-user:ttuodu_2", Relation: "ttu_or_direct_userset", User: "ttus:ttuodu_2#or_comp_from_direct_parent"},
			{Object: "usersets-user:ttuodu_3", Relation: "ttu_or_direct_userset", User: "ttus:ttuodu_3#or_comp_from_direct_parent"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user_direct",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttuodu_1", Relation: "ttu_or_direct_userset", User: "user:ttuodu_1"},
				Expectation: true,
			},
			{
				Name:        "valid_user_direct_wild",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttuodu_2", Relation: "ttu_or_direct_userset", User: "user:ttuodu_2"},
				Expectation: true,
			},
			{
				Name:        "valid_user_direct_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttuodu_3", Relation: "ttu_or_direct_userset", User: "user:ttuodu_3"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_user_direct_cond_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttuodu_3", Relation: "ttu_or_direct_userset", User: "user:ttuodu_3"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:        "invalid_user_direct",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttuodu_1", Relation: "ttu_or_direct_userset", User: "user:ttuodu_2"},
				Expectation: false,
			},
			{
				Name:        "invalid_user_direct_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttuodu_3", Relation: "ttu_or_direct_userset", User: "user:ttuodu_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:      "user_direct_cond_no_condition",
				Tuple:     &openfgav1.TupleKey{Object: "usersets-user:ttuodu_3", Relation: "ttu_or_direct_userset", User: "user:ttuodu_3"},
				ErrorCode: 2000,
			},
		},
	},
	{
		Name: "usersets_ttu_and_direct_userset",
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:ttuadu_1", Relation: "direct_cond", User: "user:ttuadu_1", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "directs-user:ttuadu_1", Relation: "direct_wild", User: "user:*"},

			{Object: "ttus:ttuadu_1", Relation: "direct_cond_parent", User: "directs-user:ttuadu_1", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},

			{Object: "usersets-user:ttuadu_1", Relation: "ttu_and_direct_userset", User: "ttus:ttuadu_1#and_comp_from_direct_parent"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttuadu_1", Relation: "ttu_and_direct_userset", User: "user:ttuadu_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: true,
			},
			{
				Name:        "valid_user_invalid_cond",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttuadu_1", Relation: "ttu_and_direct_userset", User: "user:ttuadu_1"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
				Expectation: false,
			},
			{
				Name:      "no_condition",
				Tuple:     &openfgav1.TupleKey{Object: "usersets-user:ttuadu_1", Relation: "ttu_and_direct_userset", User: "user:ttuadu_1"},
				ErrorCode: 2000,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttuadu_1", Relation: "ttu_and_direct_userset", User: "user:ttuadu_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:ttuadu_2", Relation: "ttu_and_direct_userset", User: "user:ttuadu_2"},
				Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
				Expectation: false,
			},
		},
	},
	{
		Name: "usersets_tuple_cycle2",
		Tuples: []*openfgav1.TupleKey{
			// user exists so no cycle
			{Object: "directs-user:utc2_1", Relation: "tuple_cycle2", User: "user:utc2_1"},
			{Object: "ttus:utc2_1", Relation: "direct_parent", User: "directs-user:utc2_1"},
			{Object: "usersets-user:utc2_1", Relation: "tuple_cycle2", User: "ttus:utc2_1#tuple_cycle2"},
			{Object: "directs-user:utc2_1", Relation: "tuple_cycle2", User: "usersets-user:utc2_1#tuple_cycle2"},

			// missing user leads to a cycle
			{Object: "directs-user:utc2_4", Relation: "tuple_cycle2", User: "usersets-user:utc2_4#tuple_cycle2"},
			{Object: "ttus:utc2_4", Relation: "direct_parent", User: "directs-user:utc2_4"},
			{Object: "usersets-user:utc2_4", Relation: "tuple_cycle2", User: "ttus:utc2_4#tuple_cycle2"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utc2_1", Relation: "tuple_cycle2", User: "user:utc2_1"},
				Expectation: true,
			},
			{
				Name:        "cycle",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utc2_4", Relation: "tuple_cycle2", User: "user:utc2_1"},
				Expectation: false,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utc2_1", Relation: "tuple_cycle2", User: "user:utc2_3"},
				Expectation: false,
			},
		},
	},
	{
		Name: "usersets_tuple_cycle3",
		Tuples: []*openfgav1.TupleKey{
			// user exists so no cycle
			{Object: "ttus:utc3_1", Relation: "userset_parent", User: "usersets-user:utc3_1"},
			{Object: "complexity3:utc3_1", Relation: "cycle_nested", User: "ttus:utc3_1#tuple_cycle3"},
			{Object: "directs-user:utc3_1", Relation: "tuple_cycle3", User: "user:utc3_1"},
			{Object: "directs-user:utc3_1", Relation: "tuple_cycle3", User: "complexity3:utc3_1#cycle_nested"},
			{Object: "usersets-user:utc3_1", Relation: "tuple_cycle3", User: "directs-user:utc3_1#compute_tuple_cycle3"},

			// missing user leads to cycle
			{Object: "ttus:utc3_4", Relation: "userset_parent", User: "usersets-user:utc3_4"},
			{Object: "complexity3:utc3_4", Relation: "cycle_nested", User: "ttus:utc3_4#tuple_cycle3"},
			{Object: "directs-user:utc3_4", Relation: "tuple_cycle3", User: "complexity3:utc3_4#cycle_nested"},
			{Object: "usersets-user:utc3_4", Relation: "tuple_cycle3", User: "directs-user:utc3_4#compute_tuple_cycle3"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utc3_1", Relation: "tuple_cycle3", User: "user:utc3_1"},
				Expectation: true,
			},
			{
				Name:        "cycle",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utc3_4", Relation: "tuple_cycle3", User: "user:utc3_1"},
				Expectation: false,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:utc3_1", Relation: "tuple_cycle3", User: "user:utc3_2"},
				Expectation: false,
			},
		},
	},
}
