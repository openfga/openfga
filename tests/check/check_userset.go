package check

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

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
		Name: "usersets_or_userset", // does not cover condition
		Tuples: []*openfgav1.TupleKey{
			{Object: "directs-user:userset_or_1", Relation: "direct", User: "user:userset_or_userset_valid"},
			{Object: "usersets-user:userset_or_1", Relation: "userset", User: "directs-user:userset_or_1#direct"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_userset_directs-user",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:userset_or_1", Relation: "or_userset", User: "user:userset_or_userset_valid"},
				Expectation: true,
			},
			{
				Name:        "invalid_userset",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:userset_or_1", Relation: "or_userset", User: "user:userset_or_userset_invalid"},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "usersets-user:userset_or_2", Relation: "or_userset", User: "user:userset_or_userset_invalid"},
				Expectation: false,
			},
		},
	},
}
