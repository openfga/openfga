package check

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	checktest "github.com/openfga/openfga/internal/test/check"
)

var complexityThreeTestingModelTest = []*stage{
	{
		Name: "ttu_userset_ttu",
		Tuples: []*openfgav1.TupleKey{
			// passing case
			{Object: "complexity3:complexity_3_ttu_userset_ttu_1", Relation: "userset_parent", User: "usersets-user:complexity_3_ttu_userset_ttu_1"},
			{Object: "usersets-user:complexity_3_ttu_userset_ttu_1", Relation: "ttu_direct_userset", User: "ttus:complexity_3_ttu_userset_ttu_1#direct_pa_direct_ch"},
			{Object: "ttus:complexity_3_ttu_userset_ttu_1", Relation: "mult_parent_types", User: "directs-user:complexity_3_ttu_userset_ttu_1"},
			{Object: "directs-user:complexity_3_ttu_userset_ttu_1", Relation: "direct", User: "user:complexity_3_ttu_userset_ttu_1"},
			// unconnected_directs_user
			{Object: "directs-user:complexity_3_ttu_userset_ttu_unconnected_directs_user", Relation: "direct", User: "user:complexity_3_ttu_userset_ttu_unconnected_directs_user"},
			// unconnected ttus
			{Object: "directs-user:complexity_3_ttu_userset_ttu_unconnected_ttus", Relation: "direct", User: "user:complexity_3_ttu_userset_ttu_unconnected_ttus"},
			{Object: "ttus:complexity_3_ttu_userset_ttu_unconnected_ttus", Relation: "mult_parent_types", User: "directs-user:complexity_3_ttu_userset_ttu_unconnected_ttus"},
			// unconnected usersets-user
			{Object: "directs-user:complexity_3_ttu_userset_ttu_unconnected_usersets_users", Relation: "direct", User: "user:complexity_3_ttu_userset_ttu_unconnected_usersets_users"},
			{Object: "ttus:complexity_3_ttu_userset_ttu_unconnected_usersets_users", Relation: "mult_parent_types", User: "directs-user:complexity_3_ttu_userset_ttu_unconnected_usersets_users"},
			{Object: "usersets-user:complexity_3_ttu_userset_ttu_unconnected_usersets_users", Relation: "ttu_direct_userset", User: "ttus:complexity_3_ttu_userset_ttu_unconnected_usersets_users#direct_pa_direct_ch"},
			// complexity unconnected userset
			{Object: "complexity3:complexity_3_ttu_userset_ttu_complexity_unconnected_userset", Relation: "userset_parent", User: "usersets-user:complexity_3_ttu_userset_ttu_complexity_unconnected_userset"},
			// complexity unconnected ttu
			{Object: "complexity3:complexity_3_ttu_userset_ttu_complexity_unconnected_ttu", Relation: "userset_parent", User: "usersets-user:complexity_3_ttu_userset_ttu_complexity_unconnected_ttu"},
			{Object: "usersets-user:complexity_3_ttu_userset_ttu_complexity_unconnected_ttu", Relation: "ttu_direct_userset", User: "ttus:complexity_3_ttu_userset_ttu_complexity_unconnected_ttu#direct_pa_direct_ch"},
			// complexity unconnected direct users (empty)
			{Object: "complexity3:complexity_3_ttu_userset_ttu_complexity_unconnected_direct_users", Relation: "userset_parent", User: "usersets-user:complexity_3_ttu_userset_ttu_complexity_unconnected_direct_users"},
			{Object: "usersets-user:complexity_3_ttu_userset_ttu_complexity_unconnected_direct_users", Relation: "ttu_direct_userset", User: "ttus:complexity_3_ttu_userset_ttu_complexity_unconnected_direct_users#direct_pa_direct_ch"},
			{Object: "ttus:complexity_3_ttu_userset_ttu_complexity_unconnected_direct_users", Relation: "mult_parent_types", User: "directs-user:complexity_3_ttu_userset_ttu_complexity_unconnected_direct_users"},
			// complexity unconnected direct users (other)
			{Object: "complexity3:complexity_3_ttu_userset_ttu_complexity_unconnected_direct_users_2", Relation: "userset_parent", User: "usersets-user:complexity_3_ttu_userset_ttu_complexity_unconnected_direct_users_2"},
			{Object: "usersets-user:complexity_3_ttu_userset_ttu_complexity_unconnected_direct_users_2", Relation: "ttu_direct_userset", User: "ttus:complexity_3_ttu_userset_ttu_complexity_unconnected_direct_users_2#direct_pa_direct_ch"},
			{Object: "ttus:complexity_3_ttu_userset_ttu_complexity_unconnected_direct_users_2", Relation: "mult_parent_types", User: "directs-user:complexity_3_ttu_userset_ttu_complexity_unconnected_direct_users_2"},
			{Object: "directs-user:complexity_3_ttu_userset_ttu_complexity_unconnected_direct_users_2", Relation: "direct", User: "user:complexity_3_ttu_userset_ttu_complexity_unconnected_direct_users_2"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_1", Relation: "ttu_userset_ttu", User: "user:complexity_3_ttu_userset_ttu_1"},
				Expectation: true,
			},
			{
				Name:        "valid_ttus",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_1", Relation: "ttu_userset_ttu", User: "ttus:complexity_3_ttu_userset_ttu_1#direct_pa_direct_ch"},
				Expectation: true,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_1", Relation: "ttu_userset_ttu", User: "user:complexity_3_ttu_userset_ttu_invalid"},
				Expectation: false,
			},
			{
				Name:        "unconnected_directs_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_1", Relation: "ttu_userset_ttu", User: "user:complexity_3_ttu_userset_ttu_unconnected_directs_user"},
				Expectation: false,
			},
			{
				Name:        "unconnected_ttus",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_1", Relation: "ttu_userset_ttu", User: "user:complexity_3_ttu_userset_ttu_unconnected_ttus"},
				Expectation: false,
			},
			{
				Name:        "unconnected_usersets",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_1", Relation: "ttu_userset_ttu", User: "user:complexity_3_ttu_userset_ttu_unconnected_ttus"},
				Expectation: false,
			},
			{
				Name:        "invalid_complexity",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_invalid_complexity", Relation: "ttu_userset_ttu", User: "user:complexity_3_ttu_userset_ttu_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_userset",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_complexity_unconnected_userset", Relation: "ttu_userset_ttu", User: "user:complexity_3_ttu_userset_ttu_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_ttu",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_complexity_unconnected_ttu", Relation: "ttu_userset_ttu", User: "user:complexity_3_ttu_userset_ttu_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_direct_users",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_complexity_unconnected_direct_users", Relation: "ttu_userset_ttu", User: "user:complexity_3_ttu_userset_ttu_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_direct_users_other",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_complexity_unconnected_direct_users_2", Relation: "ttu_userset_ttu", User: "user:complexity_3_ttu_userset_ttu_1"},
				Expectation: false,
			},
		},
	},
	{
		Name: "ttu_ttu_userset",
		Tuples: []*openfgav1.TupleKey{
			// base case
			{Object: "complexity3:complexity_3_ttu_ttu_userset_1", Relation: "ttu_parent", User: "ttus:complexity_3_ttu_ttu_userset_1"},
			{Object: "ttus:complexity_3_ttu_ttu_userset_1", Relation: "userset_parent", User: "usersets-user:complexity_3_ttu_ttu_userset_1"},
			{Object: "usersets-user:complexity_3_ttu_ttu_userset_1", Relation: "userset", User: "directs-user:complexity_3_ttu_ttu_userset_1#direct"},
			{Object: "directs-user:complexity_3_ttu_ttu_userset_1", Relation: "direct", User: "user:complexity_3_ttu_ttu_userset_1"},
			// unconnected direct user
			{Object: "directs-user:complexity_3_ttu_ttu_userset_unconnected_directs_user", Relation: "direct", User: "user:complexity_3_ttu_ttu_userset_unconnected_directs_user"},
			// unconnected usersets
			{Object: "usersets-user:complexity_3_ttu_ttu_userset_unconnected_usersets", Relation: "userset", User: "directs-user:complexity_3_ttu_ttu_userset_unconnected_usersets#direct"},
			{Object: "directs-user:complexity_3_ttu_ttu_userset_unconnected_usersets", Relation: "direct", User: "user:complexity_3_ttu_ttu_userset_unconnected_usersets"},
			// unconnected ttus
			{Object: "ttus:complexity_3_ttu_ttu_userset_unconnected_ttus", Relation: "userset_parent", User: "usersets-user:complexity_3_ttu_ttu_userset_unconnected_ttus"},
			{Object: "usersets-user:complexity_3_ttu_ttu_userset_unconnected_ttus", Relation: "userset", User: "directs-user:complexity_3_ttu_ttu_userset_unconnected_ttus#direct"},
			{Object: "directs-user:complexity_3_ttu_ttu_userset_unconnected_ttus", Relation: "direct", User: "user:complexity_3_ttu_ttu_userset_unconnected_ttus"},
			// complexity unconnected ttus
			{Object: "complexity3:complexity_3_ttu_ttu_userset_complexity_unconnected_ttus", Relation: "ttu_parent", User: "ttus:complexity_3_ttu_ttu_userset_complexity_unconnected_ttus"},
			// complexity unconnected userset
			{Object: "complexity3:complexity_3_ttu_ttu_userset_complexity_unconnected_userset", Relation: "ttu_parent", User: "ttus:complexity_3_ttu_ttu_userset_complexity_unconnected_userset"},
			{Object: "ttus:complexity_3_ttu_ttu_userset_complexity_unconnected_userset", Relation: "userset_parent", User: "usersets-user:complexity_3_ttu_ttu_userset_complexity_unconnected_userset"},
			// complexity unconnected directs_user (empty)
			{Object: "complexity3:complexity_3_ttu_ttu_userset_complexity_unconnected_directs_user", Relation: "ttu_parent", User: "ttus:complexity_3_ttu_ttu_userset_complexity_unconnected_directs_user"},
			{Object: "ttus:complexity_3_ttu_ttu_userset_complexity_unconnected_directs_user", Relation: "userset_parent", User: "usersets-user:complexity_3_ttu_ttu_userset_complexity_unconnected_directs_user"},
			{Object: "usersets-user:complexity_3_ttu_ttu_userset_complexity_unconnected_directs_user", Relation: "userset", User: "directs-user:complexity_3_ttu_ttu_userset_complexity_unconnected_directs_user#direct"},
			// complexity unconnected directs_user (unmatch)
			{Object: "complexity3:complexity_3_ttu_ttu_userset_complexity_unconnected_directs_user_2", Relation: "ttu_parent", User: "ttus:complexity_3_ttu_ttu_userset_complexity_unconnected_directs_user_2"},
			{Object: "ttus:complexity_3_ttu_ttu_userset_complexity_unconnected_directs_user_2", Relation: "userset_parent", User: "usersets-user:complexity_3_ttu_ttu_userset_complexity_unconnected_directs_user_2"},
			{Object: "usersets-user:complexity_3_ttu_ttu_userset_complexity_unconnected_directs_user_2", Relation: "userset", User: "directs-user:complexity_3_ttu_ttu_userset_complexity_unconnected_directs_user_2#direct"},
			{Object: "directs-user:complexity_3_ttu_ttu_userset_complexity_unconnected_directs_user_2", Relation: "direct", User: "user:complexity_3_ttu_ttu_userset_complexity_unconnected_directs_user_2"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_ttu_userset_1", Relation: "ttu_ttu_userset", User: "user:complexity_3_ttu_ttu_userset_1"},
				Expectation: true,
			},
			{
				Name:        "valid_directs-user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_ttu_userset_1", Relation: "ttu_ttu_userset", User: "directs-user:complexity_3_ttu_ttu_userset_1#direct"},
				Expectation: true,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_ttu_userset_1", Relation: "ttu_ttu_userset", User: "user:complexity_3_ttu_ttu_userset_invalid"},
				Expectation: false,
			},
			{
				Name:        "unconnected_directs_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_ttu_userset_1", Relation: "ttu_ttu_userset", User: "user:complexity_3_ttu_ttu_userset_unconnected_directs_user"},
				Expectation: false,
			},
			{
				Name:        "unconnected_directs_usersets",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_ttu_userset_1", Relation: "ttu_ttu_userset", User: "user:complexity_3_ttu_ttu_userset_unconnected_usersets"},
				Expectation: false,
			},
			{
				Name:        "unconnected_ttus",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_ttu_userset_1", Relation: "ttu_ttu_userset", User: "user:complexity_3_ttu_ttu_userset_unconnected_ttus"},
				Expectation: false,
			},
			{
				Name:        "invalid_complexity",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_ttu_userset_invalid_complexity", Relation: "ttu_ttu_userset", User: "user:complexity_3_ttu_ttu_userset_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_ttus",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_ttu_userset_complexity_unconnected_ttus", Relation: "ttu_ttu_userset", User: "user:complexity_3_ttu_ttu_userset_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_userset_empty",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_ttu_userset_complexity_unconnected_userset", Relation: "ttu_ttu_userset", User: "user:complexity_3_ttu_ttu_userset_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_userset_other",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_ttu_userset_complexity_unconnected_userset_2", Relation: "ttu_ttu_userset", User: "user:complexity_3_ttu_ttu_userset_1"},
				Expectation: false,
			},
		},
	},
	{
		Name: "userset_ttu_userset",
		Tuples: []*openfgav1.TupleKey{
			// base case
			{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "userset_ttu_userset", User: "ttus:complexity_3_userset_ttu_userset_1#userset_pa_userset_ch"},
			{Object: "ttus:complexity_3_userset_ttu_userset_1", Relation: "userset_parent", User: "usersets-user:complexity_3_userset_ttu_userset_1"},
			{Object: "usersets-user:complexity_3_userset_ttu_userset_1", Relation: "userset", User: "directs-user:complexity_3_userset_ttu_userset_1#direct"},
			{Object: "directs-user:complexity_3_userset_ttu_userset_1", Relation: "direct", User: "user:complexity_3_userset_ttu_userset_1"},
			// unconnected direct user
			{Object: "directs-user:complexity_3_userset_ttu_userset_unconnected_direct_user", Relation: "direct", User: "user:complexity_3_userset_ttu_userset_unconnected_direct_user"},
			// unconnected userset
			{Object: "usersets-user:complexity_3_userset_ttu_userset_unconnected_userset", Relation: "userset", User: "directs-user:complexity_3_userset_ttu_userset_unconnected_userset#direct"},
			{Object: "directs-user:complexity_3_userset_ttu_userset_unconnected_userset", Relation: "direct", User: "user:complexity_3_userset_ttu_userset_unconnected_userset"},
			// unconnected ttu
			{Object: "ttus:complexity_3_userset_ttu_userset_unconnected_ttu", Relation: "userset_parent", User: "usersets-user:complexity_3_userset_ttu_userset_unconnected_ttu"},
			{Object: "usersets-user:complexity_3_userset_ttu_userset_unconnected_ttu", Relation: "userset", User: "directs-user:complexity_3_userset_ttu_userset_unconnected_ttu#direct"},
			{Object: "directs-user:complexity_3_userset_ttu_userset_unconnected_ttu", Relation: "direct", User: "user:complexity_3_userset_ttu_userset_unconnected_ttu"},
			// complexity 3 unconnected ttus
			{Object: "complexity3:complexity_3_userset_ttu_userset_complexity_unconnected_ttu", Relation: "userset_ttu_userset", User: "ttus:complexity_3_userset_ttu_userset_complexity_unconnected_ttu#userset_pa_userset_ch"},
			// complexity 3 unconnected usersets
			{Object: "complexity3:complexity_3_userset_ttu_userset_complexity_unconnected_usersets", Relation: "userset_ttu_userset", User: "ttus:complexity_3_userset_ttu_userset_complexity_unconnected_usersets#userset_pa_userset_ch"},
			{Object: "ttus:complexity_3_userset_ttu_userset_complexity_unconnected_usersets", Relation: "userset_parent", User: "usersets-user:complexity_3_userset_ttu_userset_complexity_unconnected_usersets"},
			// complexity 3 unconnected direct users (empty)
			{Object: "complexity3:complexity_3_userset_ttu_userset_complexity_unconnected_direct_users", Relation: "userset_ttu_userset", User: "ttus:complexity_3_userset_ttu_userset_complexity_unconnected_direct_users#userset_pa_userset_ch"},
			{Object: "ttus:complexity_3_userset_ttu_userset_complexity_unconnected_direct_users", Relation: "userset_parent", User: "usersets-user:complexity_3_userset_ttu_userset_complexity_unconnected_direct_users"},
			{Object: "usersets-user:complexity_3_userset_ttu_userset_complexity_unconnected_direct_users", Relation: "userset", User: "directs-user:complexity_3_userset_ttu_userset_complexity_unconnected_direct_users#direct"},
			// complexity 3 unconnected direct users (other user)
			{Object: "complexity3:complexity_3_userset_ttu_userset_complexity_unconnected_direct_users_2", Relation: "userset_ttu_userset", User: "ttus:complexity_3_userset_ttu_userset_complexity_unconnected_direct_users_2#userset_pa_userset_ch"},
			{Object: "ttus:complexity_3_userset_ttu_userset_complexity_unconnected_direct_users_2", Relation: "userset_parent", User: "usersets-user:complexity_3_userset_ttu_userset_complexity_unconnected_direct_users_2"},
			{Object: "usersets-user:complexity_3_userset_ttu_userset_complexity_unconnected_direct_users_2", Relation: "userset", User: "directs-user:complexity_3_userset_ttu_userset_complexity_unconnected_direct_users_2#direct"},
			{Object: "directs-user:complexity_3_userset_ttu_userset_complexity_unconnected_direct_users_2", Relation: "direct", User: "user:complexity_3_userset_ttu_userset_complexity_unconnected_direct_users_2"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "userset_ttu_userset", User: "user:complexity_3_userset_ttu_userset_1"},
				Expectation: true,
			},
			{
				Name:        "valid_ttu",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "userset_ttu_userset", User: "ttus:complexity_3_userset_ttu_userset_1#userset_pa_userset_ch"},
				Expectation: true,
			},
			{
				Name:        "valid_direct_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "userset_ttu_userset", User: "directs-user:complexity_3_userset_ttu_userset_1#direct"},
				Expectation: true,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "userset_ttu_userset", User: "user:complexity_3_userset_ttu_userset_invalid"},
				Expectation: false,
			},
			{
				Name:        "unconnected_direct_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "userset_ttu_userset", User: "user:complexity_3_userset_ttu_userset_unconnected_direct_user"},
				Expectation: false,
			},
			{
				Name:        "unconnected_userset",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "userset_ttu_userset", User: "user:complexity_3_userset_ttu_userset_unconnected_userset"},
				Expectation: false,
			},
			{
				Name:        "unconnected_ttu",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "userset_ttu_userset", User: "user:complexity_3_userset_ttu_userset_unconnected_ttu"},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_invalid_object", Relation: "userset_ttu_userset", User: "user:complexity_3_userset_ttu_userset_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_ttu",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_complexity_unconnected_ttu", Relation: "userset_ttu_userset", User: "user:complexity_3_userset_ttu_userset_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_usersets_empty",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_complexity_unconnected_usersets", Relation: "userset_ttu_userset", User: "user:complexity_3_userset_ttu_userset_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_usersets_other",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_complexity_unconnected_usersets_2", Relation: "userset_ttu_userset", User: "user:complexity_3_userset_ttu_userset_1"},
				Expectation: false,
			},
		},
	},
	{
		Name: "userset_userset_ttu",
		Tuples: []*openfgav1.TupleKey{
			// base case
			{Object: "complexity3:complexity_3_userset_userset_ttu_1", Relation: "userset_userset_ttu", User: "usersets-user:complexity_3_userset_userset_ttu_1#ttu_direct_userset"},
			{Object: "usersets-user:complexity_3_userset_userset_ttu_1", Relation: "ttu_direct_userset", User: "ttus:complexity_3_userset_userset_ttu_1#direct_pa_direct_ch"},
			{Object: "ttus:complexity_3_userset_userset_ttu_1", Relation: "mult_parent_types", User: "directs-user:complexity_3_userset_userset_ttu_1"},
			{Object: "directs-user:complexity_3_userset_userset_ttu_1", Relation: "direct", User: "user:complexity_3_userset_userset_ttu_1"},
			// unconnected direct user
			{Object: "directs-user:complexity_3_userset_userset_ttu_unconnected_direct_user", Relation: "direct", User: "user:complexity_3_userset_userset_ttu_unconnected_direct_user"},
			// unconnected TTU
			{Object: "ttus:complexity_3_userset_userset_ttu_unconnected_ttu", Relation: "mult_parent_types", User: "directs-user:complexity_3_userset_userset_ttu_unconnected_ttu"},
			{Object: "directs-user:complexity_3_userset_userset_ttu_unconnected_ttu", Relation: "direct", User: "user:complexity_3_userset_userset_ttu_unconnected_ttu"},
			// unconnected userset
			{Object: "usersets-user:complexity_3_userset_userset_ttu_unconnected_userset", Relation: "ttu_direct_userset", User: "ttus:complexity_3_userset_userset_ttu_unconnected_userset#direct_pa_direct_ch"},
			{Object: "ttus:complexity_3_userset_userset_ttu_unconnected_userset", Relation: "mult_parent_types", User: "directs-user:complexity_3_userset_userset_ttu_unconnected_userset"},
			{Object: "directs-user:complexity_3_userset_userset_ttu_unconnected_userset", Relation: "direct", User: "user:complexity_3_userset_userset_ttu_unconnected_userset"},
			// complexity 3 unconnected userset
			{Object: "complexity3:complexity_3_userset_userset_ttu_complexity_unconnected_userset", Relation: "userset_userset_ttu", User: "usersets-user:complexity_3_userset_userset_ttu_complexity_unconnected_userset#ttu_direct_userset"},
			// complexity 3 unconnected ttus
			{Object: "complexity3:complexity_3_userset_userset_ttu_complexity_unconnected_ttu", Relation: "userset_userset_ttu", User: "usersets-user:complexity_3_userset_userset_ttu_complexity_unconnected_ttu#ttu_direct_userset"},
			{Object: "usersets-user:complexity_3_userset_userset_ttu_complexity_unconnected_ttu", Relation: "ttu_direct_userset", User: "ttus:complexity_3_userset_userset_ttu_complexity_unconnected_ttu#direct_pa_direct_ch"},
			// complexity 3 unconnected direct user (empty)
			{Object: "complexity3:complexity_3_userset_userset_ttu_complexity_unconnected_direct_user", Relation: "userset_userset_ttu", User: "usersets-user:complexity_3_userset_userset_ttu_complexity_unconnected_direct_user#ttu_direct_userset"},
			{Object: "usersets-user:complexity_3_userset_userset_ttu_complexity_unconnected_direct_user", Relation: "ttu_direct_userset", User: "ttus:complexity_3_userset_userset_ttu_complexity_unconnected_direct_user#direct_pa_direct_ch"},
			{Object: "ttus:complexity_3_userset_userset_ttu_complexity_unconnected_direct_user", Relation: "mult_parent_types", User: "directs-user:complexity_3_userset_userset_ttu_complexity_unconnected_direct_user"},
			// complexity 3 unconnected direct user (other)
			{Object: "complexity3:complexity_3_userset_userset_ttu_complexity_unconnected_direct_user_2", Relation: "userset_userset_ttu", User: "usersets-user:complexity_3_userset_userset_ttu_complexity_unconnected_direct_user_2#ttu_direct_userset"},
			{Object: "usersets-user:complexity_3_userset_userset_ttu_complexity_unconnected_direct_user_2", Relation: "ttu_direct_userset", User: "ttus:complexity_3_userset_userset_ttu_complexity_unconnected_direct_user_2#direct_pa_direct_ch"},
			{Object: "ttus:complexity_3_userset_userset_ttu_complexity_unconnected_direct_user_2", Relation: "mult_parent_types", User: "directs-user:complexity_3_userset_userset_ttu_complexity_unconnected_direct_user_2"},
			{Object: "directs-user:complexity_3_userset_userset_ttu_complexity_unconnected_direct_user_2", Relation: "direct", User: "user:complexity_3_userset_userset_ttu_complexity_unconnected_direct_user_2"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_userset_ttu_1", Relation: "userset_userset_ttu", User: "user:complexity_3_userset_userset_ttu_1"},
				Expectation: true,
			},
			{
				Name:        "valid_userset",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_userset_ttu_1", Relation: "userset_userset_ttu", User: "usersets-user:complexity_3_userset_userset_ttu_1#ttu_direct_userset"},
				Expectation: true,
			},
			{
				Name:        "valid_ttus",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_userset_ttu_1", Relation: "userset_userset_ttu", User: "ttus:complexity_3_userset_userset_ttu_1#direct_pa_direct_ch"},
				Expectation: true,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_userset_ttu_1", Relation: "userset_userset_ttu", User: "user:complexity_3_userset_userset_ttu_invalid"},
				Expectation: false,
			},
			{
				Name:        "unconnected_direct_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_userset_ttu_1", Relation: "userset_userset_ttu", User: "user:complexity_3_userset_userset_ttu_unconnected_direct_user"},
				Expectation: false,
			},
			{
				Name:        "unconnected_ttu",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_userset_ttu_1", Relation: "userset_userset_ttu", User: "user:complexity_3_userset_userset_ttu_unconnected_ttu"},
				Expectation: false,
			},
			{
				Name:        "unconnected_userset",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_userset_ttu_1", Relation: "userset_userset_ttu", User: "user:complexity_3_userset_userset_ttu_unconnected_userset"},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_userset_ttu_invalid", Relation: "userset_userset_ttu", User: "user:complexity_3_userset_userset_ttu_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_userset",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_userset_ttu_complexity_unconnected_userset", Relation: "userset_userset_ttu", User: "user:complexity_3_userset_userset_ttu_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_ttus",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_userset_ttu_complexity_unconnected_ttu", Relation: "userset_userset_ttu", User: "user:complexity_3_userset_userset_ttu_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_direct_user_empty",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_userset_ttu_complexity_unconnected_direct_user", Relation: "userset_userset_ttu", User: "user:complexity_3_userset_userset_ttu_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_direct_user_other",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_userset_ttu_complexity_unconnected_direct_user_2", Relation: "userset_userset_ttu", User: "user:complexity_3_userset_userset_ttu_1"},
				Expectation: false,
			},
		},
	},
	{
		Name:   "compute_ttu_userset_ttu",
		Tuples: []*openfgav1.TupleKey{
			// see ttu_userset_ttu
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_1", Relation: "compute_ttu_userset_ttu", User: "user:complexity_3_ttu_userset_ttu_1"},
				Expectation: true,
			},
			{
				Name:        "valid_ttus",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_1", Relation: "compute_ttu_userset_ttu", User: "ttus:complexity_3_ttu_userset_ttu_1#direct_pa_direct_ch"},
				Expectation: true,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_1", Relation: "compute_ttu_userset_ttu", User: "user:complexity_3_ttu_userset_ttu_invalid"},
				Expectation: false,
			},
			{
				Name:        "unconnected_directs_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_1", Relation: "compute_ttu_userset_ttu", User: "user:complexity_3_ttu_userset_ttu_unconnected_directs_user"},
				Expectation: false,
			},
			{
				Name:        "unconnected_ttus",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_1", Relation: "compute_ttu_userset_ttu", User: "user:complexity_3_ttu_userset_ttu_unconnected_ttus"},
				Expectation: false,
			},
			{
				Name:        "unconnected_usersets",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_1", Relation: "compute_ttu_userset_ttu", User: "user:complexity_3_ttu_userset_ttu_unconnected_ttus"},
				Expectation: false,
			},
			{
				Name:        "invalid_complexity",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_invalid_complexity", Relation: "compute_ttu_userset_ttu", User: "user:complexity_3_ttu_userset_ttu_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_userset",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_complexity_unconnected_userset", Relation: "compute_ttu_userset_ttu", User: "user:complexity_3_ttu_userset_ttu_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_ttu",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_complexity_unconnected_ttu", Relation: "compute_ttu_userset_ttu", User: "user:complexity_3_ttu_userset_ttu_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_direct_users",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_complexity_unconnected_direct_users", Relation: "compute_ttu_userset_ttu", User: "user:complexity_3_ttu_userset_ttu_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_direct_users_other",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_complexity_unconnected_direct_users_2", Relation: "compute_ttu_userset_ttu", User: "user:complexity_3_ttu_userset_ttu_1"},
				Expectation: false,
			},
		},
	},
	{
		Name:   "compute_userset_ttu_userset",
		Tuples: []*openfgav1.TupleKey{
			// see userset_ttu_userset
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "compute_userset_ttu_userset", User: "user:complexity_3_userset_ttu_userset_1"},
				Expectation: true,
			},
			{
				Name:        "valid_ttu",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "compute_userset_ttu_userset", User: "ttus:complexity_3_userset_ttu_userset_1#userset_pa_userset_ch"},
				Expectation: true,
			},
			{
				Name:        "valid_direct_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "compute_userset_ttu_userset", User: "directs-user:complexity_3_userset_ttu_userset_1#direct"},
				Expectation: true,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "compute_userset_ttu_userset", User: "user:complexity_3_userset_ttu_userset_invalid"},
				Expectation: false,
			},
			{
				Name:        "unconnected_direct_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "compute_userset_ttu_userset", User: "user:complexity_3_userset_ttu_userset_unconnected_direct_user"},
				Expectation: false,
			},
			{
				Name:        "unconnected_userset",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "compute_userset_ttu_userset", User: "user:complexity_3_userset_ttu_userset_unconnected_userset"},
				Expectation: false,
			},
			{
				Name:        "unconnected_ttu",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "compute_userset_ttu_userset", User: "user:complexity_3_userset_ttu_userset_unconnected_ttu"},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_invalid_object", Relation: "compute_userset_ttu_userset", User: "user:complexity_3_userset_ttu_userset_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_ttu",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_complexity_unconnected_ttu", Relation: "compute_userset_ttu_userset", User: "user:complexity_3_userset_ttu_userset_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_usersets_empty",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_complexity_unconnected_usersets", Relation: "compute_userset_ttu_userset", User: "user:complexity_3_userset_ttu_userset_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_usersets_other",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_complexity_unconnected_usersets_2", Relation: "compute_userset_ttu_userset", User: "user:complexity_3_userset_ttu_userset_1"},
				Expectation: false,
			},
		},
	},
	{
		Name:   "or_compute_complex3",
		Tuples: []*openfgav1.TupleKey{
			// see ttu_userset_ttu and userset_ttu_userset
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_1", Relation: "or_compute_complex3", User: "user:complexity_3_ttu_userset_ttu_1"},
				Expectation: true,
			},
			{
				Name:        "valid_ttus",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_1", Relation: "or_compute_complex3", User: "ttus:complexity_3_ttu_userset_ttu_1#direct_pa_direct_ch"},
				Expectation: true,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_1", Relation: "or_compute_complex3", User: "user:complexity_3_ttu_userset_ttu_invalid"},
				Expectation: false,
			},
			{
				Name:        "unconnected_directs_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_1", Relation: "or_compute_complex3", User: "user:complexity_3_ttu_userset_ttu_unconnected_directs_user"},
				Expectation: false,
			},
			{
				Name:        "unconnected_ttus",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_1", Relation: "or_compute_complex3", User: "user:complexity_3_ttu_userset_ttu_unconnected_ttus"},
				Expectation: false,
			},
			{
				Name:        "unconnected_usersets",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_1", Relation: "or_compute_complex3", User: "user:complexity_3_ttu_userset_ttu_unconnected_ttus"},
				Expectation: false,
			},
			{
				Name:        "invalid_complexity",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_invalid_complexity", Relation: "or_compute_complex3", User: "user:complexity_3_ttu_userset_ttu_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_userset",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_complexity_unconnected_userset", Relation: "or_compute_complex3", User: "user:complexity_3_ttu_userset_ttu_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_ttu",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_complexity_unconnected_ttu", Relation: "or_compute_complex3", User: "user:complexity_3_ttu_userset_ttu_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_direct_users",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_complexity_unconnected_direct_users", Relation: "or_compute_complex3", User: "user:complexity_3_ttu_userset_ttu_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_direct_users_other",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_ttu_userset_ttu_complexity_unconnected_direct_users_2", Relation: "or_compute_complex3", User: "user:complexity_3_ttu_userset_ttu_1"},
				Expectation: false,
			},
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "or_compute_complex3", User: "user:complexity_3_userset_ttu_userset_1"},
				Expectation: true,
			},
			{
				Name:        "valid_ttu",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "or_compute_complex3", User: "ttus:complexity_3_userset_ttu_userset_1#userset_pa_userset_ch"},
				Expectation: true,
			},
			{
				Name:        "valid_direct_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "or_compute_complex3", User: "directs-user:complexity_3_userset_ttu_userset_1#direct"},
				Expectation: true,
			},
			{
				Name:        "invalid_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "or_compute_complex3", User: "user:complexity_3_userset_ttu_userset_invalid"},
				Expectation: false,
			},
			{
				Name:        "unconnected_direct_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "or_compute_complex3", User: "user:complexity_3_userset_ttu_userset_unconnected_direct_user"},
				Expectation: false,
			},
			{
				Name:        "unconnected_userset",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "or_compute_complex3", User: "user:complexity_3_userset_ttu_userset_unconnected_userset"},
				Expectation: false,
			},
			{
				Name:        "unconnected_ttu",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_1", Relation: "or_compute_complex3", User: "user:complexity_3_userset_ttu_userset_unconnected_ttu"},
				Expectation: false,
			},
			{
				Name:        "invalid_object",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_invalid_object", Relation: "or_compute_complex3", User: "user:complexity_3_userset_ttu_userset_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_ttu",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_complexity_unconnected_ttu", Relation: "or_compute_complex3", User: "user:complexity_3_userset_ttu_userset_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_usersets_empty",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_complexity_unconnected_usersets", Relation: "or_compute_complex3", User: "user:complexity_3_userset_ttu_userset_1"},
				Expectation: false,
			},
			{
				Name:        "complexity_unconnected_usersets_other",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_userset_ttu_userset_complexity_unconnected_usersets_2", Relation: "or_compute_complex3", User: "user:complexity_3_userset_ttu_userset_1"},
				Expectation: false,
			},
		},
	},
	{
		Name: "and_nested_complex3",
		Tuples: []*openfgav1.TupleKey{
			{Object: "complexity3:complexity_3_and_nested_complex3_1", Relation: "userset_parent", User: "usersets-user:complexity_3_and_nested_complex3_1"},
			{Object: "usersets-user:complexity_3_and_nested_complex3_1", Relation: "ttu_direct_userset", User: "ttus:complexity_3_and_nested_complex3_1#direct_pa_direct_ch"},
			{Object: "ttus:complexity_3_and_nested_complex3_1", Relation: "mult_parent_types", User: "directs-user:complexity_3_and_nested_complex3_1"},
			{Object: "directs-user:complexity_3_and_nested_complex3_1", Relation: "direct", User: "user:complexity_3_and_nested_complex3_1"},
			{Object: "complexity3:complexity_3_and_nested_complex3_1", Relation: "and_nested_complex3", User: "ttus:complexity_3_and_nested_complex3_1#and_ttu"},
			{Object: "ttus:complexity_3_and_nested_complex3_1", Relation: "direct_parent", User: "directs-user:complexity_3_and_nested_complex3_1"},
			// Missing first part of and assignment
			{Object: "complexity3:complexity_3_and_nested_complex3_miss_first_child", Relation: "userset_parent", User: "usersets-user:complexity_3_and_nested_complex3_miss_first_child"},
			{Object: "usersets-user:complexity_3_and_nested_complex3_miss_first_child", Relation: "ttu_direct_userset", User: "ttus:complexity_3_and_nested_complex3_miss_first_child#direct_pa_direct_ch"},
			{Object: "ttus:complexity_3_and_nested_complex3_miss_first_child", Relation: "mult_parent_types", User: "directs-user:complexity_3_and_nested_complex3_miss_first_child"},
			{Object: "directs-user:complexity_3_and_nested_complex3_miss_first_child", Relation: "direct", User: "user:complexity_3_and_nested_complex3_miss_first_child"},
			// Missing first part of and assignment - ttus direct parent not set
			{Object: "complexity3:complexity_3_and_nested_complex3_miss_first_child_ttu", Relation: "userset_parent", User: "usersets-user:complexity_3_and_nested_complex3_miss_first_child_ttu"},
			{Object: "usersets-user:complexity_3_and_nested_complex3_miss_first_child_ttu", Relation: "ttu_direct_userset", User: "ttus:complexity_3_and_nested_complex3_miss_first_child_ttu#direct_pa_direct_ch"},
			{Object: "ttus:complexity_3_and_nested_complex3_miss_first_child_ttu", Relation: "mult_parent_types", User: "directs-user:complexity_3_and_nested_complex3_miss_first_child_ttu"},
			{Object: "directs-user:complexity_3_and_nested_complex3_miss_first_child_ttu", Relation: "direct", User: "user:complexity_3_and_nested_complex3_miss_first_child_ttu"},
			{Object: "complexity3:complexity_3_and_nested_complex3_miss_first_child_ttu", Relation: "and_nested_complex3", User: "ttus:complexity_3_and_nested_complex3_miss_first_child_ttu#and_ttu"},
			// Missing second part of and assignment
			{Object: "usersets-user:complexity_3_and_nested_complex3_miss_second_child", Relation: "ttu_direct_userset", User: "ttus:complexity_3_and_nested_complex3_miss_second_child#direct_pa_direct_ch"},
			{Object: "ttus:complexity_3_and_nested_complex3_miss_second_child", Relation: "mult_parent_types", User: "directs-user:complexity_3_and_nested_complex3_miss_second_child"},
			{Object: "directs-user:complexity_3_and_nested_complex3_miss_second_child", Relation: "direct", User: "user:complexity_3_and_nested_complex3_miss_second_child"},
			{Object: "complexity3:complexity_3_and_nested_complex3_miss_second_child", Relation: "and_nested_complex3", User: "ttus:complexity_3_and_nested_complex3_miss_second_child#and_ttu"},
			{Object: "ttus:complexity_3_and_nested_complex3_miss_second_child", Relation: "direct_parent", User: "directs-user:complexity_3_and_nested_complex3_miss_second_child"},
		},
		CheckAssertions: []*checktest.Assertion{
			{
				Name:        "valid_user",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_and_nested_complex3_1", Relation: "and_nested_complex3", User: "user:complexity_3_and_nested_complex3_1"},
				Expectation: true,
			},
			{
				Name:        "miss_first_part_and",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_and_nested_complex3_miss_first_child", Relation: "and_nested_complex3", User: "user:complexity_3_and_nested_complex3_1"},
				Expectation: false,
			},
			{
				Name:        "miss_first_part_and_ttu_not_set",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_and_nested_complex3_miss_first_child_ttu", Relation: "and_nested_complex3", User: "user:complexity_3_and_nested_complex3_1"},
				Expectation: false,
			},
			{
				Name:        "miss_second_part_and",
				Tuple:       &openfgav1.TupleKey{Object: "complexity3:complexity_3_and_nested_complex3_miss_second_child", Relation: "and_nested_complex3", User: "user:complexity_3_and_nested_complex3_1"},
				Expectation: false,
			},
		},
	},
}
