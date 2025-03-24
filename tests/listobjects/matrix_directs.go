package listobjects

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	listobjectstest "github.com/openfga/openfga/internal/test/listobjects"
)

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
			{Object: "directs:wildcard_and_condition_3", Relation: "direct_comb", User: "user:direct_comb_1", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			{Object: "directs:wildcard_and_condition_4", Relation: "direct_comb", User: "user:*", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:direct_comb_1",
					Type:     "directs",
					Relation: "direct_comb",
				},
				Context: MustNewStruct(map[string]any{
					"x": "1", // passes condition
				}),
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
				Context: MustNewStruct(map[string]any{
					"x": "9", // fails condition
				}),
				Expectation: []string{
					"directs:wildcard_and_condition_1",
					"directs:wildcard_and_condition_2",
				},
			},
		},
	},
}
