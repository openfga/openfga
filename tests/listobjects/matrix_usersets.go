package listobjects

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	listobjectstest "github.com/openfga/openfga/internal/test/listobjects"
)

var usersets = []matrixTest{
	{
		Name: "usersets_user_alg_combined",
		Tuples: []*openfgav1.TupleKey{
			{Object: "usersets-user:user_alg_1", Relation: "userset_alg_combined", User: "directs:user_alg_1#alg_combined"},
			{Object: "usersets-user:user_alg_2", Relation: "userset_alg_combined", User: "directs:user_alg_2#alg_combined"},

			{Object: "usersets-user:user_alg_3", Relation: "userset_alg_combined", User: "directs-employee:user_alg_1#alg_combined"},
			{Object: "usersets-user:user_alg_4", Relation: "userset_alg_combined", User: "directs-employee:user_alg_2#alg_combined"},

			// This satisfies directs#alg_combined
			{Object: "directs:user_alg_1", Relation: "direct_mult_types", User: "user:user_alg_1"},
			{Object: "directs:user_alg_1", Relation: "other_rel", User: "user:user_alg_1"},
			// This does not
			{Object: "directs:user_alg_2", Relation: "other_rel", User: "user:user_alg_1"},

			// This satisfies directs-employee#alg_combined
			{Object: "directs-employee:user_alg_1", Relation: "direct", User: "employee:user_alg_1"},
			{Object: "directs-employee:user_alg_1", Relation: "direct_wild", User: "employee:*"},
			// This does not
			{Object: "directs-employee:user_alg_2", Relation: "other_rel", User: "employee:user_alg_1"},
		},
		ListObjectAssertions: []*listobjectstest.Assertion{
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "directs:user_alg_1#alg_combined",
					Type:     "usersets-user",
					Relation: "userset_alg_combined",
				},
				Expectation: []string{"usersets-user:user_alg_1"},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "user:user_alg_1",
					Type:     "usersets-user",
					Relation: "userset_alg_combined",
				},

				Expectation: []string{"usersets-user:user_alg_1"},
			},
			{
				Request: &openfgav1.ListObjectsRequest{
					User:     "employee:user_alg_1",
					Type:     "usersets-user",
					Relation: "userset_alg_combined",
				},

				Expectation: []string{"usersets-user:user_alg_3"},
			},
		},
	},
}
