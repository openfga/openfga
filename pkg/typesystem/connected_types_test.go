package typesystem

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/testutils"
)

func TestTypesystemAreTypesConnectedViaRelation(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type employee

		type document
			relations
				define computed: computed2
				define computed2: direct_user
				define direct_user: [user]
				define direct_employee: [employee]
				define direct_user_or_computed: [user] or direct_user
				define direct_user_and_employee: [ user, employee ]
				define union_user_or_employee: direct_employee or direct_user
				define intersection_user_and_employee: direct_user and direct_employee
				define intersection_direct_and_computed_user: [user] and direct_user
	`

	tests := []struct {
		expectedAreConnected      bool
		expectedTerminalRelations []string
		objectType                string
		relation                  string
		subjectType               string
	}{
		// // Intersection
		{
			objectType:                "document",
			relation:                  "intersection_user_and_employee",
			subjectType:               "employee",
			expectedAreConnected:      false,
			expectedTerminalRelations: []string{},
		},
		{
			objectType:                "document",
			relation:                  "intersection_user_and_employee",
			subjectType:               "user",
			expectedAreConnected:      false,
			expectedTerminalRelations: []string{},
		},

		// // Union
		{
			objectType:                "document",
			relation:                  "union_user_or_employee",
			subjectType:               "employee",
			expectedAreConnected:      true,
			expectedTerminalRelations: []string{"direct_employee"},
		},
		{
			objectType:                "document",
			relation:                  "union_user_or_employee",
			subjectType:               "user",
			expectedAreConnected:      true,
			expectedTerminalRelations: []string{"direct_user"},
		},
		{
			objectType:                "document",
			relation:                  "direct_user_or_computed",
			subjectType:               "user",
			expectedAreConnected:      true,
			expectedTerminalRelations: []string{"direct_user_or_computed", "direct_user"},
		},
		{
			objectType:                "document",
			relation:                  "direct_user_or_computed",
			subjectType:               "user",
			expectedAreConnected:      true,
			expectedTerminalRelations: []string{"direct_user", "direct_user_or_computed"},
		},

		// Impossible cases
		{
			objectType:                "document",
			relation:                  "computed2",
			subjectType:               "employee",
			expectedAreConnected:      false,
			expectedTerminalRelations: []string{},
		},
		{
			objectType:                "document",
			relation:                  "computed",
			subjectType:               "employee",
			expectedAreConnected:      false,
			expectedTerminalRelations: []string{},
		},
		{
			objectType:                "document",
			relation:                  "direct_user",
			subjectType:               "employee",
			expectedAreConnected:      false,
			expectedTerminalRelations: []string{},
		},

		// Direct relation
		{
			objectType:                "document",
			relation:                  "direct_user",
			subjectType:               "user",
			expectedAreConnected:      true,
			expectedTerminalRelations: []string{"direct_user"},
		},
		{
			objectType:                "document",
			relation:                  "direct_user",
			subjectType:               "user",
			expectedAreConnected:      true,
			expectedTerminalRelations: []string{"direct_user"},
		},
		{
			objectType:                "document",
			relation:                  "direct_user_and_employee",
			subjectType:               "user",
			expectedAreConnected:      true,
			expectedTerminalRelations: []string{"direct_user_and_employee"},
		},
		{
			objectType:                "document",
			relation:                  "direct_user_and_employee",
			subjectType:               "employee",
			expectedAreConnected:      true,
			expectedTerminalRelations: []string{"direct_user_and_employee"},
		},

		// Computed relation case
		{
			objectType:                "document",
			relation:                  "computed",
			subjectType:               "user",
			expectedAreConnected:      true,
			expectedTerminalRelations: []string{"direct_user"},
		},
		{
			objectType:                "document",
			relation:                  "computed2",
			subjectType:               "user",
			expectedAreConnected:      true,
			expectedTerminalRelations: []string{"direct_user"},
		},
	}

	ts, err := NewAndValidate(context.Background(), testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)
	for i, test := range tests {
		t.Run(fmt.Sprintf("stage-%d", i), func(t *testing.T) {
			actualAreConnected, actualTerminalRelations := ts.AreTypesConnectedViaRelations(test.objectType, test.relation, test.subjectType)
			require.Equal(t, test.expectedAreConnected, actualAreConnected)
			require.ElementsMatch(t, test.expectedTerminalRelations, actualTerminalRelations)
		})
	}
}
