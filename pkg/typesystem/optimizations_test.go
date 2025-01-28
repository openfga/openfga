package typesystem

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/testutils"
)

func TestClassifyRelationWithRecursiveTTUAndAlgebraicOps(t *testing.T) {
	tests := map[string]struct {
		model                string
		objectType           string
		relation             string
		userType             string
		expected             bool
		assertOnResultingMap func(t *testing.T, resultMap Operands)
	}{
		`recursive_ttu_or_computed_weight_one_1`: {
			model: `
model
  schema 1.1
type user
type document
  relations
    define rel1: rel2 or rel1 from parent 
    define parent: [document]
    define rel2: [user] and rel3
    define rel3: rel4 but not rel5
    define rel4: [user]
    define rel5: [user]
`,
			objectType: "document",
			relation:   "rel1",
			userType:   "user",
			expected:   true,
			assertOnResultingMap: func(t *testing.T, resultMap Operands) {
				require.Len(t, resultMap, 1)
				require.Contains(t, resultMap, "rel2")
			},
		},
		`recursive_ttu_or_computed_weight_one_2`: {
			model: `
model
  schema 1.1
type user
type document
  relations
    define rel1: [user] or rel2 or rel1 from parent 
    define parent: [document]
    define rel2: [user] and rel3
    define rel3: rel4 but not rel5
    define rel4: [user]
    define rel5: [user]
`,
			objectType: "document",
			relation:   "rel1",
			userType:   "user",
			expected:   true,
			assertOnResultingMap: func(t *testing.T, resultMap Operands) {
				require.Len(t, resultMap, 2)
				require.Contains(t, resultMap, "rel2")
				require.Contains(t, resultMap, "rel1")
			},
		},
		`recursive_ttu_or_computed_weight_one_3`: {
			model: `
model
  schema 1.1
type user
type document
  relations
    define rel1: rel2 or rel6 or rel1 from parent
    define parent: [document]
    define rel2: [user] and rel3
    define rel3: rel4 but not rel5
    define rel4: [user]
    define rel5: [user]
    define rel6: [user]
`,
			objectType: "document",
			relation:   "rel1",
			userType:   "user",
			expected:   true,
			assertOnResultingMap: func(t *testing.T, resultMap Operands) {
				require.Len(t, resultMap, 2)
				require.Contains(t, resultMap, "rel2")
				require.Contains(t, resultMap, "rel6")
			},
		},
		`recursive_ttu_or_computed_weight_one_infinity`: {
			model: `
model
  schema 1.1
type user
type document
  relations
    define rel1: rel2 or rel1 from parent 
    define noapplyrel: rel1 or noapplyrel from parent
    define parent: [document]
    define rel2: [user] and rel3
    define rel3: rel4 but not rel5
    define rel4: [user]
    define rel5: [user]
`,
			objectType: "document",
			relation:   "noapplyrel",
			userType:   "user",
			expected:   false,
		},
		`recursive_ttu_or_terminal_type`: {
			model: `
		model
            schema 1.1
          type user
          type folder
            relations
              define parent: [folder]
              define viewer: [user] or viewer from parent`,
			objectType: "folder",
			relation:   "viewer",
			userType:   "user",
			expected:   true,
			assertOnResultingMap: func(t *testing.T, resultMap Operands) {
				require.Len(t, resultMap, 1)
				require.Contains(t, resultMap, "viewer")
			},
		},
		`recursive_ttu_or_wildcard`: {
			model: `
		model
            schema 1.1
          type user
          type folder
            relations
              define parent: [folder]
              define viewer: [user, user:*] or viewer from parent`,
			objectType: "folder",
			relation:   "viewer",
			userType:   "user",
			expected:   true,
			assertOnResultingMap: func(t *testing.T, resultMap Operands) {
				require.Len(t, resultMap, 1)
				require.Contains(t, resultMap, "viewer")
			},
		},
		`complex_ttu_multiple_parent_types`: {
			model: `
model
	schema 1.1
type user
type employee
type team
	relations
		define parent: [team]
		define member: [user]
type group
	relations
		define parent: [group, team]
		define member: [user] or member from parent
`,
			objectType: "group",
			relation:   "member",
			userType:   "user",
			expected:   false,
		},
		`complex_ttu_directly_other_assigned_userset_1`: {
			model: `
model
	schema 1.1
type user
type group
	relations
		define parent: [group]
		define otherRelation: [user]
		define member: [user, group#otherRelation] or member from parent
`,
			objectType: "group",
			relation:   "member",
			userType:   "user",
			expected:   false,
		},
		`complex_ttu_directly_other_assigned_userset_2`: {
			model: `
model
	schema 1.1
type user
type group
	relations
		define parent: [group]
		define member: [user, group#member] or member from parent
`,
			objectType: "group",
			relation:   "member",
			userType:   "user",
			expected:   false,
		},
		`complex_non_recursive_userset_from_directly_assignable`: {
			model: `
model
	schema 1.1
type user
type group
	relations
		define parent: [group]
		define owner: [user]
		define member: [user] or owner from parent
`,
			objectType: "group",
			relation:   "member",
			userType:   "user",
			expected:   false,
		},
		`complex_non_recursive_userset_from_computed`: {
			model: `
model
	schema 1.1
type user
type group
	relations
		define parent: [group]
		define owner: [user]
		define other_owner: owner
		define member: [user] or other_owner from parent
`,
			objectType: "group",
			relation:   "member",
			userType:   "user",
			expected:   false,
		},
		`recursive_userset`: {
			model: `
model
  schema 1.1
type user
type document
  relations
    define rel1: [user, document#rel1]
`,
			objectType: "document",
			relation:   "rel1",
			userType:   "user",
			expected:   false,
		},
		`nested_wildcard`: {
			model: `
model
  schema 1.1
type user
type document
  relations
    define rel1: rel2 or rel1 from parent
    define parent: [document]
    define rel2: [user:*] and rel3
    define rel3: rel4 but not rel5
    define rel4: [user:*]
    define rel5: [user]
`,
			objectType: "document",
			relation:   "rel1",
			userType:   "user",
			expected:   true,
			assertOnResultingMap: func(t *testing.T, resultMap Operands) {
				require.Len(t, resultMap, 1)
				require.Contains(t, resultMap, "rel2")
			},
		},
		`two_ttus`: {
			model: `
model
  schema 1.1
type user
type document
  relations
    define rel1: (rel2 from parent) or rel1 from parent
    define parent: [document]
    define rel2: [user] and rel3
    define rel3: rel4 but not rel5
    define rel4: [user]
    define rel5: [user]
`,
			objectType: "document",
			relation:   "rel1",
			userType:   "user",
			expected:   false,
		},
		`ttu_or_intersection`: {
			model: `
model
  schema 1.1
type user
type document
  relations
    define rel1: (rel2 and rel3) or rel1 from parent
    define parent: [document]
    define rel2: [user]
    define rel3: [user]
`,
			objectType: "document",
			relation:   "rel1",
			userType:   "user",
			expected:   true,
			assertOnResultingMap: func(t *testing.T, resultMap Operands) {
				require.Len(t, resultMap, 1)
				require.Contains(t, resultMap, "rel1")
			},
		},
		`ttu_or_intersection_that_is_weight_2`: {
			model: `
model
  schema 1.1
type user
type document
  relations
    define rel1: (rel2 and rel3) or rel1 from parent
    define parent: [document]
    define rel2: rel3 from parent
    define rel3: [user]
`,
			objectType: "document",
			relation:   "rel1",
			userType:   "user",
			expected:   false,
		},
	}
	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(test.model)
			typesys, err := NewAndValidate(context.Background(), model)
			require.NoError(t, err)
			resultMap, is := IsRelationWithRecursiveTTUAndAlgebraicOperations(typesys, test.objectType, test.relation, test.userType)
			require.Equal(t, test.expected, is)
			if test.expected {
				test.assertOnResultingMap(t, resultMap)
			} else {
				require.Empty(t, resultMap)
			}
		})
	}
}

func TestIsRelationWithRecursiveUsersetAndAlgebraicOperations(t *testing.T) {
	tests := map[string]struct {
		model                string
		objectType           string
		relation             string
		userType             string
		expected             bool
		assertOnResultingMap func(t *testing.T, resultMap Operands)
	}{
		`recursive_userset_or_computed_weight_one_1`: {
			model: `
				model
				  schema 1.1
				type user
				type document
					relations
						define rel1: [document#rel1] or rel2
						define rel2: rel3 and rel4
						define rel3: [user]
						define rel4: [user]
`,
			objectType: "document",
			relation:   "rel1",
			userType:   "user",
			expected:   true,
			assertOnResultingMap: func(t *testing.T, resultMap Operands) {
				require.Len(t, resultMap, 1)
				require.Contains(t, resultMap, "rel2")
			},
		},
	}
	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(test.model)
			typesys, err := NewAndValidate(context.Background(), model)
			require.NoError(t, err)
			resultMap, is := IsRelationWithRecursiveUsersetAndAlgebraicOperations(typesys, test.objectType, test.relation, test.userType)
			require.Equal(t, test.expected, is)
			if test.expected {
				test.assertOnResultingMap(t, resultMap)
			} else {
				require.Empty(t, resultMap)
			}
		})
	}
}
