package typesystem

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/testutils"
)

func TestClassifyRelationWithRecursiveTTUAndAlgebraicOps(t *testing.T) {
	tests := map[string]struct {
		model      string
		objectType string
		relation   string
		userType   string
		expected   bool
	}{
		`recursive_ttu_or_computed_weight_one_1`: {
			model: `
model
  schema 1.1
type user
type document
  relations
    define rel1a: rel2 or rel1a from parent 
    define rel1b: [user] or rel2 or rel1b from parent 
    define rel1c: rel2 or rel6 or rel1c from parent
    define noapplyrel: rel1a or noapplyrel from parent
    define parent: [document]
    define rel2: [user] and rel3
    define rel3: rel4 but not rel5
    define rel4: [user]
    define rel5: [user]
    define rel6: [user]
`,
			objectType: "document",
			relation:   "rel1a",
			userType:   "user",
			expected:   true,
		},
		`recursive_ttu_or_computed_weight_one_2`: {
			model: `
model
  schema 1.1
type user
type document
  relations
    define rel1a: rel2 or rel1a from parent 
    define rel1b: [user] or rel2 or rel1b from parent 
    define rel1c: rel2 or rel6 or rel1c from parent
    define noapplyrel: rel1a or noapplyrel from parent
    define parent: [document]
    define rel2: [user] and rel3
    define rel3: rel4 but not rel5
    define rel4: [user]
    define rel5: [user]
    define rel6: [user]
`,
			objectType: "document",
			relation:   "rel1b",
			userType:   "user",
			expected:   true,
		},
		`recursive_ttu_or_computed_weight_one_3`: {
			model: `
model
  schema 1.1
type user
type document
  relations
    define rel1a: rel2 or rel1a from parent 
    define rel1b: [user] or rel2 or rel1b from parent 
    define rel1c: rel2 or rel6 or rel1c from parent
    define noapplyrel: rel1a or noapplyrel from parent
    define parent: [document]
    define rel2: [user] and rel3
    define rel3: rel4 but not rel5
    define rel4: [user]
    define rel5: [user]
    define rel6: [user]
`,
			objectType: "document",
			relation:   "rel1c",
			userType:   "user",
			expected:   true,
		},
		`recursive_ttu_or_computed_weight_one_infinity`: {
			model: `
model
  schema 1.1
type user
type document
  relations
    define rel1a: rel2 or rel1a from parent 
    define rel1b: [user] or rel2 or rel1b from parent 
    define rel1c: rel2 or rel6 or rel1c from parent
    define noapplyrel: rel1a or noapplyrel from parent
    define parent: [document]
    define rel2: [user] and rel3
    define rel3: rel4 but not rel5
    define rel4: [user]
    define rel5: [user]
    define rel6: [user]
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
		`wildcard`: {
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
	}
	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			fmt.Println(testName)
			model := testutils.MustTransformDSLToProtoWithID(test.model)
			typesys, err := NewAndValidate(context.Background(), model)
			require.NoError(t, err)
			result := typesys.IsRelationWithRecursiveTTUAndAlgebraicOperations(test.objectType, test.relation, test.userType)
			require.Equal(t, test.expected, result)
		})
	}
}
