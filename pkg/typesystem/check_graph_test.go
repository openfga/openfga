package typesystem

import (
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	language "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetDOTRepresentation also tests that the graph is built correctly.
func TestGetDOTRepresentation(t *testing.T) {
	testCases := map[string]struct {
		model          string
		expectedOutput string
	}{
		`with_union`: { // https://github.com/openfga/openfga/blob/main/docs/list_objects/example/example.md
			model: `
				model
					schema 1.1
				type user
				
				type group
				  relations
					define member: [user, group#member]
				
				type folder
				  relations
					define viewer: [user]
				
				type document
				  relations
					define parent: [folder]
					define editor: [user]
					define viewer: [user, user:*, group#member] or editor or viewer from parent`,
			expectedOutput: `digraph {
graph [
rankdir=BT
];

// Node definitions.
0 [label=document];
1 [label="document:*"];
2 [label="document#editor"];
3 [label=user];
4 [label="document#parent"];
5 [label=folder];
6 [label="document#viewer"];
7 [label="user:*"];
8 [label="group#member"];
9 [label="folder#viewer"];
10 [label="folder:*"];
11 [label=group];
12 [label="group:*"];

// Edge definitions.
2 -> 6 [
label=6
style=dashed
];
3 -> 2 [label=1];
3 -> 6 [label=3];
3 -> 8 [label=9];
3 -> 9 [label=8];
5 -> 4 [label=2];
7 -> 6 [label=4];
8 -> 6 [label=5];
8 -> 8 [label=10];
9 -> 6 [
label=7
headlabel="(document#parent)"
];
}`,
		},
		`with_intersection`: { // https://github.com/openfga/openfga/blob/main/docs/list_objects/example_with_intersection_or_exclusion/example.md
			model: `
				model
					schema 1.1
				type user
				type document
				   relations
					 define a: [user]
					 define b: [user]
					 define c: a and b`,
			expectedOutput: `digraph {
graph [
rankdir=BT
];

// Node definitions.
0 [label=document];
1 [label="document:*"];
2 [label="document#a"];
3 [label=user];
4 [label="document#b"];
5 [label="document#c"];
6 [label="user:*"];

// Edge definitions.
2 -> 5 [
label=3
style=dashed
];
3 -> 2 [label=1];
3 -> 4 [label=2];
4 -> 5 [
label=4
style=dashed
];
}`,
		},
		`with_exclusion`: { // https://github.com/openfga/openfga/blob/main/docs/list_objects/example_with_intersection_or_exclusion/example.md
			model: `
				model
					schema 1.1
				type user
				type document
				   relations
					 define a: [user]
					 define b: [user]
					 define c: a but not b`,
			expectedOutput: `digraph {
graph [
rankdir=BT
];

// Node definitions.
0 [label=document];
1 [label="document:*"];
2 [label="document#a"];
3 [label=user];
4 [label="document#b"];
5 [label="document#c"];
6 [label="user:*"];

// Edge definitions.
2 -> 5 [
label=3
style=dashed
];
3 -> 2 [label=1];
3 -> 4 [label=2];
4 -> 5 [
label=4
style=dashed
];
}`,
		},
		`with_conditions_on_type`: {
			model: `
			model
				schema 1.1
			
			type user
			
			type document
				relations
					define admin: [user with condition1]
					define writer: [user with condition2]
					define viewer: [user:* with condition3]
			
			condition condition1(x: int) {
				x < 100
			}
			
			condition condition2(x: int) {
				x < 100
			}
			
			condition condition3(x: int) {
				x < 100
			}`,
			expectedOutput: `digraph {
graph [
rankdir=BT
];

// Node definitions.
0 [label=document];
1 [label="document:*"];
2 [label="document#admin"];
3 [label=user];
4 [label="document#viewer"];
5 [label="user:*"];
6 [label="document#writer"];

// Edge definitions.
3 -> 2 [label=1];
3 -> 4 [label=3];
3 -> 6 [label=4];
5 -> 4 [label=2];
}`,
		},
		`with_conditions_on_userset`: {
			model: `
			model
				schema 1.1
			
			type group
				relations
					define member: [user]
			
			type document
				relations
					define admin: [group#member with condition1]
			
			condition condition1(x: int) {
				x < 100
			}`,
			expectedOutput: `digraph {
graph [
rankdir=BT
];

// Node definitions.
0 [label=document];
1 [label="document:*"];
2 [label="document#admin"];
3 [label="group#member"];
4 [label=group];
5 [label="group:*"];
6 [label=user];

// Edge definitions.
3 -> 2 [label=1];
6 -> 3 [label=2];
}`,
		},
		`with_wildcard`: {
			model: `
			model
				schema 1.1
			
			type user
			
			type document
				relations
					define admin: [user:*]`,
			expectedOutput: `digraph {
graph [
rankdir=BT
];

// Node definitions.
0 [label=document];
1 [label="document:*"];
2 [label="document#admin"];
3 [label="user:*"];
4 [label=user];

// Edge definitions.
3 -> 2 [label=1];
4 -> 2 [label=2];
}`,
		},
		`with_ttu`: {
			model: `
				model
				  schema 1.1
				
				type user
				
				type folder
				  relations
					define viewer: [user]
				
				type document
				  relations
					define parent: [folder]
					define viewer: viewer from parent`,
			expectedOutput: `digraph {
graph [
rankdir=BT
];

// Node definitions.
0 [label=document];
1 [label="document:*"];
2 [label="document#parent"];
3 [label=folder];
4 [label="document#viewer"];
5 [label="folder#viewer"];
6 [label="folder:*"];
7 [label=user];
8 [label="user:*"];

// Edge definitions.
3 -> 2 [label=1];
5 -> 4 [
label=2
headlabel="(document#parent)"
];
7 -> 5 [label=3];
}`,
		},
		`multiple_edges_between_same_nodes`: {
			model: `
				model
				  schema 1.1
				
				type user
				
				type state
				  relations
					define can_view: [user]
				
				type transition
				  relations
					define start: [state]
					define end: [state]
					define can_apply: [user] and can_view from start and can_view from end`,
			expectedOutput: `digraph {
graph [
rankdir=BT
];

// Node definitions.
0 [label=state];
1 [label="state:*"];
2 [label="state#can_view"];
3 [label=user];
4 [label=transition];
5 [label="transition:*"];
6 [label="transition#can_apply"];
7 [label="transition#end"];
8 [label="transition#start"];
9 [label="user:*"];

// Edge definitions.
0 -> 7 [label=5];
0 -> 8 [label=6];
2 -> 6 [
label=3
headlabel="(transition#start)"
];
2 -> 6 [
label=4
headlabel="(transition#end)"
];
3 -> 2 [label=1];
3 -> 6 [label=2];
}`,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			model := language.MustTransformDSLToProto(test.model)
			ts := New(model)
			g, err := NewDotEncodedGraph(ts)
			require.NoError(t, err)

			actualDOT := g.GetDOT()
			actualSorted := getSorted(actualDOT)
			expectedSorted := getSorted(test.expectedOutput)
			diff := cmp.Diff(expectedSorted, actualSorted)

			require.Empty(t, diff, "expected %s, got %s", test.expectedOutput, actualDOT)
		})
	}
}

func TestGetConnected(t *testing.T) {
	testCases := map[string]struct {
		model           string
		source          string
		target          string
		expectErr       bool
		expectConnected bool
	}{
		`are_connected`: {
			model: `
				model
					schema 1.1
				type user
				type group
				type resource
					relations
						define a: [user]`,
			source:          "user",
			target:          "resource#a",
			expectConnected: true,
		},
		`are_not_connected`: {
			model: `
				model
					schema 1.1
				type user
				type group
				type resource
					relations
						define a: [user]`,
			source:          "user",
			target:          "group",
			expectConnected: false,
		},
		`error`: {
			model: `
				model
					schema 1.1
				type user
				type group
				type resource
					relations
						define a: [user]`,
			source:    "resource#a",
			target:    "unknown",
			expectErr: true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			model := language.MustTransformDSLToProto(test.model)
			ts := New(model)
			g, err := NewDotEncodedGraph(ts)
			require.NoError(t, err)

			connected, err := g.AreConnected(test.source, test.target)
			if test.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectConnected, connected)
			}
		})
	}
}

// getSorted assumes the input has multiple lines and returns the sorted version of it.
func getSorted(input string) string {
	lines := strings.FieldsFunc(input, func(r rune) bool {
		return r == '\n'
	})

	sort.Strings(lines)

	return strings.Join(lines, "\n")
}
