package planner

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/adaptertest"
)

// buildSQL renders a query with a render-only builder and returns its SQL and bind args.
func buildSQL(t *testing.T, q adapter.Query) (string, []any) {
	t.Helper()
	bq, ok := q.(interface{ Build() (string, []any) })
	require.True(t, ok, "query %T does not expose Build", q)
	return bq.Build()
}

// leaf is a small helper for a weight-1 QueryNode in build tests.
func leaf(relation, subjectID, subjectRelation string, conditions ...string) *QueryNode {
	return &QueryNode{
		Relation:        relation,
		SubjectID:       subjectID,
		SubjectRelation: subjectRelation,
		Conditions:      conditions,
		Weight:          1,
		Label:           "document#" + relation,
	}
}

var testBound = bound{
	store:           "store1",
	objectType:      "document",
	objectID:        "1",
	subjectType:     "user",
	subjectID:       "alice",
	subjectRelation: "",
}

func TestBuildHavingQuery_Union(t *testing.T) {
	root := &CombineNode{Op: CombineUnion, Children: []Node{
		leaf("viewer", "alice", "", ""),
		leaf("editor", "alice", "", ""),
	}}
	sql, args := buildSQL(t, buildHavingQuery(adaptertest.New(nil), testBound, root))

	// The shared WHERE binds the store, object, subject type/relation, then the subject id
	// superset (exact id + wildcard), then the relation filter pruning the scan to the
	// referenced relations (sorted: editor, viewer). Each leaf then contributes a
	// COUNT(CASE WHEN <match> THEN 1 END) atom whose args are: the relation, the subject id
	// pair, the unconditioned sentinel, the CASE THEN value (1), and the >= 1 threshold.
	require.Equal(t, []any{
		1, "store1", "document", "1", "user", "", "alice", "*", // SELECT 1 + shared WHERE
		"editor", "viewer", // relation filter (sorted)
		"viewer", "alice", "*", "", 1, 1, // viewer atom
		"editor", "alice", "*", "", 1, 1, // editor atom
	}, args)

	require.True(t, strings.HasPrefix(sql, "SELECT ? FROM tuple t WHERE "), "unexpected prefix: %s", sql)
	require.Contains(t, sql, "HAVING (COUNT(CASE WHEN ", "expected HAVING over COUNT(CASE WHEN ...)")
	require.Contains(t, sql, " THEN ? END) >= ? OR COUNT(CASE WHEN ", "union combines atoms with OR")
	require.Contains(t, sql, "(t.condition_name IS NULL OR t.condition_name = ?)", "condition-free atom restricts to unconditioned tuples")
}

func TestBuildHavingQuery_Intersection(t *testing.T) {
	root := &CombineNode{Op: CombineIntersect, Children: []Node{
		leaf("viewer", "alice", "", ""),
		leaf("editor", "alice", "", ""),
	}}
	sql, _ := buildSQL(t, buildHavingQuery(adaptertest.New(nil), testBound, root))
	require.Contains(t, sql, " THEN ? END) >= ? AND COUNT(CASE WHEN ", "intersection combines atoms with AND")
}

func TestBuildHavingQuery_Exclusion(t *testing.T) {
	root := &CombineNode{Op: CombineExcept, Children: []Node{
		leaf("viewer", "alice", "", ""),
		leaf("banned", "alice", "", ""),
	}}
	sql, _ := buildSQL(t, buildHavingQuery(adaptertest.New(nil), testBound, root))
	require.Contains(t, sql, " THEN ? END) >= ? AND NOT (COUNT(CASE WHEN ", "exclusion subtracts with AND NOT")
}

func TestBuildHavingQuery_Nested(t *testing.T) {
	// (viewer OR editor) AND NOT banned
	root := &CombineNode{Op: CombineExcept, Children: []Node{
		&CombineNode{Op: CombineUnion, Children: []Node{
			leaf("viewer", "alice", "", ""),
			leaf("editor", "alice", "", ""),
		}},
		leaf("banned", "alice", "", ""),
	}}
	sql, _ := buildSQL(t, buildHavingQuery(adaptertest.New(nil), testBound, root))
	// The union is parenthesized as the base, then subtracted with AND NOT.
	require.Contains(t, sql, "HAVING ((COUNT(CASE WHEN ", "nested union is parenthesized as the base")
	require.Contains(t, sql, " THEN ? END) >= ? OR COUNT(CASE WHEN ", "inner union uses OR")
	require.Contains(t, sql, " THEN ? END) >= ?) AND NOT (COUNT(CASE WHEN ", "outer exclusion subtracts the union")
}

func TestBuildGatherQuery_WorkedExample(t *testing.T) {
	// super_admin = [user with sudoer] and admin, where admin = [user].
	leaves := []*QueryNode{
		leaf("super_admin", "alice", "", "sudoer"),
		leaf("admin", "alice", "", ""),
	}
	sql, args := buildSQL(t, buildGatherQuery(adaptertest.New(nil), testBound, leaves))

	// Projection pulls relation, subject id, and the condition columns the executor needs.
	require.True(t, strings.HasPrefix(sql, "SELECT t.relation, "), "gather must project the relation: %s", sql)
	require.Contains(t, sql, "t.condition_name, t.condition_context FROM tuple t WHERE ", "gather projects condition columns")
	// The operand match is an OR of per-leaf (relation = R AND <condition membership>).
	require.Contains(t, sql, "((t.relation = ? AND t.condition_name = ?) OR (t.relation = ? AND (t.condition_name IS NULL OR t.condition_name = ?)))",
		"gather OR-of-operands with condition membership: %s", sql)

	require.Equal(t, []any{
		"store1", "document", "1", "user", "", "alice", "*", // shared WHERE
		"super_admin", "sudoer", // conditioned operand
		"admin", "", // unconditioned operand (NULL-or-'' sentinel arg)
	}, args)
}

func TestBuildGatherQuery_Userset(t *testing.T) {
	// A userset subject (group:eng#member) is never satisfied by the public-access
	// wildcard, so the shared WHERE binds only the exact subject id.
	usersetBound := bound{
		store: "store1", objectType: "document", objectID: "1",
		subjectType: "group", subjectID: "eng", subjectRelation: "member",
	}
	leaves := []*QueryNode{leaf("viewer", "eng", "member", "", "c")}
	sql, args := buildSQL(t, buildGatherQuery(adaptertest.New(nil), usersetBound, leaves))

	require.NotContains(t, sql, " IN (?, ?)", "userset subject must not admit the wildcard id")
	require.Equal(t, []any{
		"store1", "document", "1", "group", "member", "eng", // shared WHERE, no wildcard
		"viewer", "", "c", // operand: relation, NULL-or-'' sentinel, named condition
	}, args)
}
