package planner

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage/adapter/adaptertest"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
)

// These tests pin the exact SQL text and bind arguments the planner emits for each query
// shape. They drive a real adapter.Builder (adaptertest.New) backed by a Recorder, which
// captures the rendered statement and ordinal args when Plan.Execute runs the query — the
// same path a live datastore would take, minus the database. This is the planner's
// contract with the storage layer: a regression in how a plan compiles to SQL surfaces
// here as a string diff rather than as a wrong authorization decision in production.

// recordSQL plans a Check of objectType#viewer over the model and executes it through a
// Recorder-backed builder, returning the SQL and bind args the planner handed the
// executor. The condition evaluator always denies; it only matters that one is supplied so
// a gather plan can run.
func recordSQL(t *testing.T, model, objectType, user string) (string, []any) {
	t.Helper()
	ts, err := typesystem.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)
	g := ts.GetWeightedGraph()
	require.NotNil(t, g)

	rec := &adaptertest.Recorder{}
	p, err := New(adaptertest.New(rec)).Plan(g, "store1", objectType, "1", "viewer", user)
	require.NoError(t, err)

	_, err = p.Execute(context.Background(), evalFunc(func(string, []byte) (bool, error) { return false, nil }))
	require.NoError(t, err)
	return rec.SQL, rec.Parameters
}

// The subject is stored packed as "type:id[#relation]" in the _user column, so the
// standard dialect decodes each logical field with ANSI string functions. These vars
// reproduce those decoded expressions for the table alias "t" the planner always uses, so
// the golden SQL below stays readable; the decoding itself is pinned literally in the ansi
// package's render tests.
var (
	subjType = "SUBSTRING(t._user FROM 1 FOR POSITION(':' IN t._user) - 1)"

	subjIDRest = "SUBSTRING(t._user FROM POSITION(':' IN t._user) + 1)"
	subjID     = "CASE WHEN POSITION('#' IN " + subjIDRest + ") = 0 THEN " + subjIDRest +
		" ELSE SUBSTRING(" + subjIDRest + " FROM 1 FOR POSITION('#' IN " + subjIDRest + ") - 1) END"

	subjRel = "CASE WHEN POSITION('#' IN t._user) = 0 THEN '' ELSE SUBSTRING(t._user" +
		" FROM POSITION('#' IN t._user) + 1) END"
)

// sharedWhere is the WHERE prefix common to both query shapes: the store, bound object,
// bound subject type / relation, and the subject-id superset (exact id plus the
// public-access wildcard). Its bind args, in order, are
// store, object_type, object_id, subject_type, subject_relation, subject_id, "*".
func sharedWhere() string {
	return "t.store = ? AND t.object_type = ? AND t.object_id = ? AND " +
		subjType + " = ? AND " + subjRel + " = ? AND " + subjID + " IN (?, ?)"
}

// relFilter is the relation-pruning predicate the HAVING query adds to the shared WHERE:
// "t.relation = ?" for a single referenced relation, or "t.relation IN (?, ...)" for
// several. Its bind args are the relations, which the planner emits sorted.
func relFilter(n int) string {
	if n == 1 {
		return "t.relation = ?"
	}
	marks := make([]string, n)
	for i := range marks {
		marks[i] = "?"
	}
	return "t.relation IN (" + strings.Join(marks, ", ") + ")"
}

// countAtom is the HAVING atom for one condition-free leaf: COUNT over a searched CASE
// that matches the leaf's relation, subject (exact id OR wildcard), and unconditioned
// tuples, compared ">= 1". Its bind args are relation, subject_id, "*", "" (the
// unconditioned sentinel), 1 (the CASE result), 1 (the threshold).
func countAtom() string {
	return "COUNT(CASE WHEN ((t.relation = ? AND (" + subjID + " = ? OR " + subjID + " = ?)) " +
		"AND (t.condition_name IS NULL OR t.condition_name = ?)) THEN ? END) >= ?"
}

// condDisjunct is the gather disjunct for a leaf that accepts exactly one named condition:
// the leaf's relation AND that condition name. Its bind args are relation, condition_name.
func condDisjunct() string {
	return "(t.relation = ? AND t.condition_name = ?)"
}

// uncondDisjunct is the gather disjunct for a leaf that accepts only unconditioned tuples:
// the leaf's relation AND a null / empty condition. Its bind args are relation, "".
func uncondDisjunct() string {
	return "(t.relation = ? AND (t.condition_name IS NULL OR t.condition_name = ?))"
}

func TestPlanSQL_DirectHaving(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user]`

	sql, args := recordSQL(t, model, "document", "user:alice")

	// The shared WHERE is pruned to the single referenced relation; the HAVING atom then
	// re-tests it per leaf.
	want := "SELECT ? FROM tuple t WHERE " + sharedWhere() + " AND " + relFilter(1) +
		" GROUP BY t.object_id HAVING " + countAtom()
	require.Equal(t, want, sql)
	require.Equal(t, []any{
		1, "store1", "document", "1", "user", "", "alice", "*", // SELECT 1 + shared WHERE
		"viewer",                         // relation filter
		"viewer", "alice", "*", "", 1, 1, // viewer count atom
	}, args)
}

func TestPlanSQL_UnionHaving(t *testing.T) {
	sql, args := recordSQL(t, unionModel, "document", "user:alice")

	// A union folds to "(atom OR atom)" over the two operand leaves. The relation filter
	// lists both referenced relations, sorted (editor, viewer).
	want := "SELECT ? FROM tuple t WHERE " + sharedWhere() + " AND " + relFilter(2) +
		" GROUP BY t.object_id HAVING (" + countAtom() + " OR " + countAtom() + ")"
	require.Equal(t, want, sql)
	require.Equal(t, []any{
		1, "store1", "document", "1", "user", "", "alice", "*",
		"editor", "viewer", // relation filter (sorted)
		"viewer", "alice", "*", "", 1, 1, // [user] leaf
		"editor", "alice", "*", "", 1, 1, // editor leaf
	}, args)
}

func TestPlanSQL_IntersectionHaving(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define editor: [user]
				define viewer: [user] and editor`

	sql, args := recordSQL(t, model, "document", "user:alice")

	// An intersection folds to "(atom AND atom)". The relation filter lists both relations,
	// sorted (editor, viewer).
	want := "SELECT ? FROM tuple t WHERE " + sharedWhere() + " AND " + relFilter(2) +
		" GROUP BY t.object_id HAVING (" + countAtom() + " AND " + countAtom() + ")"
	require.Equal(t, want, sql)
	require.Equal(t, []any{
		1, "store1", "document", "1", "user", "", "alice", "*",
		"editor", "viewer", // relation filter (sorted)
		"viewer", "alice", "*", "", 1, 1, // [user] leaf
		"editor", "alice", "*", "", 1, 1, // editor leaf
	}, args)
}

func TestPlanSQL_ExclusionHaving(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define banned: [user]
				define viewer: [user] but not banned`

	sql, args := recordSQL(t, model, "document", "user:alice")

	// An exclusion folds to "base AND NOT (subtract)". The relation filter must include the
	// subtract leaf's relation (banned): dropping it would force the subtract count to 0 and
	// silently disable the exclusion. Relations are sorted (banned, viewer).
	want := "SELECT ? FROM tuple t WHERE " + sharedWhere() + " AND " + relFilter(2) +
		" GROUP BY t.object_id HAVING (" + countAtom() + " AND NOT (" + countAtom() + "))"
	require.Equal(t, want, sql)
	require.Equal(t, []any{
		1, "store1", "document", "1", "user", "", "alice", "*",
		"banned", "viewer", // relation filter (sorted; includes subtract leaf)
		"viewer", "alice", "*", "", 1, 1, // base [user] leaf
		"banned", "alice", "*", "", 1, 1, // subtract banned leaf
	}, args)
}

func TestPlanSQL_ConditionedGather(t *testing.T) {
	// viewer mixes a conditioned operand ([user with sudoer]) with an unconditioned one
	// (admin), so the plan gathers candidate rows for in-process CEL rather than folding
	// in HAVING. The projection carries the columns the executor attributes rows by.
	model := `
		model
			schema 1.1
		type user
		type account
			relations
				define admin: [user]
				define viewer: [user with sudoer] and admin
		condition sudoer(name: string) {
			name == "x"
		}`

	sql, args := recordSQL(t, model, "account", "user:alice")

	want := "SELECT t.relation, " + subjID + ", t.condition_name, t.condition_context " +
		"FROM tuple t WHERE " + sharedWhere() +
		" AND (" + condDisjunct() + " OR " + uncondDisjunct() + ")" // [user with sudoer] OR admin
	require.Equal(t, want, sql)
	require.Equal(t, []any{
		"store1", "account", "1", "user", "", "alice", "*", // shared WHERE (no leading SELECT literal)
		"viewer", "sudoer", // conditioned viewer disjunct
		"admin", "", // unconditioned admin disjunct
	}, args)
}

func TestPlanSQL_NestedSetOperationsHaving(t *testing.T) {
	// A deeper rewrite tree that exercises all three operators over five computed
	// relations, each itself defined from direct leaves:
	//
	//	editor     = direct_a or direct_b          (union)
	//	approver   = direct_c and direct_d         (intersection)
	//	privileged = editor and approver           (intersection of the two above)
	//	blocked    = direct_e
	//	viewer     = privileged but not blocked    (exclusion)
	//
	// Every operand is unconditioned, so the whole tree folds in one HAVING clause. The
	// computed relations flatten to their direct leaves, nesting as
	// (((a OR b) AND (c AND d)) AND NOT (e)).
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define direct_a: [user]
				define direct_b: [user]
				define direct_c: [user]
				define direct_d: [user]
				define direct_e: [user]
				define editor: direct_a or direct_b
				define approver: direct_c and direct_d
				define blocked: direct_e
				define privileged: editor and approver
				define viewer: privileged but not blocked`

	sql, args := recordSQL(t, model, "document", "user:alice")

	editor := "(" + countAtom() + " OR " + countAtom() + ")"    // direct_a OR direct_b
	approver := "(" + countAtom() + " AND " + countAtom() + ")" // direct_c AND direct_d
	privileged := "(" + editor + " AND " + approver + ")"       // editor AND approver
	viewer := "(" + privileged + " AND NOT (" + countAtom() + "))"

	// All five direct relations are referenced; the filter lists them sorted (already in
	// direct_a..direct_e order).
	want := "SELECT ? FROM tuple t WHERE " + sharedWhere() + " AND " + relFilter(5) +
		" GROUP BY t.object_id HAVING " + viewer
	require.Equal(t, want, sql)
	require.Equal(t, []any{
		1, "store1", "document", "1", "user", "", "alice", "*",
		"direct_a", "direct_b", "direct_c", "direct_d", "direct_e", // relation filter (sorted)
		"direct_a", "alice", "*", "", 1, 1, // editor base
		"direct_b", "alice", "*", "", 1, 1, // editor alternate
		"direct_c", "alice", "*", "", 1, 1, // approver base
		"direct_d", "alice", "*", "", 1, 1, // approver other
		"direct_e", "alice", "*", "", 1, 1, // blocked (subtract)
	}, args)
}

func TestPlanSQL_NestedSetOperationsWithConditionsGather(t *testing.T) {
	// The same nested rewrite tree as TestPlanSQL_NestedSetOperationsHaving, but two of the
	// five direct leaves carry an ABAC condition:
	//
	//	direct_a = [user with cond_one]            (conditioned)
	//	direct_d = [user with cond_two]            (conditioned)
	//	editor     = direct_a or direct_b          (union)
	//	approver   = direct_c and direct_d         (intersection)
	//	privileged = editor and approver           (intersection)
	//	viewer     = privileged but not blocked    (exclusion; blocked = direct_e)
	//
	// Because the tree mentions conditions, it cannot fold in HAVING: the planner compiles
	// it to one gather scan that pulls the candidate tuples for every leaf (a disjunct per
	// leaf, in pre-order), leaving the set algebra and CEL to the executor. The two
	// conditioned leaves match their named condition; the three plain ones match
	// unconditioned tuples.
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define direct_a: [user with cond_one]
				define direct_b: [user]
				define direct_c: [user]
				define direct_d: [user with cond_two]
				define direct_e: [user]
				define editor: direct_a or direct_b
				define approver: direct_c and direct_d
				define blocked: direct_e
				define privileged: editor and approver
				define viewer: privileged but not blocked
		condition cond_one(x: int) {
			x > 0
		}
		condition cond_two(y: string) {
			y == "ok"
		}`

	sql, args := recordSQL(t, model, "document", "user:alice")

	// One disjunct per leaf, OR'd together, in the tree's pre-order:
	// direct_a (cond_one), direct_b, direct_c, direct_d (cond_two), direct_e.
	operandMatch := "(" + condDisjunct() + " OR " + uncondDisjunct() + " OR " + uncondDisjunct() +
		" OR " + condDisjunct() + " OR " + uncondDisjunct() + ")"
	want := "SELECT t.relation, " + subjID + ", t.condition_name, t.condition_context " +
		"FROM tuple t WHERE " + sharedWhere() + " AND " + operandMatch
	require.Equal(t, want, sql)
	require.Equal(t, []any{
		"store1", "document", "1", "user", "", "alice", "*", // shared WHERE (no leading SELECT literal)
		"direct_a", "cond_one", // editor base: conditioned
		"direct_b", "", // editor alternate: unconditioned
		"direct_c", "", // approver base: unconditioned
		"direct_d", "cond_two", // approver other: conditioned
		"direct_e", "", // blocked (subtract): unconditioned
	}, args)
}

// recordLeafSQL plans objectType#viewer (a weight-2 plan), executes every leaf query
// through a Recorder-backed builder, and returns the SQL and bind args for the leaf at
// index i in compile order. The condition evaluator always denies; it only matters that one
// is supplied so a conditioned leaf can run.
func recordLeafSQL(t *testing.T, model, objectType, user string, i int) (string, []any) {
	t.Helper()
	ts, err := typesystem.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)
	g := ts.GetWeightedGraph()
	require.NotNil(t, g)

	rec := &adaptertest.Recorder{}
	p, err := New(adaptertest.New(rec)).Plan(g, "store1", objectType, "1", "viewer", user)
	require.NoError(t, err)
	require.Equal(t, unitMulti, p.unit.kind)
	require.Greater(t, len(p.unit.multi), i)

	_, err = p.unit.multi[i].query.Execute(context.Background())
	require.NoError(t, err)
	return rec.SQL, rec.Parameters
}

// t1/t2 are the decoded subject expressions for the self-join aliases the planner uses, the
// two-alias analogues of the single-alias subjType/subjID/subjRel vars above.
func decodedSubjType(alias string) string {
	return "SUBSTRING(" + alias + "._user FROM 1 FOR POSITION(':' IN " + alias + "._user) - 1)"
}

func decodedSubjID(alias string) string {
	rest := "SUBSTRING(" + alias + "._user FROM POSITION(':' IN " + alias + "._user) + 1)"
	return "CASE WHEN POSITION('#' IN " + rest + ") = 0 THEN " + rest +
		" ELSE SUBSTRING(" + rest + " FROM 1 FOR POSITION('#' IN " + rest + ") - 1) END"
}

func decodedSubjRel(alias string) string {
	return "CASE WHEN POSITION('#' IN " + alias + "._user) = 0 THEN '' ELSE SUBSTRING(" + alias +
		"._user FROM POSITION('#' IN " + alias + "._user) + 1) END"
}

// countAtomAlias is countAtom for an arbitrary tuple alias: the per-leaf HAVING atom the
// hop-2 fold of a weight-2 boolean self-join emits over the t2 alias. Its bind args are
// relation, subject_id, "*", "" (unconditioned sentinel), 1 (CASE result), 1 (threshold).
func countAtomAlias(alias string) string {
	id := decodedSubjID(alias)
	return "COUNT(CASE WHEN ((" + alias + ".relation = ? AND (" + id + " = ? OR " + id + " = ?)) " +
		"AND (" + alias + ".condition_name IS NULL OR " + alias + ".condition_name = ?)) THEN ? END) >= ?"
}

func TestPlanSQL_WeightTwoTTUJoin(t *testing.T) {
	// viewer: admin from parent compiles to one boolean self-join: t1 names the bound
	// document's parent folders, t2 grants the subject admin on those folders, joined on
	// t1.subject_id = t2.object_id. Both hops require unconditioned tuples.
	model := `
		model
			schema 1.1
		type user
		type folder
			relations
				define admin: [user]
		type document
			relations
				define parent: [folder]
				define viewer: admin from parent`

	sql, args := recordLeafSQL(t, model, "document", "user:alice", 0)

	// t1 names the parent folders (unconditioned); t2 holds the subject's grants on those
	// folders, pruned to the hop-2 relations and bound subject. The hop-2 set algebra (here a
	// single leaf) folds in HAVING, grouped per intermediate object (GROUP BY t2.object_id),
	// so a qualifying folder yields a row.
	t1Uncond := "(t1.condition_name IS NULL OR t1.condition_name = ?)"
	want := "SELECT ? FROM tuple t1 INNER JOIN tuple t2 ON " + decodedSubjID("t1") + " = t2.object_id" +
		" WHERE t1.store = ? AND t1.object_type = ? AND t1.object_id = ? AND t1.relation = ?" +
		" AND " + decodedSubjType("t1") + " = ? AND " + decodedSubjRel("t1") + " = ? AND " + t1Uncond +
		" AND t2.store = ? AND t2.object_type = ?" +
		" AND " + decodedSubjType("t2") + " = ? AND " + decodedSubjRel("t2") + " = ? AND " + decodedSubjID("t2") + " IN (?, ?)" +
		" AND t2.relation = ?" +
		" GROUP BY t2.object_id HAVING " + countAtomAlias("t2")
	require.Equal(t, want, sql)
	require.Equal(t, []any{
		1,                         // SELECT 1
		"store1", "document", "1", // t1 store/object
		"parent",     // t1 hop-1 relation
		"folder", "", // t1 subject type/relation (intermediate object)
		"",                 // t1 unconditioned sentinel
		"store1", "folder", // t2 store/type
		"user", "", "alice", "*", // t2 subject type/relation/id (+ wildcard)
		"admin",                         // t2 relation prune
		"admin", "alice", "*", "", 1, 1, // hop-2 HAVING count atom
	}, args)
}

func TestPlanSQL_WeightTwoUsersetJoin(t *testing.T) {
	// viewer: [group#member] compiles to a boolean self-join whose t1 subject side is the
	// `member` userset (subject_relation = "member"), not an object.
	sql, args := recordLeafSQL(t, usersetModel, "document", "user:alice", 0)

	require.Contains(t, sql, "INNER JOIN tuple t2")
	require.Contains(t, sql, "GROUP BY t2.object_id HAVING")
	require.Equal(t, []any{
		1,
		"store1", "document", "1",
		"viewer",          // t1 hop-1 relation (the relation being checked)
		"group", "member", // t1 subject is the group#member userset
		"", // t1 unconditioned sentinel
		"store1", "group",
		"user", "", "alice", "*",
		"member",                         // t2 relation prune
		"member", "alice", "*", "", 1, 1, // hop-2 HAVING count atom
	}, args)
}

func TestPlanSQL_WeightTwoConditionedGather(t *testing.T) {
	// A condition on the hop-1 edge switches the self-join to a gather: the projection
	// carries both hops' condition columns plus the hop-2 grant the executor attributes.
	model := `
		model
			schema 1.1
		type user
		type group
			relations
				define member: [user]
		type document
			relations
				define viewer: [group#member with cond]
		condition cond(x: int) {
			x > 0
		}`

	sql, args := recordLeafSQL(t, model, "document", "user:alice", 0)

	// The conditioned gather projects the intermediate object id (the group key) first so the
	// executor can group rows per intermediate object, then both hops' condition columns and
	// the hop-2 grant it attributes to a leaf.
	require.True(t, strings.HasPrefix(sql, "SELECT t2.object_id, t1.condition_name, t1.condition_context, t2.relation,"),
		"conditioned join must project the group key then both hops' condition columns, got: %s", sql)
	// The hop-1 condition membership is the named condition (not the unconditioned form).
	require.Contains(t, sql, "t1.condition_name = ?")
	require.Equal(t, []any{
		"store1", "document", "1",
		"viewer",
		"group", "member",
		"cond", // t1 hop-1 condition membership
		"store1", "group",
		"user", "", "alice", "*",
		"member", "", // t2 hop-2 leaf disjunct: relation + unconditioned membership
	}, args)
}

func TestPlanSQL_UnreachableSubjectRunsNoQuery(t *testing.T) {
	// employee never grants document#viewer: the plan is trivially false and must issue
	// no query at all, so the Recorder captures nothing.
	model := `
		model
			schema 1.1
		type user
		type employee
		type document
			relations
				define viewer: [user]`

	sql, args := recordSQL(t, model, "document", "employee:e1")
	require.Empty(t, sql, "an unreachable subject must not execute a query")
	require.Nil(t, args)
}
