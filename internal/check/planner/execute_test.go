package planner

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/adaptertest"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
)

// gRow is one gather-scan row: relation, subject id, condition name, condition context.
// An empty condition name renders as a NULL condition column.
type gRow struct {
	relation  string
	subjectID string
	condName  string
	condCtx   []byte
}

// fakeExecutor answers each rendered query. The boolean reports whether the query is a
// gather scan (projects t.relation first) so the test can route by shape; for a HAVING
// query only the row count matters (the database would have folded the set algebra).
type fakeExecutor struct {
	respond func(sql string, args []any, gather bool) []gRow
}

func (f fakeExecutor) Query(_ context.Context, sql string, args []any) (adapter.Rows, error) {
	gather := strings.HasPrefix(sql, "SELECT t.relation,")
	return &fakeRows{rows: f.respond(sql, args, gather)}, nil
}

type fakeRows struct {
	rows []gRow
	idx  int
	cur  gRow
}

func (r *fakeRows) Next() bool {
	if r.idx >= len(r.rows) {
		return false
	}
	r.cur = r.rows[r.idx]
	r.idx++
	return true
}

// Scan copies the gather projection into (relation, subject_id, condition_name,
// condition_context). It is only called for the gather shape.
func (r *fakeRows) Scan(dest ...any) error {
	*(dest[0].(*sql.NullString)) = nullable(r.cur.relation)
	*(dest[1].(*sql.NullString)) = nullable(r.cur.subjectID)
	*(dest[2].(*sql.NullString)) = nullable(r.cur.condName)
	*(dest[3].(*[]byte)) = r.cur.condCtx
	return nil
}

func (r *fakeRows) Close() error { return nil }
func (r *fakeRows) Err() error   { return nil }

func nullable(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// rowsFor builds an executor that returns the given rows for every query.
func rowsFor(rows ...gRow) fakeExecutor {
	return fakeExecutor{respond: func(string, []any, bool) []gRow { return rows }}
}

// planFor plans a Check of the given user against objectType#viewer using a builder
// backed by exec. Every test relation is named "viewer".
func planFor(t *testing.T, exec adaptertest.Executor, model, objectType, user string) *Plan {
	t.Helper()
	ts, err := typesystem.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)
	g := ts.GetWeightedGraph()
	require.NotNil(t, g)

	p, err := New(adaptertest.New(exec)).Plan(g, "store1", objectType, "1", "viewer", user)
	require.NoError(t, err)
	return p
}

const unionModel = `
	model
		schema 1.1
	type user
	type document
		relations
			define editor: [user]
			define viewer: [user] or editor`

const directModel = `
	model
		schema 1.1
	type user
	type document
		relations
			define viewer: [user]`

// TestExecute_Having covers the condition-free path: the database folds the set algebra,
// so the executor only reports a row / no row and Execute mirrors it.
func TestExecute_Having(t *testing.T) {
	t.Run("granted", func(t *testing.T) {
		p := planFor(t, rowsFor(gRow{}), unionModel, "document", "user:alice")
		require.Equal(t, unitHaving, p.unit.kind)
		got, err := p.Execute(context.Background(), nil)
		require.NoError(t, err)
		require.True(t, got)
	})

	t.Run("denied", func(t *testing.T) {
		p := planFor(t, rowsFor(), directModel, "document", "user:alice")
		got, err := p.Execute(context.Background(), nil)
		require.NoError(t, err)
		require.False(t, got)
	})
}

// TestExecute_Unreachable covers a Check that cannot reach the subject type: it is
// trivially false and runs no query.
func TestExecute_Unreachable(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type employee
		type document
			relations
				define viewer: [user]`
	exec := fakeExecutor{respond: func(string, []any, bool) []gRow {
		t.Fatal("unreachable Check must not run a query")
		return nil
	}}
	p := planFor(t, exec, model, "document", "employee:e1")
	require.Equal(t, unitFalse, p.unit.kind)
	got, err := p.Execute(context.Background(), nil)
	require.NoError(t, err)
	require.False(t, got)
}

const sudoerModel = `
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

// TestExecute_GatherCondition is the worked example: super_admin = [user with sudoer] and
// admin. One gather scan returns both operands' tuples; Go evaluates the condition and
// folds the intersection.
func TestExecute_GatherCondition(t *testing.T) {
	// Both operands present: a conditioned super_admin (here `viewer`) tuple and an
	// unconditioned admin tuple.
	rows := []gRow{
		{relation: "viewer", subjectID: "alice", condName: "sudoer", condCtx: []byte("ctx")},
		{relation: "admin", subjectID: "alice"},
	}

	t.Run("condition_passes", func(t *testing.T) {
		p := planFor(t, rowsFor(rows...), sudoerModel, "account", "user:alice")
		require.Equal(t, unitGather, p.unit.kind)
		got, err := p.Execute(context.Background(), evalFunc(func(string, []byte) (bool, error) { return true, nil }))
		require.NoError(t, err)
		require.True(t, got)
	})

	t.Run("condition_fails", func(t *testing.T) {
		// The conditioned operand is not satisfied, so the intersection is false.
		p := planFor(t, rowsFor(rows...), sudoerModel, "account", "user:alice")
		got, err := p.Execute(context.Background(), evalFunc(func(string, []byte) (bool, error) { return false, nil }))
		require.NoError(t, err)
		require.False(t, got)
	})

	t.Run("other_operand_missing", func(t *testing.T) {
		// admin tuple absent: even with the condition passing, the intersection is false.
		only := rowsFor(gRow{relation: "viewer", subjectID: "alice", condName: "sudoer", condCtx: []byte("ctx")})
		p := planFor(t, only, sudoerModel, "account", "user:alice")
		got, err := p.Execute(context.Background(), evalFunc(func(string, []byte) (bool, error) { return true, nil }))
		require.NoError(t, err)
		require.False(t, got)
	})
}

// TestExecute_GatherExclusion checks a conditioned exclusion folds in Go: base BUT NOT a
// conditioned subtract. When the subtract's condition passes it removes the grant.
func TestExecute_GatherExclusion(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define banned: [user with sudoer]
				define viewer: [user] but not banned
		condition sudoer(name: string) {
			name == "x"
		}`

	base := gRow{relation: "viewer", subjectID: "alice"}
	banned := gRow{relation: "banned", subjectID: "alice", condName: "sudoer", condCtx: []byte("ctx")}

	t.Run("subtract_condition_passes_denies", func(t *testing.T) {
		p := planFor(t, rowsFor(base, banned), model, "document", "user:alice")
		require.Equal(t, unitGather, p.unit.kind)
		got, err := p.Execute(context.Background(), evalFunc(func(string, []byte) (bool, error) { return true, nil }))
		require.NoError(t, err)
		require.False(t, got)
	})

	t.Run("subtract_condition_fails_grants", func(t *testing.T) {
		p := planFor(t, rowsFor(base, banned), model, "document", "user:alice")
		got, err := p.Execute(context.Background(), evalFunc(func(string, []byte) (bool, error) { return false, nil }))
		require.NoError(t, err)
		require.True(t, got)
	})
}

// TestExecute_GatherWildcard checks that a public-access (subject_id "*") row grants a
// non-userset subject, attributed correctly in the gather fold.
func TestExecute_GatherWildcard(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define admin: [user]
				define viewer: [user:*, user with sudoer] and admin
		condition sudoer(name: string) {
			name == "x"
		}`

	// viewer satisfied by the wildcard tuple (no condition), admin by alice directly.
	rows := []gRow{
		{relation: "viewer", subjectID: "*"},
		{relation: "admin", subjectID: "alice"},
	}
	p := planFor(t, rowsFor(rows...), model, "document", "user:alice")
	require.Equal(t, unitGather, p.unit.kind)
	got, err := p.Execute(context.Background(), evalFunc(func(string, []byte) (bool, error) { return false, nil }))
	require.NoError(t, err)
	require.True(t, got, "wildcard tuple grants the non-userset subject")
}

// TestExecute_GatherEvalError verifies that a CEL evaluation error from the gather path
// surfaces from Execute rather than being swallowed into a (wrong) deny. The conditioned
// operand's evaluator returns an error, so the per-leaf CEL fold must fail the whole Check.
func TestExecute_GatherEvalError(t *testing.T) {
	rows := []gRow{
		{relation: "viewer", subjectID: "alice", condName: "sudoer", condCtx: []byte("ctx")},
		{relation: "admin", subjectID: "alice"},
	}
	p := planFor(t, rowsFor(rows...), sudoerModel, "account", "user:alice")
	require.Equal(t, unitGather, p.unit.kind)

	sentinel := errors.New("cel blew up")
	_, err := p.Execute(context.Background(), evalFunc(func(string, []byte) (bool, error) { return false, sentinel }))
	require.ErrorIs(t, err, sentinel)
}

// TestExecute_GatherNilEvaluator verifies the defensive guard for a conditioned tuple with no
// evaluator supplied: the gather path must error (not silently deny) when a row carries a
// condition but eval is nil.
func TestExecute_GatherNilEvaluator(t *testing.T) {
	rows := []gRow{
		{relation: "viewer", subjectID: "alice", condName: "sudoer", condCtx: []byte("ctx")},
		{relation: "admin", subjectID: "alice"},
	}
	p := planFor(t, rowsFor(rows...), sudoerModel, "account", "user:alice")
	require.Equal(t, unitGather, p.unit.kind)

	_, err := p.Execute(context.Background(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no evaluator was provided")
}

// TestExecute_HavingQueryError verifies that a datastore error from a condition-free (unitHaving)
// plan's single boolean query surfaces from Execute, wrapped, rather than being reported as a
// clean deny.
func TestExecute_HavingQueryError(t *testing.T) {
	sentinel := errors.New("datastore exploded")
	p := planFor(t, errExecutor{err: sentinel}, unionModel, "document", "user:alice")
	require.Equal(t, unitHaving, p.unit.kind)

	_, err := p.Execute(context.Background(), nil)
	require.ErrorIs(t, err, sentinel)
}

// TestExecute_GatherQueryError verifies that a datastore error from a conditioned (unitGather)
// plan's scan surfaces from Execute rather than being swallowed.
func TestExecute_GatherQueryError(t *testing.T) {
	sentinel := errors.New("datastore exploded")
	p := planFor(t, errExecutor{err: sentinel}, sudoerModel, "account", "user:alice")
	require.Equal(t, unitGather, p.unit.kind)

	_, err := p.Execute(context.Background(), evalFunc(func(string, []byte) (bool, error) { return true, nil }))
	require.ErrorIs(t, err, sentinel)
}

// TestExecute_StandaloneLeafGatherEvalError covers the standalone conditioned weight-1 leaf
// gather (the leafGather runLeaf branch for a lone leaf, distinct from a merged region): a CEL
// error on that leaf must surface from Execute. The model unions a single conditioned direct
// leaf with a weight-2 hop, so the leaf compiles to its own single-leaf gather beside the join.
func TestExecute_StandaloneLeafGatherEvalError(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type group
			relations
				define member: [user]
		type document
			relations
				define direct_viewer: [user with cond]
				define from_group: [group#member]
				define viewer: direct_viewer or from_group
		condition cond(x: int) {
			x > 0
		}`

	sentinel := errors.New("cel blew up")
	exec := w2Executor{
		gatherRows: func(string, []any) []gRow {
			return []gRow{{relation: "direct_viewer", subjectID: "alice", condName: "cond", condCtx: []byte("c")}}
		},
		boolRow: func(string, []any) bool { return false },
	}
	p := w2PlanFor(t, exec, model)
	require.Equal(t, unitMulti, p.unit.kind)
	// The conditioned direct leaf must be a standalone gather (no sibling to merge with), not a
	// merged region: assert no unit records subLeaves.
	for _, lq := range p.unit.multi {
		require.Nil(t, lq.subLeaves, "the lone conditioned leaf must be a standalone gather, not a merged region")
	}

	_, err := p.Execute(context.Background(), evalFunc(func(string, []byte) (bool, error) { return false, sentinel }))
	require.ErrorIs(t, err, sentinel)
}

// TestExecute_GatherWildcardConditionNotMisattributed guards against malformed data: in
// `viewer: [user, user:* with isOk]` the condition isOk is declared only on the public-access
// wildcard assignment, so a concrete-user tuple (user:alice) carrying isOk should never exist.
// If it somehow does, it must not grant the Check — the conditioned isOk leaf matches only the
// wildcard subject_id "*", and the unconditioned [user] leaf accepts only unconditioned tuples,
// so neither leaf attributes the stray row. The gather SQL's WHERE is a superset and does return
// the row, so this pins the executor's per-leaf attribution as the thing that excludes it. The
// evaluator returns true throughout, proving the exclusion is structural (subject + condition
// matching), not a CEL denial — CEL is never consulted for this row.
func TestExecute_GatherWildcardConditionNotMisattributed(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user, user:* with isOk]
		condition isOk(x: int) {
			x > 0
		}`
	grantAll := evalFunc(func(string, []byte) (bool, error) { return true, nil })

	t.Run("concrete_conditioned_tuple_excluded", func(t *testing.T) {
		// The malformed row: a concrete subject (alice), the wildcard-only condition isOk, and a
		// nil context. It must not be attributed to either leaf, so the Check is denied.
		row := gRow{relation: "viewer", subjectID: "alice", condName: "isOk", condCtx: nil}
		p := planFor(t, rowsFor(row), model, "document", "user:alice")
		require.Equal(t, unitGather, p.unit.kind)

		got, err := p.Execute(context.Background(), grantAll)
		require.NoError(t, err)
		require.False(t, got, "a concrete-user tuple carrying the wildcard-only condition must not grant")
	})

	t.Run("legitimate_wildcard_conditioned_grants", func(t *testing.T) {
		// Control: a well-formed wildcard tuple (subject_id "*") with isOk grants when CEL passes,
		// proving the model itself can grant this way and the exclusion above is specific.
		row := gRow{relation: "viewer", subjectID: "*", condName: "isOk", condCtx: nil}
		p := planFor(t, rowsFor(row), model, "document", "user:alice")

		got, err := p.Execute(context.Background(), grantAll)
		require.NoError(t, err)
		require.True(t, got, "a well-formed wildcard tuple with a passing condition grants")
	})

	t.Run("legitimate_concrete_unconditioned_grants", func(t *testing.T) {
		// Control: a well-formed concrete tuple ([user], unconditioned) grants regardless of CEL.
		row := gRow{relation: "viewer", subjectID: "alice", condName: ""}
		p := planFor(t, rowsFor(row), model, "document", "user:alice")

		got, err := p.Execute(context.Background(), grantAll)
		require.NoError(t, err)
		require.True(t, got, "a well-formed unconditioned [user] tuple grants")
	})
}

// evalFunc adapts a function to the ConditionEvaluator interface.
type evalFunc func(name string, context []byte) (bool, error)

func (f evalFunc) Eval(_ context.Context, name string, context []byte) (bool, error) {
	return f(name, context)
}

// TestEvalCondition_CacheKeyNoCollision guards against the old key scheme that joined
// name and context with a single null byte: (name "a", ctx "\x00b") and (name "a\x00",
// ctx "b") concatenated to the same "a\x00\x00b" string, so the second pair wrongly
// reused the first's cached result. The tagged, length-prefixed key keeps them distinct,
// so each pair triggers its own Eval.
func TestEvalCondition_CacheKeyNoCollision(t *testing.T) {
	var calls int
	eval := evalFunc(func(string, []byte) (bool, error) {
		calls++
		return true, nil
	})
	leaf := &QueryNode{Label: "document#viewer"}
	cache := make(map[keys.Key]bool)

	_, err := evalCondition(context.Background(), eval, leaf, gatherRow{condName: "a", condCtx: []byte("\x00b")}, cache)
	require.NoError(t, err)
	_, err = evalCondition(context.Background(), eval, leaf, gatherRow{condName: "a\x00", condCtx: []byte("b")}, cache)
	require.NoError(t, err)

	require.Equal(t, 2, calls, "distinct (name, context) pairs must not share a cache entry")
	require.Len(t, cache, 2)

	// A repeat of the first pair is served from the cache, not re-evaluated.
	_, err = evalCondition(context.Background(), eval, leaf, gatherRow{condName: "a", condCtx: []byte("\x00b")}, cache)
	require.NoError(t, err)
	require.Equal(t, 2, calls, "an identical pair must reuse its cached result")
}
