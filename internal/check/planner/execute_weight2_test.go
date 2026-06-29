package planner

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/adaptertest"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
)

// w2Executor answers each rendered query by shape so a weight-2 multi-query plan can be
// driven without a database. A plan with weight-2 joins issues three query shapes, told
// apart by their SELECT prefix:
//
//   - "SELECT ?"               a boolean existence query (weight-1 leaf or condition-free
//     self-join); granted iff bools returns a row for that query.
//   - "SELECT t.relation,"     a single-leaf gather (conditioned weight-1 leaf).
//   - "SELECT t1.condition..." a self-join gather (conditioned weight-2 hop).
//
// Each shape's response is supplied by a callback keyed on the rendered SQL and args, so a
// test can return different rows per leaf.
type w2Executor struct {
	boolRow    func(sql string, args []any) bool   // existence: true => one row
	gatherRows func(sql string, args []any) []gRow // single-leaf gather rows
	joinRows   func(sql string, args []any) []jRow // self-join gather rows
}

// jRow is one self-join gather row, in buildJoinGatherQuery projection order.
type jRow struct {
	intermediateID string
	hop1Cond       string
	hop1CondCtx    []byte
	hop2Rel        string
	subjectID      string
	hop2Cond       string
	hop2CondCtx    []byte
}

func (e w2Executor) Query(_ context.Context, query string, args []any) (adapter.Rows, error) {
	switch {
	case strings.HasPrefix(query, "SELECT t2.object_id,"):
		var rows []jRow
		if e.joinRows != nil {
			rows = e.joinRows(query, args)
		}
		return &jRows{rows: rows}, nil
	case strings.HasPrefix(query, "SELECT t.relation,"):
		var rows []gRow
		if e.gatherRows != nil {
			rows = e.gatherRows(query, args)
		}
		return &fakeRows{rows: rows}, nil
	default: // "SELECT ?" boolean existence
		one := false
		if e.boolRow != nil {
			one = e.boolRow(query, args)
		}
		if one {
			return &boolRows{remaining: 1}, nil
		}
		return &boolRows{}, nil
	}
}

// boolRows is a result with a fixed number of empty rows for an existence query.
type boolRows struct{ remaining int }

func (r *boolRows) Next() bool {
	if r.remaining <= 0 {
		return false
	}
	r.remaining--
	return true
}
func (r *boolRows) Scan(...any) error { return nil }
func (r *boolRows) Close() error      { return nil }
func (r *boolRows) Err() error        { return nil }

// jRows scans the seven-column self-join gather projection.
type jRows struct {
	rows []jRow
	idx  int
	cur  jRow
}

func (r *jRows) Next() bool {
	if r.idx >= len(r.rows) {
		return false
	}
	r.cur = r.rows[r.idx]
	r.idx++
	return true
}

func (r *jRows) Scan(dest ...any) error {
	*(dest[0].(*sql.NullString)) = nullable(r.cur.intermediateID)
	*(dest[1].(*sql.NullString)) = nullable(r.cur.hop1Cond)
	*(dest[2].(*[]byte)) = r.cur.hop1CondCtx
	*(dest[3].(*sql.NullString)) = nullable(r.cur.hop2Rel)
	*(dest[4].(*sql.NullString)) = nullable(r.cur.subjectID)
	*(dest[5].(*sql.NullString)) = nullable(r.cur.hop2Cond)
	*(dest[6].(*[]byte)) = r.cur.hop2CondCtx
	return nil
}

func (r *jRows) Close() error { return nil }
func (r *jRows) Err() error   { return nil }

// w2PlanFor plans document#viewer for user:alice over the given model with an executor backing
// asserting the plan is a multi-query (weight-2) plan.
func w2PlanFor(t *testing.T, exec adaptertest.Executor, model string) *Plan {
	t.Helper()
	ts, err := typesystem.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)
	g := ts.GetWeightedGraph()
	require.NotNil(t, g)

	p, err := New(adaptertest.New(exec)).Plan(g, "store1", "document", "1", "viewer", "user:alice")
	require.NoError(t, err)
	require.Equal(t, unitMulti, p.unit.kind)
	return p
}

const ttuModel = `
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

const usersetModel = `
	model
		schema 1.1
	type user
	type group
		relations
			define member: [user]
	type document
		relations
			define viewer: [group#member]`

// TestExecuteW2_BooleanTTU covers a condition-free TTU hop: the self-join is one boolean
// query, granted iff it returns a row.
func TestExecuteW2_BooleanTTU(t *testing.T) {
	t.Run("granted", func(t *testing.T) {
		exec := w2Executor{boolRow: func(string, []any) bool { return true }}
		p := w2PlanFor(t, exec, ttuModel)
		got, err := p.Execute(context.Background(), nil)
		require.NoError(t, err)
		require.True(t, got)
	})

	t.Run("denied", func(t *testing.T) {
		exec := w2Executor{boolRow: func(string, []any) bool { return false }}
		p := w2PlanFor(t, exec, ttuModel)
		got, err := p.Execute(context.Background(), nil)
		require.NoError(t, err)
		require.False(t, got)
	})
}

// TestExecuteW2_BooleanUserset covers a condition-free userset hop.
func TestExecuteW2_BooleanUserset(t *testing.T) {
	exec := w2Executor{boolRow: func(string, []any) bool { return true }}
	p := w2PlanFor(t, exec, usersetModel)
	got, err := p.Execute(context.Background(), nil)
	require.NoError(t, err)
	require.True(t, got)
}

// TestExecuteW2_MixedUnion covers a union of a weight-1 leaf and a weight-2 join: the union
// is granted if either query returns a row. The leaf query is denied; the join is granted.
func TestExecuteW2_MixedUnion(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type group
			relations
				define member: [user]
		type document
			relations
				define viewer: [user, group#member]`

	t.Run("join_grants", func(t *testing.T) {
		// The direct [user] leaf finds nothing; the group#member hop grants.
		exec := w2Executor{boolRow: func(sql string, _ []any) bool {
			return strings.Contains(sql, "INNER JOIN")
		}}
		p := w2PlanFor(t, exec, model)
		got, err := p.Execute(context.Background(), nil)
		require.NoError(t, err)
		require.True(t, got)
	})

	t.Run("neither_grants", func(t *testing.T) {
		exec := w2Executor{boolRow: func(string, []any) bool { return false }}
		p := w2PlanFor(t, exec, model)
		got, err := p.Execute(context.Background(), nil)
		require.NoError(t, err)
		require.False(t, got)
	})
}

// TestExecuteW2_ConditionedHop covers a weight-2 userset hop with a condition on hop-1: the
// self-join gathers joined rows and the executor evaluates the hop-1 condition in process.
func TestExecuteW2_ConditionedHop(t *testing.T) {
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

	// One joined row: the hop-1 (viewer) tuple carries `cond`, the hop-2 (member) grant is
	// unconditioned for alice.
	row := jRow{intermediateID: "g1", hop1Cond: "cond", hop1CondCtx: []byte("ctx"), hop2Rel: "member", subjectID: "alice"}
	exec := w2Executor{joinRows: func(string, []any) []jRow { return []jRow{row} }}

	t.Run("condition_passes_grants", func(t *testing.T) {
		p := w2PlanFor(t, exec, model)
		got, err := p.Execute(context.Background(), evalFunc(func(string, []byte) (bool, error) { return true, nil }))
		require.NoError(t, err)
		require.True(t, got)
	})

	t.Run("condition_fails_denies", func(t *testing.T) {
		p := w2PlanFor(t, exec, model)
		got, err := p.Execute(context.Background(), evalFunc(func(string, []byte) (bool, error) { return false, nil }))
		require.NoError(t, err)
		require.False(t, got)
	})

	t.Run("no_joined_rows_denies", func(t *testing.T) {
		empty := w2Executor{joinRows: func(string, []any) []jRow { return nil }}
		p := w2PlanFor(t, empty, model)
		got, err := p.Execute(context.Background(), evalFunc(func(string, []byte) (bool, error) { return true, nil }))
		require.NoError(t, err)
		require.False(t, got)
	})
}

// TestExecuteW2_IntersectionMixed covers `viewer: [user] and admin from parent` (a weight-1
// leaf intersected with a weight-2 TTU hop): granted only if both queries return a row.
func TestExecuteW2_IntersectionMixed(t *testing.T) {
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
				define direct: [user]
				define viewer: direct and admin from parent`

	t.Run("both_grant", func(t *testing.T) {
		exec := w2Executor{boolRow: func(string, []any) bool { return true }}
		p := w2PlanFor(t, exec, model)
		got, err := p.Execute(context.Background(), nil)
		require.NoError(t, err)
		require.True(t, got)
	})

	t.Run("only_join_grants_denies", func(t *testing.T) {
		// The TTU hop grants but the direct leaf does not: the intersection is false.
		exec := w2Executor{boolRow: func(sql string, _ []any) bool {
			return strings.Contains(sql, "INNER JOIN")
		}}
		p := w2PlanFor(t, exec, model)
		got, err := p.Execute(context.Background(), nil)
		require.NoError(t, err)
		require.False(t, got)
	})
}

// hop2IntersectionCondModel is a weight-2 TTU whose hop-2 relation is an intersection of a
// conditioned and an unconditioned leaf (`admin: [user with cond] and b`). The condition
// forces the conditioned (gather) path, so the per-intermediate-object fold runs in process:
// the hop is granted only when a single folder carries both `admin` (condition passing) and
// `b` for the subject.
const hop2IntersectionCondModel = `
	model
		schema 1.1
	type user
	type folder
		relations
			define admin: [user with cond]
			define b: [user]
			define grant: admin and b
	type document
		relations
			define parent: [folder]
			define viewer: grant from parent
	condition cond(x: int) {
		x > 0
	}`

// TestExecuteW2_Hop2IntersectionPerObject is the correctness guard for hop-2 set operations:
// an intersection must be satisfied by ONE intermediate object holding every operand, not by
// operands spread across different objects. It drives the conditioned gather path, where the
// executor groups joined rows by intermediate object and folds the hop-2 subtree per object.
func TestExecuteW2_Hop2IntersectionPerObject(t *testing.T) {
	pass := evalFunc(func(string, []byte) (bool, error) { return true, nil })

	t.Run("single_object_has_both_grants", func(t *testing.T) {
		// folder f1 carries both admin (cond) and b → intersection holds for f1 → granted.
		exec := w2Executor{joinRows: func(string, []any) []jRow {
			return []jRow{
				{intermediateID: "f1", hop1Cond: "", hop2Rel: "admin", subjectID: "alice", hop2Cond: "cond", hop2CondCtx: []byte("c")},
				{intermediateID: "f1", hop1Cond: "", hop2Rel: "b", subjectID: "alice"},
			}
		}}
		p := w2PlanFor(t, exec, hop2IntersectionCondModel)
		got, err := p.Execute(context.Background(), pass)
		require.NoError(t, err)
		require.True(t, got)
	})

	t.Run("operands_split_across_objects_denies", func(t *testing.T) {
		// folder f1 has only admin, folder f2 has only b: no single folder satisfies the
		// intersection, so the hop is denied. This is the case a relation-set filter would
		// wrongly accept.
		exec := w2Executor{joinRows: func(string, []any) []jRow {
			return []jRow{
				{intermediateID: "f1", hop1Cond: "", hop2Rel: "admin", subjectID: "alice", hop2Cond: "cond", hop2CondCtx: []byte("c")},
				{intermediateID: "f2", hop1Cond: "", hop2Rel: "b", subjectID: "alice"},
			}
		}}
		p := w2PlanFor(t, exec, hop2IntersectionCondModel)
		got, err := p.Execute(context.Background(), pass)
		require.NoError(t, err)
		require.False(t, got, "operands on different intermediate objects must not satisfy a hop-2 intersection")
	})

	t.Run("condition_fails_drops_admin_denies", func(t *testing.T) {
		// f1 has both grants, but admin's condition fails, so only b survives → intersection
		// false → denied.
		exec := w2Executor{joinRows: func(string, []any) []jRow {
			return []jRow{
				{intermediateID: "f1", hop1Cond: "", hop2Rel: "admin", subjectID: "alice", hop2Cond: "cond", hop2CondCtx: []byte("c")},
				{intermediateID: "f1", hop1Cond: "", hop2Rel: "b", subjectID: "alice"},
			}
		}}
		p := w2PlanFor(t, exec, hop2IntersectionCondModel)
		got, err := p.Execute(context.Background(), evalFunc(func(string, []byte) (bool, error) { return false, nil }))
		require.NoError(t, err)
		require.False(t, got)
	})
}

// TestExecuteW2_Hop2ExclusionPerObject guards the hop-2 exclusion (`grant: admin but not
// banned`) under the conditioned path: an intermediate object grants only if it has admin
// and is NOT banned, evaluated per object.
func TestExecuteW2_Hop2ExclusionPerObject(t *testing.T) {
	const model = `
		model
			schema 1.1
		type user
		type folder
			relations
				define admin: [user with cond]
				define banned: [user]
				define grant: admin but not banned
		type document
			relations
				define parent: [folder]
				define viewer: grant from parent
		condition cond(x: int) {
			x > 0
		}`
	pass := evalFunc(func(string, []byte) (bool, error) { return true, nil })

	t.Run("admin_not_banned_grants", func(t *testing.T) {
		exec := w2Executor{joinRows: func(string, []any) []jRow {
			return []jRow{
				{intermediateID: "f1", hop2Rel: "admin", subjectID: "alice", hop2Cond: "cond", hop2CondCtx: []byte("c")},
			}
		}}
		p := w2PlanFor(t, exec, model)
		got, err := p.Execute(context.Background(), pass)
		require.NoError(t, err)
		require.True(t, got)
	})

	t.Run("admin_but_banned_denies", func(t *testing.T) {
		exec := w2Executor{joinRows: func(string, []any) []jRow {
			return []jRow{
				{intermediateID: "f1", hop2Rel: "admin", subjectID: "alice", hop2Cond: "cond", hop2CondCtx: []byte("c")},
				{intermediateID: "f1", hop2Rel: "banned", subjectID: "alice"},
			}
		}}
		p := w2PlanFor(t, exec, model)
		got, err := p.Execute(context.Background(), pass)
		require.NoError(t, err)
		require.False(t, got, "a banned subject on the same object must be excluded")
	})
}

// complexW2Model combines set operations at both levels of weight-2 resolution: three
// distinct weight-2 hops are joined by the outer relation through an intersection and an
// exclusion, and the first hop itself carries a hop-2 intersection.
//
//	viewer = (from_folder AND from_org) BUT NOT from_blocked
//	  from_folder  = grant from parent     (hop-2 = admin AND editor on the folder)
//	  from_org     = owner from org        (hop-2 = owner on the org)
//	  from_blocked = editor from blocked   (hop-2 = editor on the blocked folder)
//
// Every hop is condition-free, so each compiles to a boolean self-join (GROUP BY / HAVING
// folds the hop-2 algebra per intermediate object) and the outer intersection / exclusion
// folds the three booleans in process.
const complexW2Model = `
	model
		schema 1.1
	type user
	type folder
		relations
			define admin: [user]
			define editor: [user]
			define grant: admin and editor
	type org
		relations
			define owner: [user]
	type document
		relations
			define parent: [folder]
			define org: [org]
			define blocked: [folder]
			define from_folder: grant from parent
			define from_org: owner from org
			define from_blocked: editor from blocked
			define privileged: from_folder and from_org
			define viewer: privileged but not from_blocked`

// hopGranted routes the boolean self-join responses by the hop-1 relation each query binds
// (the relation appears in the bind args), so a test can grant or deny each of the three
// hops independently.
func hopGranted(grants map[string]bool) func(sql string, args []any) bool {
	return func(_ string, args []any) bool {
		for _, a := range args {
			if s, ok := a.(string); ok {
				if g, named := grants[s]; named {
					return g
				}
			}
		}
		return false
	}
}

// TestExecuteW2_ComplexBothLevels exercises set operations at both the outer level (an
// intersection feeding an exclusion over three weight-2 hops) and the hop-2 level (the
// from_folder hop's admin AND editor). It walks the truth table of
// (from_folder AND from_org) BUT NOT from_blocked by granting each hop independently.
func TestExecuteW2_ComplexBothLevels(t *testing.T) {
	cases := []struct {
		name     string
		grants   map[string]bool // hop-1 relation -> whether that hop's self-join returns a row
		expected bool
	}{
		{
			name:     "both_required_present_not_blocked_grants",
			grants:   map[string]bool{"parent": true, "org": true, "blocked": false},
			expected: true,
		},
		{
			name:     "from_folder_missing_denies",
			grants:   map[string]bool{"parent": false, "org": true, "blocked": false},
			expected: false,
		},
		{
			name:     "from_org_missing_denies",
			grants:   map[string]bool{"parent": true, "org": false, "blocked": false},
			expected: false,
		},
		{
			name:     "privileged_but_blocked_denies",
			grants:   map[string]bool{"parent": true, "org": true, "blocked": true},
			expected: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exec := w2Executor{boolRow: hopGranted(tc.grants)}
			p := w2PlanFor(t, exec, complexW2Model)
			require.Len(t, p.unit.multi, 3, "three weight-2 hops, each its own query")
			got, err := p.Execute(context.Background(), nil)
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}
}

// mergedW2Model places two weight-1 leaves (direct_viewer, direct_owner) in the same union as a
// weight-2 userset hop, so the two leaves merge into one query (a HAVING fold) while the hop
// stays its own self-join. It proves the merged region resolves correctly beside a join, and in
// particular that neither merged operand is lost when the original union is rewritten.
//
//	viewer = (direct_viewer OR direct_owner OR from_group)
const mergedW2Model = `
	model
		schema 1.1
	type user
	type group
		relations
			define member: [user]
	type document
		relations
			define direct_viewer: [user]
			define direct_owner: [user]
			define from_group: [group#member]
			define viewer: direct_viewer or direct_owner or from_group`

// TestExecuteW2_MergedRegionUnion exercises the weight-1 sibling-merge optimization end to end
// against the fake executor: the merged (direct_viewer OR direct_owner) region is one boolean
// query, the from_group hop another, and the outer union folds the two. It walks each operand
// granting alone so a dropped operand in the rewrite would surface as a wrong decision.
func TestExecuteW2_MergedRegionUnion(t *testing.T) {
	// grants routes the boolean responses by the relation each query binds: the merged region's
	// HAVING query binds direct_viewer and direct_owner; the hop binds the userset relation.
	cases := []struct {
		name     string
		grants   map[string]bool
		expected bool
	}{
		{name: "grant_via_direct_viewer", grants: map[string]bool{"direct_viewer": true}, expected: true},
		{name: "grant_via_direct_owner", grants: map[string]bool{"direct_owner": true}, expected: true},
		{name: "grant_via_hop", grants: map[string]bool{"member": true}, expected: true},
		{name: "deny_when_none_grant", grants: map[string]bool{}, expected: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exec := w2Executor{boolRow: hopGranted(tc.grants)}
			p := w2PlanFor(t, exec, mergedW2Model)
			require.Len(t, p.unit.multi, 2, "merged weight-1 region + one hop")
			got, err := p.Execute(context.Background(), nil)
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}
}

// TestExecuteW2_MergedRegionConditioned exercises a conditioned merged region beside a join: the
// region compiles to one gather scan whose rows the executor attributes to its leaves and folds,
// rather than one query per conditioned leaf. Here direct_viewer and direct_owner both carry a
// condition and union with a weight-2 hop.
func TestExecuteW2_MergedRegionConditioned(t *testing.T) {
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
				define direct_owner: [user with cond]
				define from_group: [group#member]
				define viewer: direct_viewer or direct_owner or from_group
		condition cond(x: int) {
			x > 0
		}`

	t.Run("region_grants_when_condition_passes", func(t *testing.T) {
		// The region's gather returns a direct_owner row; the hop finds nothing. CEL passes.
		exec := w2Executor{
			gatherRows: func(string, []any) []gRow {
				return []gRow{{relation: "direct_owner", subjectID: "alice", condName: "cond", condCtx: []byte("c")}}
			},
			boolRow: func(string, []any) bool { return false },
		}
		p := w2PlanFor(t, exec, model)
		require.Len(t, p.unit.multi, 2)
		got, err := p.Execute(context.Background(), evalFunc(func(string, []byte) (bool, error) { return true, nil }))
		require.NoError(t, err)
		require.True(t, got)
	})

	t.Run("region_denies_when_condition_fails", func(t *testing.T) {
		exec := w2Executor{
			gatherRows: func(string, []any) []gRow {
				return []gRow{{relation: "direct_owner", subjectID: "alice", condName: "cond", condCtx: []byte("c")}}
			},
			boolRow: func(string, []any) bool { return false },
		}
		p := w2PlanFor(t, exec, model)
		got, err := p.Execute(context.Background(), evalFunc(func(string, []byte) (bool, error) { return false, nil }))
		require.NoError(t, err)
		require.False(t, got)
	})

	t.Run("grant_via_hop_when_region_empty", func(t *testing.T) {
		// The region gathers no rows, but the hop grants → union holds.
		exec := w2Executor{
			gatherRows: func(string, []any) []gRow { return nil },
			boolRow:    func(string, []any) bool { return true },
		}
		p := w2PlanFor(t, exec, model)
		got, err := p.Execute(context.Background(), evalFunc(func(string, []byte) (bool, error) { return true, nil }))
		require.NoError(t, err)
		require.True(t, got)
	})
}

// mergedExclusionModel is `(([user] or editor) and owner from parent) but not (blocked and old)`.
// It produces three compiled units in the unitMulti plan: a merged weight-1 UNION region
// ([user] OR editor), one weight-2 TTU self-join (owner from parent), and a merged weight-1
// INTERSECT region (blocked AND old). The outer fold is positive BUT NOT negative, where
// positive = (union AND ttu) and negative = the merged intersect.
const mergedExclusionModel = `
	model
		schema 1.1
	type user
	type folder
		relations
			define owner: [user]
	type document
		relations
			define parent: [folder]
			define editor: [user]
			define blocked: [user]
			define old: [user]
			define direct_or_editor: [user] or editor
			define owner_from_parent: owner from parent
			define positive: direct_or_editor and owner_from_parent
			define negative: blocked and old
			define viewer: positive but not negative`

// TestExecuteW2_MergedExclusion proves the three-unit plan folds correctly across the full truth
// table of its units. Each unit is the boolean a single query returns: the merged UNION region
// (positiveUnion), the TTU self-join (ttu), and the merged INTERSECT region (negative). The DB
// decides each region's internal algebra, so the unit booleans are supplied directly; this test
// asserts the in-process fold positive=(union AND ttu), result=positive AND NOT negative.
func TestExecuteW2_MergedExclusion(t *testing.T) {
	// route maps a rendered query to the unit it belongs to: the TTU is the only self-join
	// (INNER JOIN); the negative region binds blocked/old; everything else is the positive union.
	route := func(positiveUnion, ttu, negative bool) func(sql string, args []any) bool {
		return func(sql string, args []any) bool {
			if strings.Contains(sql, "INNER JOIN") {
				return ttu
			}
			for _, a := range args {
				if s, ok := a.(string); ok && (s == "blocked" || s == "old") {
					return negative
				}
			}
			return positiveUnion
		}
	}

	cases := []struct {
		name                         string
		positiveUnion, ttu, negative bool
		expected                     bool
	}{
		// positive = union AND ttu; result = positive AND NOT negative.
		{"all_false_denies", false, false, false, false},
		{"union_only_denies", true, false, false, false},
		{"ttu_only_denies", false, true, false, false},
		{"positive_satisfied_not_negated_grants", true, true, false, true},
		{"positive_satisfied_but_negated_denies", true, true, true, false},
		{"negative_only_denies", false, false, true, false},
		{"union_and_negative_denies", true, false, true, false},
		{"ttu_and_negative_denies", false, true, true, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exec := w2Executor{boolRow: route(tc.positiveUnion, tc.ttu, tc.negative)}
			p := w2PlanFor(t, exec, mergedExclusionModel)
			require.Len(t, p.unit.multi, 3, "merged union + TTU + merged intersect")
			got, err := p.Execute(context.Background(), nil)
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}
}
