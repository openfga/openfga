//go:build docker

// These integration tests are the MySQL analogue of integration_pg_test.go: they drive the
// planner end to end against a real MySQL database in a test container, proving the SQL the
// planner emits through the production MySQL adapter (pkg/storage/adapter/mysql) returns
// correct authorization decisions when MySQL actually runs it.
//
// The shared scaffolding — the plannerCase shape, the conditionEvaluator bridge to real CEL,
// the grant/condGrant helpers, and the nested model fixtures — is declared in
// integration_pg_test.go (same package, same build tag) and reused verbatim here; this file
// adds only the MySQL-backed environment and the cases that run against it. The case tables
// mirror the Postgres suite so both engines walk the same scenarios.
//
// Gated behind the `docker` build tag alongside the rest of the planner integration suite;
// run with `go test -tags=docker ./internal/check/planner/...`.
package planner

import (
	"context"
	"database/sql"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage/adapter"
	mysqladapter "github.com/openfga/openfga/pkg/storage/adapter/mysql"
	mysqlds "github.com/openfga/openfga/pkg/storage/mysql"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// mysqlEnv is the shared MySQL backing for the suite: a datastore used to seed tuples and the
// mysql adapter the planner emits queries against. Both point at the same container, which
// storagefixtures bootstraps once and tears down in TestMain.
type mysqlEnv struct {
	ds      *mysqlds.Datastore
	builder adapter.Builder
	db      *sql.DB
}

// setupMysqlEnv boots (or joins) the shared MySQL container and returns an env wired to it.
// The datastore and adapter handle are closed via t.Cleanup. GetConnectionURI returns a DSN
// the go-sql-driver/mysql driver accepts directly, so it backs both the datastore and the
// adapter unchanged.
func setupMysqlEnv(t *testing.T) *mysqlEnv {
	t.Helper()

	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "mysql")
	uri := testDatastore.GetConnectionURI(true)

	ds, err := mysqlds.New(uri, sqlcommon.NewConfig())
	require.NoError(t, err)
	t.Cleanup(ds.Close)

	builder, db, err := mysqladapter.Open(uri)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	return &mysqlEnv{ds: ds, builder: builder, db: db}
}

// run plans and executes the case against MySQL under a fresh store id (so cases sharing the
// container's database never collide) and returns the planner's decision. It mirrors
// pgEnv.run, differing only in the backing datastore and adapter.
func (e *mysqlEnv) run(t *testing.T, tc plannerCase) bool {
	t.Helper()
	ctx := context.Background()
	store := ulid.Make().String()

	ts, err := typesystem.New(testutils.MustTransformDSLToProtoWithID(tc.model))
	require.NoError(t, err)
	g := ts.GetWeightedGraph()
	require.NotNil(t, g)

	if len(tc.tuples) > 0 {
		require.NoError(t, e.ds.Write(ctx, store, nil, tc.tuples))
	}

	plan, err := New(e.builder).Plan(g, store, tc.objectType, objectID, tc.relation, tc.subject)
	require.NoError(t, err)

	evaluator := newConditionEvaluator(t, ts, tc.requestContext)
	got, err := plan.Execute(ctx, evaluator)
	require.NoError(t, err)
	return got
}

// TestPlannerIntegrationMySQL_ConditionFree covers the HAVING path, where MySQL folds the
// whole set algebra and a returned row means granted.
func TestPlannerIntegrationMySQL_ConditionFree(t *testing.T) {
	env := setupMysqlEnv(t)

	cases := []plannerCase{
		{
			name: "direct_grant",
			model: `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: [user]`,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:"+objectID, "viewer", "user:alice"),
			},
			expected: true,
		},
		{
			name: "direct_deny_no_tuple",
			model: `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: [user]`,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			expected:   false,
		},
		{
			name: "union_via_operand",
			model: `
				model
					schema 1.1
				type user
				type document
					relations
						define editor: [user]
						define viewer: [user] or editor`,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:"+objectID, "editor", "user:alice"),
			},
			expected: true,
		},
		{
			name: "intersection_both_present",
			model: `
				model
					schema 1.1
				type user
				type document
					relations
						define author: [user]
						define owner: [user] and author`,
			objectType: "document",
			relation:   "owner",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:"+objectID, "owner", "user:alice"),
				tuple.NewTupleKey("document:"+objectID, "author", "user:alice"),
			},
			expected: true,
		},
		{
			name: "intersection_missing_operand",
			model: `
				model
					schema 1.1
				type user
				type document
					relations
						define author: [user]
						define owner: [user] and author`,
			objectType: "document",
			relation:   "owner",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:"+objectID, "owner", "user:alice"),
			},
			expected: false,
		},
		{
			name: "exclusion_grants_when_not_subtracted",
			model: `
				model
					schema 1.1
				type user
				type document
					relations
						define banned: [user]
						define viewer: [user] but not banned`,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:"+objectID, "viewer", "user:alice"),
			},
			expected: true,
		},
		{
			name: "exclusion_denies_when_subtracted",
			model: `
				model
					schema 1.1
				type user
				type document
					relations
						define banned: [user]
						define viewer: [user] but not banned`,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:"+objectID, "viewer", "user:alice"),
				tuple.NewTupleKey("document:"+objectID, "banned", "user:alice"),
			},
			expected: false,
		},
		{
			name: "wildcard_public_access",
			model: `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer: [user, user:*]`,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:bob",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:"+objectID, "viewer", "user:*"),
			},
			expected: true,
		},
		{
			name: "unreachable_subject_type",
			model: `
				model
					schema 1.1
				type user
				type employee
				type document
					relations
						define viewer: [user]`,
			objectType: "document",
			relation:   "viewer",
			subject:    "employee:e1",
			expected:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, env.run(t, tc))
		})
	}
}

// TestPlannerIntegrationMySQL_NestedConditionFree drives the complex nested tree end to end
// against MySQL, the integration analogue of TestPlanSQL_NestedSetOperationsHaving. It seeds
// tuples for representative points in the expression's truth table and asserts the decision
// MySQL folds out of the HAVING clause.
func TestPlannerIntegrationMySQL_NestedConditionFree(t *testing.T) {
	env := setupMysqlEnv(t)

	cases := []plannerCase{
		{
			name:       "grant_via_direct_a",
			model:      nestedModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				grant("direct_a"), grant("direct_c"), grant("direct_d"),
			},
			expected: true,
		},
		{
			name:       "grant_via_direct_b",
			model:      nestedModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				grant("direct_b"), grant("direct_c"), grant("direct_d"),
			},
			expected: true,
		},
		{
			name:       "deny_incomplete_approver",
			model:      nestedModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				grant("direct_a"), grant("direct_c"),
			},
			expected: false,
		},
		{
			name:       "deny_empty_editor",
			model:      nestedModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				grant("direct_c"), grant("direct_d"),
			},
			expected: false,
		},
		{
			name:       "deny_blocked_by_exclusion",
			model:      nestedModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				grant("direct_a"), grant("direct_b"),
				grant("direct_c"), grant("direct_d"),
				grant("direct_e"),
			},
			expected: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, env.run(t, tc))
		})
	}
}

// TestPlannerIntegrationMySQL_NestedConditioned drives the conditioned nested tree end to end
// against MySQL, the integration analogue of
// TestPlanSQL_NestedSetOperationsWithConditionsGather: MySQL only scans the candidate tuples,
// and the planner folds the set algebra in process after evaluating CEL. Each condition's
// parameters are stored on the tuple, so the CEL inputs come back from the database and no
// request context is supplied.
func TestPlannerIntegrationMySQL_NestedConditioned(t *testing.T) {
	env := setupMysqlEnv(t)

	cases := []plannerCase{
		{
			name:       "grant_both_conditions_pass",
			model:      nestedCondModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				condGrant("direct_a", "cond_one", map[string]any{"x": 1}, t),
				grant("direct_c"),
				condGrant("direct_d", "cond_two", map[string]any{"y": "ok"}, t),
			},
			expected: true,
		},
		{
			name:       "grant_via_unconditioned_direct_b",
			model:      nestedCondModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				condGrant("direct_a", "cond_one", map[string]any{"x": -1}, t),
				grant("direct_b"),
				grant("direct_c"),
				condGrant("direct_d", "cond_two", map[string]any{"y": "ok"}, t),
			},
			expected: true,
		},
		{
			name:       "deny_conditioned_approver_operand_fails",
			model:      nestedCondModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				condGrant("direct_a", "cond_one", map[string]any{"x": 1}, t),
				grant("direct_c"),
				condGrant("direct_d", "cond_two", map[string]any{"y": "no"}, t),
			},
			expected: false,
		},
		{
			name:       "deny_blocked_by_exclusion",
			model:      nestedCondModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				condGrant("direct_a", "cond_one", map[string]any{"x": 1}, t),
				grant("direct_c"),
				condGrant("direct_d", "cond_two", map[string]any{"y": "ok"}, t),
				grant("direct_e"),
			},
			expected: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, env.run(t, tc))
		})
	}
}

// TestPlannerIntegrationMySQL_WeightTwo drives weight-2 resolution paths — a single
// tuple-to-userset or userset hop — end to end against MySQL, proving the emitted self-join
// SQL returns the decision the two-hop relationship dictates when MySQL runs it.
func TestPlannerIntegrationMySQL_WeightTwo(t *testing.T) {
	env := setupMysqlEnv(t)

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

	cases := []plannerCase{
		{
			name:       "ttu_grant",
			model:      ttuModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:"+objectID, "parent", "folder:f1"),
				tuple.NewTupleKey("folder:f1", "admin", "user:alice"),
			},
			expected: true,
		},
		{
			name:       "ttu_deny_no_inner_grant",
			model:      ttuModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:"+objectID, "parent", "folder:f1"),
			},
			expected: false,
		},
		{
			name:       "userset_grant",
			model:      usersetModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:"+objectID, "viewer", "group:g1#member"),
				tuple.NewTupleKey("group:g1", "member", "user:alice"),
			},
			expected: true,
		},
		{
			name:       "userset_deny_not_member",
			model:      usersetModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:"+objectID, "viewer", "group:g1#member"),
				tuple.NewTupleKey("group:g1", "member", "user:bob"),
			},
			expected: false,
		},
		{
			name: "mixed_weight_union_grant_via_hop",
			model: `
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]
				type document
					relations
						define viewer: [user, group#member]`,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:"+objectID, "viewer", "group:g1#member"),
				tuple.NewTupleKey("group:g1", "member", "user:alice"),
			},
			expected: true,
		},
		{
			// Hop-2 intersection satisfied by one folder holding both grants → granted.
			name: "ttu_hop2_intersection_same_object_grants",
			model: `
				model
					schema 1.1
				type user
				type folder
					relations
						define admin: [user]
						define editor: [user]
						define grant: admin and editor
				type document
					relations
						define parent: [folder]
						define viewer: grant from parent`,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:"+objectID, "parent", "folder:f1"),
				tuple.NewTupleKey("folder:f1", "admin", "user:alice"),
				tuple.NewTupleKey("folder:f1", "editor", "user:alice"),
			},
			expected: true,
		},
		{
			// Hop-2 intersection operands split across two parent folders → denied.
			name: "ttu_hop2_intersection_split_objects_denies",
			model: `
				model
					schema 1.1
				type user
				type folder
					relations
						define admin: [user]
						define editor: [user]
						define grant: admin and editor
				type document
					relations
						define parent: [folder]
						define viewer: grant from parent`,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:"+objectID, "parent", "folder:f1"),
				tuple.NewTupleKey("document:"+objectID, "parent", "folder:f2"),
				tuple.NewTupleKey("folder:f1", "admin", "user:alice"),
				tuple.NewTupleKey("folder:f2", "editor", "user:alice"),
			},
			expected: false,
		},
		{
			// Hop-2 exclusion: admin and not banned on the parent folder → granted.
			name: "ttu_hop2_exclusion_grants",
			model: `
				model
					schema 1.1
				type user
				type folder
					relations
						define admin: [user]
						define banned: [user]
						define grant: admin but not banned
				type document
					relations
						define parent: [folder]
						define viewer: grant from parent`,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:"+objectID, "parent", "folder:f1"),
				tuple.NewTupleKey("folder:f1", "admin", "user:alice"),
			},
			expected: true,
		},
		{
			// Same exclusion, subject banned on that folder → denied.
			name: "ttu_hop2_exclusion_banned_denies",
			model: `
				model
					schema 1.1
				type user
				type folder
					relations
						define admin: [user]
						define banned: [user]
						define grant: admin but not banned
				type document
					relations
						define parent: [folder]
						define viewer: grant from parent`,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:"+objectID, "parent", "folder:f1"),
				tuple.NewTupleKey("folder:f1", "admin", "user:alice"),
				tuple.NewTupleKey("folder:f1", "banned", "user:alice"),
			},
			expected: false,
		},
		{
			name: "userset_conditioned_hop_grants",
			model: `
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]
				type document
					relations
						define viewer: [group#member with non_negative]
				condition non_negative(x: int) {
					x >= 0
				}`,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:"+objectID, "viewer", "group:g1#member", "non_negative", nil),
				tuple.NewTupleKey("group:g1", "member", "user:alice"),
			},
			requestContext: map[string]any{"x": 1},
			expected:       true,
		},
		{
			name: "userset_conditioned_hop_denies",
			model: `
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]
				type document
					relations
						define viewer: [group#member with non_negative]
				condition non_negative(x: int) {
					x >= 0
				}`,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:"+objectID, "viewer", "group:g1#member", "non_negative", nil),
				tuple.NewTupleKey("group:g1", "member", "user:alice"),
			},
			requestContext: map[string]any{"x": -1},
			expected:       false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, env.run(t, tc))
		})
	}
}

// TestPlannerIntegrationMySQL_WeightTwoComplexBothLevels is the MySQL analogue of the
// Postgres complex-both-levels test: set operations across three weight-2 hops
// ((from_folder AND from_org) BUT NOT from_blocked) with a hop-2 intersection inside
// from_folder (admin AND editor on the same folder), run end to end against MySQL.
func TestPlannerIntegrationMySQL_WeightTwoComplexBothLevels(t *testing.T) {
	env := setupMysqlEnv(t)

	links := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:"+objectID, "parent", "folder:f1"),
		tuple.NewTupleKey("document:"+objectID, "org", "org:o1"),
		tuple.NewTupleKey("document:"+objectID, "blocked", "folder:b1"),
	}
	with := func(extra ...*openfgav1.TupleKey) []*openfgav1.TupleKey {
		return append(append([]*openfgav1.TupleKey{}, links...), extra...)
	}

	cases := []plannerCase{
		{
			name:       "all_satisfied_not_blocked_grants",
			model:      complexWeightTwoModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: with(
				tuple.NewTupleKey("folder:f1", "admin", "user:alice"),
				tuple.NewTupleKey("folder:f1", "editor", "user:alice"),
				tuple.NewTupleKey("org:o1", "owner", "user:alice"),
			),
			expected: true,
		},
		{
			name:       "hop2_intersection_incomplete_denies",
			model:      complexWeightTwoModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: with(
				tuple.NewTupleKey("folder:f1", "admin", "user:alice"),
				tuple.NewTupleKey("org:o1", "owner", "user:alice"),
			),
			expected: false,
		},
		{
			name:       "outer_intersection_missing_org_denies",
			model:      complexWeightTwoModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: with(
				tuple.NewTupleKey("folder:f1", "admin", "user:alice"),
				tuple.NewTupleKey("folder:f1", "editor", "user:alice"),
			),
			expected: false,
		},
		{
			name:       "privileged_but_blocked_denies",
			model:      complexWeightTwoModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: with(
				tuple.NewTupleKey("folder:f1", "admin", "user:alice"),
				tuple.NewTupleKey("folder:f1", "editor", "user:alice"),
				tuple.NewTupleKey("org:o1", "owner", "user:alice"),
				tuple.NewTupleKey("folder:b1", "editor", "user:alice"),
			),
			expected: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, env.run(t, tc))
		})
	}
}

// TestPlannerIntegrationMySQL_Conditioned covers the gather path: the plan mentions an ABAC
// condition, so MySQL only scans candidate tuples and the planner folds the set algebra in
// process after evaluating CEL. The request context drives the condition result.
func TestPlannerIntegrationMySQL_Conditioned(t *testing.T) {
	env := setupMysqlEnv(t)

	const condModel = `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user with non_negative]
		condition non_negative(x: int) {
			x >= 0
		}`

	cases := []plannerCase{
		{
			name:       "condition_passes_grants",
			model:      condModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:"+objectID, "viewer", "user:alice", "non_negative", nil),
			},
			requestContext: map[string]any{"x": 1},
			expected:       true,
		},
		{
			name:       "condition_fails_denies",
			model:      condModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:"+objectID, "viewer", "user:alice", "non_negative", nil),
			},
			requestContext: map[string]any{"x": -1},
			expected:       false,
		},
		{
			name:       "condition_context_from_tuple_grants",
			model:      condModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:"+objectID, "viewer", "user:alice", "non_negative",
					testutils.MustNewStruct(t, map[string]any{"x": 5})),
			},
			expected: true,
		},
		{
			name: "union_conditioned_and_unconditioned_grants_via_unconditioned",
			model: `
				model
					schema 1.1
				type user
				type document
					relations
						define editor: [user]
						define viewer: [user with non_negative] or editor
				condition non_negative(x: int) {
					x >= 0
				}`,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:"+objectID, "editor", "user:alice"),
			},
			requestContext: map[string]any{"x": -1},
			expected:       true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, env.run(t, tc))
		})
	}
}
