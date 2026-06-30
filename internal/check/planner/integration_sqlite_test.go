// These integration tests are the SQLite analogue of integration_pg_test.go / integration_mysql_test.go:
// they drive the planner end to end against a real SQLite database, proving the SQL the planner emits
// through the production SQLite adapter (pkg/storage/adapter/sqlite) returns correct authorization
// decisions when SQLite actually runs it.
//
// Unlike the Postgres and MySQL suites, this one carries NO `docker` build tag: SQLite runs in-process
// against a ":memory:" database (the modernc.org/sqlite driver, no CGO, no container), so it runs in
// the default `make test` lane. The case tables, models, and CEL bridge are shared from
// integration_shared_test.go, so SQLite walks the exact same scenarios as the two Docker-gated engines
// and must agree on every decision.
//
// A ":memory:" database is scoped to its single owning connection, so the migrations, the datastore
// that seeds tuples, and the adapter that queries them all share ONE *sql.DB pinned to a single
// connection (adapter/sqlite.Open sets MaxOpenConns(1)). The planner runs its leaf queries
// concurrently, but each goroutine holds the one connection only for the span of a single
// Execute→scan→Close, so they serialize on it rather than deadlock.
package planner

import (
	"context"
	"database/sql"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/pkg/storage/adapter"
	sqliteadapter "github.com/openfga/openfga/pkg/storage/adapter/sqlite"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	sqliteds "github.com/openfga/openfga/pkg/storage/sqlite"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
)

// sqliteEnv is the shared SQLite backing for the suite: a datastore used to seed tuples and the
// sqlite adapter the planner emits queries against. Both wrap the SAME in-memory *sql.DB, because a
// ":memory:" database is private to its connection — a second handle (or a second pooled connection)
// would see an empty, separate database.
type sqliteEnv struct {
	ds      *sqliteds.Datastore
	builder adapter.Builder
	db      *sql.DB
}

// setupSqliteEnv opens a fresh ":memory:" database, migrates it to the current schema, and returns an
// env whose datastore and adapter both ride that one pinned connection. The handle is closed via
// t.Cleanup. Each test gets its own database, so there is no shared-container cross-talk and no
// TestMain teardown is needed (unlike the Postgres/MySQL suites).
func setupSqliteEnv(t *testing.T) *sqliteEnv {
	t.Helper()

	// adapter/sqlite.Open pins the pool to a single connection (SetMaxOpenConns(1)) so the
	// in-memory database survives across every query and seed on this handle.
	builder, db, err := sqliteadapter.Open(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Migrate the in-memory schema on this same connection. goose needs the embedded migration FS
	// and the sqlite dialect; the dialect string matches what the datastore itself sets.
	goose.SetBaseFS(assets.EmbedMigrations)
	require.NoError(t, goose.SetDialect("sqlite"))
	require.NoError(t, goose.Up(db, assets.SqliteMigrationDir))

	ds, err := sqliteds.NewWithDB(db, sqlcommon.NewConfig())
	require.NoError(t, err)
	// The datastore shares the handle the adapter and migrations use; closing it would close that
	// shared handle, so the t.Cleanup above (one Close) is the single owner of the connection.

	return &sqliteEnv{ds: ds, builder: builder, db: db}
}

// run plans and executes the case against SQLite under a fresh store id (so cases sharing the
// in-memory database never collide) and returns the planner's decision. It mirrors pgEnv.run and
// mysqlEnv.run, differing only in the backing datastore and adapter.
func (e *sqliteEnv) run(t *testing.T, tc plannerCase) bool {
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

// runCases walks a shared case table against the SQLite env. Each case gets a fresh env (and thus a
// fresh in-memory database) so the cases are fully isolated.
func (e *sqliteEnv) runCases(t *testing.T, cases []plannerCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, e.run(t, tc))
		})
	}
}

// TestPlannerIntegrationSQLite_ConditionFree covers the HAVING path, where SQLite folds the whole set
// algebra and a returned row means granted.
func TestPlannerIntegrationSQLite_ConditionFree(t *testing.T) {
	setupSqliteEnv(t).runCases(t, conditionFreeCases(t))
}

// TestPlannerIntegrationSQLite_NestedConditionFree drives the complex nested tree end to end, the
// integration analogue of TestPlanSQL_NestedSetOperationsHaving.
func TestPlannerIntegrationSQLite_NestedConditionFree(t *testing.T) {
	setupSqliteEnv(t).runCases(t, nestedConditionFreeCases(t))
}

// TestPlannerIntegrationSQLite_NestedConditioned drives the conditioned nested tree through the
// gather path, the integration analogue of TestPlanSQL_NestedSetOperationsWithConditionsGather.
func TestPlannerIntegrationSQLite_NestedConditioned(t *testing.T) {
	setupSqliteEnv(t).runCases(t, nestedConditionedCases(t))
}

// TestPlannerIntegrationSQLite_WeightTwo drives weight-2 resolution paths — a single
// tuple-to-userset or userset hop — end to end against SQLite.
func TestPlannerIntegrationSQLite_WeightTwo(t *testing.T) {
	setupSqliteEnv(t).runCases(t, weightTwoCases(t))
}

// TestPlannerIntegrationSQLite_WeightTwoComplexBothLevels drives set operations across three weight-2
// hops with a hop-2 intersection inside from_folder, run end to end against SQLite.
func TestPlannerIntegrationSQLite_WeightTwoComplexBothLevels(t *testing.T) {
	setupSqliteEnv(t).runCases(t, weightTwoComplexBothLevelsCases(t))
}

// TestPlannerIntegrationSQLite_WeightTwoCondLeft drives the LEFT-side conditioned model against
// SQLite: each hop-1 edge carries a condition, the hop-2 relations do not.
func TestPlannerIntegrationSQLite_WeightTwoCondLeft(t *testing.T) {
	setupSqliteEnv(t).runCases(t, weightTwoCondLeftCases(t))
}

// TestPlannerIntegrationSQLite_WeightTwoCondRight drives the RIGHT-side conditioned model against
// SQLite: the hop-2 relations carry conditions, the hop-1 edges do not.
func TestPlannerIntegrationSQLite_WeightTwoCondRight(t *testing.T) {
	setupSqliteEnv(t).runCases(t, weightTwoCondRightCases(t))
}

// TestPlannerIntegrationSQLite_WeightTwoCondBoth drives the BOTH-sides conditioned model against
// SQLite: every traversal carries a condition on its hop-1 edge and its hop-2 relation.
func TestPlannerIntegrationSQLite_WeightTwoCondBoth(t *testing.T) {
	setupSqliteEnv(t).runCases(t, weightTwoCondBothCases(t))
}

// TestPlannerIntegrationSQLite_WeightTwoComplexMixed drives the combined mixed fixture against SQLite:
// TTU and userset hops across union/intersection/exclusion with conditions on left, right, and both.
func TestPlannerIntegrationSQLite_WeightTwoComplexMixed(t *testing.T) {
	setupSqliteEnv(t).runCases(t, complexWeightTwoMixedCases(t))
}

// TestPlannerIntegrationSQLite_MergedExclusion drives the merged-region exclusion model end to end
// against SQLite, proving the three compiled units fold to the correct decision across the model's
// truth table.
func TestPlannerIntegrationSQLite_MergedExclusion(t *testing.T) {
	setupSqliteEnv(t).runCases(t, mergedExclusionCases(t))
}

// TestPlannerIntegrationSQLite_Conditioned covers the gather path: the plan mentions an ABAC
// condition, so SQLite only scans candidate tuples and the planner folds the set algebra in process
// after evaluating CEL.
func TestPlannerIntegrationSQLite_Conditioned(t *testing.T) {
	setupSqliteEnv(t).runCases(t, conditionedCases(t))
}

// TestPlannerIntegrationSQLite_StaleConditionOnConditionFreeRelation is the SQLite analogue of the
// Postgres/MySQL test of the same name: a stale conditioned tuple on a plain `[user]` relation must
// not satisfy the condition-free (HAVING) plan, because the count atom matches only unconditioned
// tuples.
func TestPlannerIntegrationSQLite_StaleConditionOnConditionFreeRelation(t *testing.T) {
	setupSqliteEnv(t).runCases(t, staleConditionCases(t))
}
