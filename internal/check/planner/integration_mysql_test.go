//go:build docker

// These integration tests are the MySQL analogue of integration_pg_test.go: they drive the planner
// end to end against a real MySQL database in a test container, proving the SQL the planner emits
// through the production MySQL adapter (pkg/storage/adapter/mysql) returns correct authorization
// decisions when MySQL actually runs it.
//
// The case tables, models, and CEL bridge are engine-agnostic and live in integration_shared_test.go
// (no build tag); this file adds only the MySQL-backed env and the tests that walk those shared
// cases, so Postgres, MySQL, and SQLite exercise the same scenarios and must agree.
//
// Gated behind the `docker` build tag alongside the rest of the planner integration suite; run with
// `go test -tags=docker ./internal/check/planner/...`.
package planner

import (
	"context"
	"database/sql"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage/adapter"
	mysqladapter "github.com/openfga/openfga/pkg/storage/adapter/mysql"
	mysqlds "github.com/openfga/openfga/pkg/storage/mysql"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/testutils"
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

// setupMysqlEnv boots (or joins) the shared MySQL container and returns an env wired to it. The
// datastore and adapter handle are closed via t.Cleanup. GetConnectionURI returns a DSN the
// go-sql-driver/mysql driver accepts directly, so it backs both the datastore and the adapter
// unchanged.
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
// container's database never collide) and returns the planner's decision. It mirrors pgEnv.run,
// differing only in the backing datastore and adapter.
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

// runCases walks a shared case table against the MySQL env.
func (e *mysqlEnv) runCases(t *testing.T, cases []plannerCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, e.run(t, tc))
		})
	}
}

// TestPlannerIntegrationMySQL_ConditionFree covers the HAVING path, where MySQL folds the whole set
// algebra and a returned row means granted.
func TestPlannerIntegrationMySQL_ConditionFree(t *testing.T) {
	setupMysqlEnv(t).runCases(t, conditionFreeCases(t))
}

// TestPlannerIntegrationMySQL_NestedConditionFree drives the complex nested tree end to end, the
// integration analogue of TestPlanSQL_NestedSetOperationsHaving.
func TestPlannerIntegrationMySQL_NestedConditionFree(t *testing.T) {
	setupMysqlEnv(t).runCases(t, nestedConditionFreeCases(t))
}

// TestPlannerIntegrationMySQL_NestedConditioned drives the conditioned nested tree through the
// gather path, the integration analogue of TestPlanSQL_NestedSetOperationsWithConditionsGather.
func TestPlannerIntegrationMySQL_NestedConditioned(t *testing.T) {
	setupMysqlEnv(t).runCases(t, nestedConditionedCases(t))
}

// TestPlannerIntegrationMySQL_WeightTwo drives weight-2 resolution paths — a single
// tuple-to-userset or userset hop — end to end against MySQL.
func TestPlannerIntegrationMySQL_WeightTwo(t *testing.T) {
	setupMysqlEnv(t).runCases(t, weightTwoCases(t))
}

// TestPlannerIntegrationMySQL_WeightTwoComplexBothLevels drives set operations across three
// weight-2 hops with a hop-2 intersection inside from_folder, run end to end against MySQL.
func TestPlannerIntegrationMySQL_WeightTwoComplexBothLevels(t *testing.T) {
	setupMysqlEnv(t).runCases(t, weightTwoComplexBothLevelsCases(t))
}

// TestPlannerIntegrationMySQL_WeightTwoCondLeft drives the LEFT-side conditioned model against
// MySQL: each hop-1 edge carries a condition, the hop-2 relations do not.
func TestPlannerIntegrationMySQL_WeightTwoCondLeft(t *testing.T) {
	setupMysqlEnv(t).runCases(t, weightTwoCondLeftCases(t))
}

// TestPlannerIntegrationMySQL_WeightTwoCondRight drives the RIGHT-side conditioned model against
// MySQL: the hop-2 relations carry conditions, the hop-1 edges do not.
func TestPlannerIntegrationMySQL_WeightTwoCondRight(t *testing.T) {
	setupMysqlEnv(t).runCases(t, weightTwoCondRightCases(t))
}

// TestPlannerIntegrationMySQL_WeightTwoCondBoth drives the BOTH-sides conditioned model against
// MySQL: every traversal carries a condition on its hop-1 edge and its hop-2 relation.
func TestPlannerIntegrationMySQL_WeightTwoCondBoth(t *testing.T) {
	setupMysqlEnv(t).runCases(t, weightTwoCondBothCases(t))
}

// TestPlannerIntegrationMySQL_WeightTwoComplexMixed drives the combined mixed fixture against MySQL:
// TTU and userset hops across union/intersection/exclusion with conditions on left, right, and both.
func TestPlannerIntegrationMySQL_WeightTwoComplexMixed(t *testing.T) {
	setupMysqlEnv(t).runCases(t, complexWeightTwoMixedCases(t))
}

// TestPlannerIntegrationMySQL_MergedExclusion drives the merged-region exclusion model end to end
// against MySQL, proving the three compiled units fold to the correct decision across the model's
// truth table.
func TestPlannerIntegrationMySQL_MergedExclusion(t *testing.T) {
	setupMysqlEnv(t).runCases(t, mergedExclusionCases(t))
}

// TestPlannerIntegrationMySQL_Conditioned covers the gather path: the plan mentions an ABAC
// condition, so MySQL only scans candidate tuples and the planner folds the set algebra in process
// after evaluating CEL.
func TestPlannerIntegrationMySQL_Conditioned(t *testing.T) {
	setupMysqlEnv(t).runCases(t, conditionedCases(t))
}

// TestPlannerIntegrationMySQL_StaleConditionOnConditionFreeRelation is the MySQL analogue of the
// Postgres test of the same name: a stale conditioned tuple on a plain `[user]` relation must not
// satisfy the condition-free (HAVING) plan, because the count atom matches only unconditioned
// tuples.
func TestPlannerIntegrationMySQL_StaleConditionOnConditionFreeRelation(t *testing.T) {
	setupMysqlEnv(t).runCases(t, staleConditionCases(t))
}
