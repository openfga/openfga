//go:build docker

// These integration tests drive the planner end to end against a real PostgreSQL database in a
// test container. Each case turns a model DSL into a weighted graph, plans a Check with the
// production pg adapter (pkg/storage/adapter/pg), seeds tuples through the real datastore,
// executes the plan, and asserts the boolean decision.
//
// The planner's other tests pin plan-tree shape and golden SQL against a fake builder; this suite
// closes the remaining gap by proving the emitted SQL returns correct authorization decisions when
// Postgres actually runs it. The case tables, models, and CEL bridge are engine-agnostic and live
// in integration_shared_test.go (no build tag); this file adds only the Postgres-backed env and the
// tests that walk those shared cases, so Postgres, MySQL, and SQLite exercise the same scenarios.
//
// Gated behind the `docker` build tag so the planner package stays Docker-free in the
// `make test-unit` / `make test` lanes; run with `go test -tags=docker ./internal/check/planner/...`.
package planner

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/pg"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
)

// pgEnv is the shared PostgreSQL backing for the suite: a datastore used to seed tuples and the
// pg adapter the planner emits queries against. Both point at the same container, which
// storagefixtures bootstraps once and tears down in TestMain.
type pgEnv struct {
	ds      *postgres.Datastore
	builder adapter.Builder
	pool    *pgxpool.Pool
}

// setupPgEnv boots (or joins) the shared Postgres container and returns an env wired to it. The
// datastore and adapter pool are closed via t.Cleanup.
func setupPgEnv(t *testing.T) *pgEnv {
	t.Helper()

	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")
	uri := testDatastore.GetConnectionURI(true)

	ds, err := postgres.New(uri, sqlcommon.NewConfig())
	require.NoError(t, err)
	t.Cleanup(ds.Close)

	builder, pool, err := pg.Open(context.Background(), uri)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	return &pgEnv{ds: ds, builder: builder, pool: pool}
}

// run plans and executes the case against Postgres under a fresh store id (so cases sharing the
// container's database never collide) and returns the planner's decision.
func (e *pgEnv) run(t *testing.T, tc plannerCase) bool {
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

// runCases walks a shared case table against the Postgres env.
func (e *pgEnv) runCases(t *testing.T, cases []plannerCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, e.run(t, tc))
		})
	}
}

// TestPlannerIntegrationPostgres_ConditionFree covers the HAVING path, where Postgres folds the
// whole set algebra and a returned row means granted.
func TestPlannerIntegrationPostgres_ConditionFree(t *testing.T) {
	setupPgEnv(t).runCases(t, conditionFreeCases(t))
}

// TestPlannerIntegrationPostgres_NestedConditionFree drives the complex nested tree end to end,
// the integration analogue of TestPlanSQL_NestedSetOperationsHaving.
func TestPlannerIntegrationPostgres_NestedConditionFree(t *testing.T) {
	setupPgEnv(t).runCases(t, nestedConditionFreeCases(t))
}

// TestPlannerIntegrationPostgres_NestedConditioned drives the conditioned nested tree through the
// gather path, the integration analogue of TestPlanSQL_NestedSetOperationsWithConditionsGather.
func TestPlannerIntegrationPostgres_NestedConditioned(t *testing.T) {
	setupPgEnv(t).runCases(t, nestedConditionedCases(t))
}

// TestPlannerIntegrationPostgres_WeightTwo drives weight-2 resolution paths — a single
// tuple-to-userset or userset hop — end to end against Postgres.
func TestPlannerIntegrationPostgres_WeightTwo(t *testing.T) {
	setupPgEnv(t).runCases(t, weightTwoCases(t))
}

// TestPlannerIntegrationPostgres_WeightTwoComplexBothLevels drives the complex combination of set
// operations across hops and within a hop end to end against Postgres.
func TestPlannerIntegrationPostgres_WeightTwoComplexBothLevels(t *testing.T) {
	setupPgEnv(t).runCases(t, weightTwoComplexBothLevelsCases(t))
}

// TestPlannerIntegrationPostgres_WeightTwoCondLeft drives the LEFT-side conditioned model: each
// hop-1 edge carries a condition, the hop-2 relations do not.
func TestPlannerIntegrationPostgres_WeightTwoCondLeft(t *testing.T) {
	setupPgEnv(t).runCases(t, weightTwoCondLeftCases(t))
}

// TestPlannerIntegrationPostgres_WeightTwoCondRight drives the RIGHT-side conditioned model: the
// hop-2 relations carry conditions, the hop-1 edges do not.
func TestPlannerIntegrationPostgres_WeightTwoCondRight(t *testing.T) {
	setupPgEnv(t).runCases(t, weightTwoCondRightCases(t))
}

// TestPlannerIntegrationPostgres_WeightTwoCondBoth drives the BOTH-sides conditioned model: every
// traversal carries a condition on its hop-1 edge and its hop-2 relation.
func TestPlannerIntegrationPostgres_WeightTwoCondBoth(t *testing.T) {
	setupPgEnv(t).runCases(t, weightTwoCondBothCases(t))
}

// TestPlannerIntegrationPostgres_WeightTwoComplexMixed drives the combined fixture: TTU and userset
// hops mixed across union/intersection/exclusion, with conditions on the left, the right, and both.
func TestPlannerIntegrationPostgres_WeightTwoComplexMixed(t *testing.T) {
	setupPgEnv(t).runCases(t, complexWeightTwoMixedCases(t))
}

// TestPlannerIntegrationPostgres_MergedExclusion drives the merged-region exclusion model end to
// end, proving the three compiled units (merged union, TTU self-join, merged intersect) fold to
// the correct decision across the model's truth table.
func TestPlannerIntegrationPostgres_MergedExclusion(t *testing.T) {
	setupPgEnv(t).runCases(t, mergedExclusionCases(t))
}

// TestPlannerIntegrationPostgres_Conditioned covers the gather path on weight-1 relations: the
// plan mentions an ABAC condition, so Postgres only scans candidate tuples and the planner folds
// the set algebra in process after evaluating CEL.
func TestPlannerIntegrationPostgres_Conditioned(t *testing.T) {
	setupPgEnv(t).runCases(t, conditionedCases(t))
}

// TestPlannerIntegrationPostgres_StaleConditionOnConditionFreeRelation guards a data-integrity
// edge: a conditioned tuple on a plain `[user]` relation must not satisfy the condition-free
// (HAVING) plan, because the count atom matches only unconditioned tuples. The stale row is seeded
// directly through the datastore, which writes at the storage layer without model validation.
func TestPlannerIntegrationPostgres_StaleConditionOnConditionFreeRelation(t *testing.T) {
	setupPgEnv(t).runCases(t, staleConditionCases(t))
}
