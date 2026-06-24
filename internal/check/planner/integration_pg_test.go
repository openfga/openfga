//go:build docker

// These integration tests drive the planner end to end against a real PostgreSQL
// database in a test container. Each case turns a model DSL into a weighted graph, plans a
// Check with the production pg adapter (pkg/storage/adapter/pg), seeds tuples through the
// real datastore, executes the plan, and asserts the boolean decision.
//
// The planner's other tests pin plan-tree shape and golden SQL against a fake builder; this
// suite closes the remaining gap by proving the emitted SQL returns correct authorization
// decisions when Postgres actually runs it — including the conditioned gather path (real
// CEL evaluation) and the unreachable-subject path (no query at all).
//
// Gated behind the `docker` build tag so the planner package stays Docker-free in the
// `make test-unit` / `make test` lanes; run with `go test -tags=docker ./internal/check/planner/...`.
package planner

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/condition/eval"
	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/pg"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

const objectID = "1"

// pgEnv is the shared PostgreSQL backing for the suite: a datastore used to seed tuples and
// the pg adapter the planner emits queries against. Both point at the same container, which
// storagefixtures bootstraps once and tears down in TestMain.
type pgEnv struct {
	ds      *postgres.Datastore
	builder adapter.Builder
	pool    *pgxpool.Pool
}

// setupPgEnv boots (or joins) the shared Postgres container and returns an env wired to it.
// The datastore and adapter pool are closed via t.Cleanup.
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

// plannerCase is one end-to-end scenario: a model, the tuples to seed, the Check to plan,
// and the expected decision. The requestContext field supplies condition parameters for the
// gather (ABAC) path; it is nil for condition-free cases.
type plannerCase struct {
	name           string
	model          string
	objectType     string
	relation       string
	subject        string
	tuples         []*openfgav1.TupleKey
	requestContext map[string]any
	expected       bool
}

// run plans and executes the case against Postgres under a fresh store id (so cases sharing
// the container's database never collide) and returns the planner's decision.
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

// conditionEvaluator bridges the planner's ConditionEvaluator to the real CEL evaluator. It
// reuses the conditions already compiled by the TypeSystem and merges the request context
// (built once per case) with each tuple's stored context.
type conditionEvaluator struct {
	conditions map[string]*condition.EvaluableCondition
	requestCtx *structpb.Struct
}

func newConditionEvaluator(t *testing.T, ts *typesystem.TypeSystem, requestContext map[string]any) *conditionEvaluator {
	t.Helper()
	var reqCtx *structpb.Struct
	if requestContext != nil {
		reqCtx = testutils.MustNewStruct(t, requestContext)
	}
	return &conditionEvaluator{conditions: ts.GetConditions(), requestCtx: reqCtx}
}

// Eval decodes the gather scan's marshalled condition context and evaluates the named
// condition's CEL against the merged request + tuple context. An empty name is the
// unconditioned sentinel and always passes.
func (e *conditionEvaluator) Eval(ctx context.Context, name string, condCtx []byte) (bool, error) {
	if name == "" {
		return true, nil
	}
	c, ok := e.conditions[name]
	if !ok {
		return false, fmt.Errorf("condition %q not in model", name)
	}

	// condition_context is stored as a protobuf-marshalled structpb.Struct (see
	// sqlcommon.MarshalRelationshipCondition); the gather scan returns those bytes verbatim.
	var tupleCtx structpb.Struct
	if len(condCtx) > 0 {
		if err := proto.Unmarshal(condCtx, &tupleCtx); err != nil {
			return false, fmt.Errorf("unmarshal condition context for %q: %w", name, err)
		}
	}

	tk := &openfgav1.TupleKey{Condition: &openfgav1.RelationshipCondition{Name: name, Context: &tupleCtx}}
	return eval.EvaluateTupleCondition(ctx, tk, c, e.requestCtx)
}

// TestPlannerIntegrationPostgres_ConditionFree covers the HAVING path, where Postgres folds
// the whole set algebra and a returned row means granted.
func TestPlannerIntegrationPostgres_ConditionFree(t *testing.T) {
	env := setupPgEnv(t)

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

// nestedModel is the complex condition-free rewrite tree mirrored from
// TestPlanSQL_NestedSetOperationsHaving: five direct leaves combined through all three set
// operators, which the planner flattens and folds into a single HAVING clause shaped
//
//	viewer = (((direct_a OR direct_b) AND (direct_c AND direct_d)) AND NOT direct_e)
//
// Each case below seeds a different leaf combination so the suite walks the truth table of
// that expression against a live Postgres fold, proving the emitted HAVING returns the
// boolean the set algebra dictates.
const nestedModel = `
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

// grant is a small helper for a viewer-granting tuple on the shared object: it pairs a
// direct relation with user:alice so the nested cases read as the leaf set they seed.
func grant(relation string) *openfgav1.TupleKey {
	return tuple.NewTupleKey("document:"+objectID, relation, "user:alice")
}

// TestPlannerIntegrationPostgres_NestedConditionFree drives the complex nested tree end to
// end against Postgres. It is the integration analogue of the unit test
// TestPlanSQL_NestedSetOperationsHaving: rather than pinning the SQL text, it seeds tuples
// for representative points in the expression's truth table and asserts the decision
// Postgres folds out of the HAVING clause.
func TestPlannerIntegrationPostgres_NestedConditionFree(t *testing.T) {
	env := setupPgEnv(t)

	cases := []plannerCase{
		{
			// editor via direct_a, approver fully satisfied, not blocked → granted.
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
			// editor via the alternate operand direct_b carries the union → granted.
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
			// editor satisfied, but approver's intersection is missing direct_d → denied.
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
			// approver satisfied, but neither editor operand is present → denied.
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
			// privileged is fully satisfied, but direct_e blocks it via the exclusion → denied.
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

// nestedCondModel is the same nested tree as nestedModel, but two leaves carry ABAC
// conditions (direct_a / direct_d), mirroring TestPlanSQL_NestedSetOperationsWithConditionsGather.
// Because conditions appear, the planner cannot fold in HAVING and instead gathers candidate
// tuples for in-process CEL — so these cases exercise the gather path through the full tree.
const nestedCondModel = `
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

// condGrant is a small helper for a conditioned direct-relation tuple on the shared object.
// The condition context is stored on the tuple itself, so the gather scan reads it back from
// the database and the case never supplies a request context.
func condGrant(relation, condition string, ctx map[string]any, t *testing.T) *openfgav1.TupleKey {
	t.Helper()
	return tuple.NewTupleKeyWithCondition("document:"+objectID, relation, "user:alice", condition,
		testutils.MustNewStruct(t, ctx))
}

// TestPlannerIntegrationPostgres_NestedConditioned drives the conditioned nested tree end to
// end. It is the integration analogue of TestPlanSQL_NestedSetOperationsWithConditionsGather:
// Postgres only scans the candidate tuples, and the planner folds the set algebra in process
// after evaluating CEL. Here each condition's parameters are stored on the tuple, so the CEL
// inputs come back from the database and no request context is supplied.
func TestPlannerIntegrationPostgres_NestedConditioned(t *testing.T) {
	env := setupPgEnv(t)

	cases := []plannerCase{
		{
			// Both conditions pass and the structure is satisfied → granted. editor comes from
			// the conditioned direct_a, approver from direct_c plus the conditioned direct_d.
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
			// cond_one fails for direct_a, but the unconditioned direct_b still carries editor,
			// and direct_d's cond_two passes → granted despite the failed condition.
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
			// cond_two fails for direct_d, the only direct_d tuple, so approver's intersection
			// collapses → denied even though editor is satisfied.
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
			// Everything passes and privileged is satisfied, but the unconditioned direct_e
			// blocks via the exclusion → denied.
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

// TestPlannerIntegrationPostgres_Conditioned covers the gather path: the plan mentions an
// ABAC condition, so Postgres only scans candidate tuples and the planner folds the set
// algebra in process after evaluating CEL. The request context drives the condition result.
func TestPlannerIntegrationPostgres_Conditioned(t *testing.T) {
	env := setupPgEnv(t)

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
			// The conditioned operand has no tuple; the unconditioned editor grant carries the
			// union. The condition still parameterizes the plan, so a request context is supplied.
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
