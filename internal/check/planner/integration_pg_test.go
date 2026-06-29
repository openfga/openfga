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

// condLink builds a conditioned hop-1 link tuple on the shared document object — the LEFT side
// of a weight-2 traversal; the object argument is the linked intermediate (e.g. "folder:f1" for
// a TTU tupleset, or "group:g1#member" for a userset). The condition's parameters come from the
// case's requestContext, so the stored tuple context is nil.
func condLink(relation, object, condition string) *openfgav1.TupleKey {
	return tuple.NewTupleKeyWithCondition("document:"+objectID, relation, object, condition, nil)
}

// condHop2 builds a conditioned hop-2 tuple on an intermediate object — the RIGHT side of a
// weight-2 traversal — granting user:alice the inner relation under the given condition. Like
// condLink, parameters come from the case's requestContext.
func condHop2(object, relation, condition string) *openfgav1.TupleKey {
	return tuple.NewTupleKeyWithCondition(object, relation, "user:alice", condition, nil)
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

// TestPlannerIntegrationPostgres_WeightTwo drives weight-2 resolution paths — a single
// tuple-to-userset or userset hop — end to end against Postgres. Each hop compiles to a
// self-join (or, when conditioned, a self-join gather), so these cases prove the emitted
// join SQL returns the decision the two-hop relationship dictates when Postgres runs it.
func TestPlannerIntegrationPostgres_WeightTwo(t *testing.T) {
	env := setupPgEnv(t)

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
			// document:1 parent folder:f1; alice is admin of folder:f1 → granted via TTU.
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
			// The parent link exists but alice is not the folder's admin → denied.
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
			// admin grant on a different folder than the one document:1 points at → denied.
			name:       "ttu_deny_wrong_parent",
			model:      ttuModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:"+objectID, "parent", "folder:f1"),
				tuple.NewTupleKey("folder:f2", "admin", "user:alice"),
			},
			expected: false,
		},
		{
			// document:1 viewer userset group:g1#member; alice is a member of g1 → granted.
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
			// The userset is wired but alice is not a member of the group → denied.
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
			// Multiple parent types: admin from parent over [folder, org], granted via org.
			name: "ttu_multiple_parent_types_grant_via_org",
			model: `
				model
					schema 1.1
				type user
				type folder
					relations
						define admin: [user]
				type org
					relations
						define admin: [user]
				type document
					relations
						define parent: [folder, org]
						define viewer: admin from parent`,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:"+objectID, "parent", "org:o1"),
				tuple.NewTupleKey("org:o1", "admin", "user:alice"),
			},
			expected: true,
		},
		{
			// A weight-1 direct grant unioned with a weight-2 userset hop: granted via the hop.
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
			// Hop-2 intersection (admin and editor on the folder), satisfied by one folder
			// holding both grants for the subject → granted. Exercises GROUP BY/HAVING.
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
			// Same hop-2 intersection, but the two grants are on DIFFERENT folders, both
			// parents of the document. No single folder satisfies the intersection → denied.
			// This is the per-object-fold correctness case.
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
			// Hop-2 exclusion (admin but not banned on the folder); the subject is admin and
			// not banned on the parent folder → granted.
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
			// Same exclusion, but the subject is banned on that folder → denied.
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
			// Conditioned userset hop: the hop-1 (viewer) tuple carries a condition that passes.
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
			// Same conditioned hop, but the condition fails → denied.
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

// complexWeightTwoModel combines set operations at both levels of weight-2 resolution: the
// outer relation intersects two weight-2 hops and excludes a third, and the first hop's
// hop-2 relation (grant) is itself an intersection on the folder:
//
//	viewer = (from_folder AND from_org) BUT NOT from_blocked
//	  from_folder  = grant from parent     (grant = admin AND editor on the folder)
//	  from_org     = owner from org
//	  from_blocked = editor from blocked
const complexWeightTwoModel = `
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

// TestPlannerIntegrationPostgres_WeightTwoComplexBothLevels drives the complex combination
// of set operations across hops and within a hop end to end against Postgres. It walks
// representative points of (from_folder AND from_org) BUT NOT from_blocked, where from_folder
// additionally requires admin AND editor on the same folder — proving the per-object
// GROUP BY/HAVING fold and the in-process outer fold agree with the relationship semantics
// when a real database runs the SQL.
func TestPlannerIntegrationPostgres_WeightTwoComplexBothLevels(t *testing.T) {
	env := setupPgEnv(t)

	// Tuple sets reused across cases: the parent/org/blocked links are fixed; what varies is
	// which inner grants exist on the linked objects.
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
			// from_folder: f1 has admin AND editor → grant. from_org: o1 owner. not blocked.
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
			// from_folder fails: f1 has admin but not editor, so grant (admin AND editor) is
			// not satisfied on the folder → intersection false → denied.
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
			// from_org fails: no owner on o1 → outer intersection false → denied.
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
			// Everything for privileged holds, but the subject is editor on the blocked folder
			// b1, so from_blocked holds and the exclusion denies.
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

// The next four fixtures extend weight-2 coverage to conditioned traversals that combine a TTU
// hop and a userset hop in the same set-operation tree, placing ABAC conditions on the LEFT
// (the hop-1 edge), the RIGHT (the hop-2 relation on the intermediate type), and BOTH. They are
// the integration analogues of the conditioned weight-2 unit tests, proving the join SQL the
// planner emits returns correct decisions when a real database runs it.

// weightTwoCondLeftModel conditions only the hop-1 edges: the TTU's tupleset edge
// (`parent: [folder with cond_parent]`) and the userset edge
// (`from_members: [group#member with cond_members]`). The hop-2 relations (folder#admin,
// group#member) are unconditioned. The outer relation intersects the two hops.
const weightTwoCondLeftModel = `
	model
		schema 1.1
	type user
	type folder
		relations
			define admin: [user]
	type group
		relations
			define member: [user]
	type document
		relations
			define parent: [folder with cond_parent]
			define from_folder: admin from parent
			define from_members: [group#member with cond_members]
			define viewer: from_folder and from_members
	condition cond_parent(p_parent: int) {
		p_parent > 0
	}
	condition cond_members(p_members: string) {
		p_members == "active"
	}`

// weightTwoCondRightModel conditions only the hop-2 relations on the intermediate types
// (folder#admin, group#member); the hop-1 edges are unconditioned. The outer relation unions
// the two hops so each side's hop-2 condition can be proven in isolation.
const weightTwoCondRightModel = `
	model
		schema 1.1
	type user
	type folder
		relations
			define admin: [user with cond_admin]
	type group
		relations
			define member: [user with cond_member]
	type document
		relations
			define parent: [folder]
			define from_folder: admin from parent
			define from_members: [group#member]
			define viewer: from_folder or from_members
	condition cond_admin(p_admin: int) {
		p_admin > 0
	}
	condition cond_member(p_member: string) {
		p_member == "active"
	}`

// weightTwoCondBothModel conditions BOTH the hop-1 edge and the hop-2 relation of each
// traversal: the TTU carries cond_parent on parent and cond_admin on folder#admin; the userset
// carries cond_members on the link and cond_member on group#member. The outer relation
// intersects the two hops, so a single failed condition anywhere denies.
const weightTwoCondBothModel = `
	model
		schema 1.1
	type user
	type folder
		relations
			define admin: [user with cond_admin]
	type group
		relations
			define member: [user with cond_member]
	type document
		relations
			define parent: [folder with cond_parent]
			define from_folder: admin from parent
			define from_members: [group#member with cond_members]
			define viewer: from_folder and from_members
	condition cond_parent(p_parent: int) {
		p_parent > 0
	}
	condition cond_members(p_members: string) {
		p_members == "active"
	}
	condition cond_admin(p_admin: int) {
		p_admin > 0
	}
	condition cond_member(p_member: string) {
		p_member == "active"
	}`

// complexWeightTwoMixedModel is the combined fixture: it mixes a TTU hop and a userset hop
// across all three set operators, with conditions on the left, the right, and both, plus a
// hop-2 intersection (grant = admin AND editor, exercising the per-object fold) and an
// exclusion arm. Shaped:
//
//	viewer = ((from_folder AND from_org) AND from_members) BUT NOT from_blocked
//	  from_folder  = grant from parent          (TTU, hop-1 cond_parent + hop-2 cond_admin on admin)
//	  from_org      = owner from org             (TTU, unconditioned control)
//	  from_members  = [group#member with cond]   (userset, hop-1 cond_members + hop-2 cond_member)
//	  from_blocked  = editor from blocked        (TTU exclusion arm, unconditioned)
const complexWeightTwoMixedModel = `
	model
		schema 1.1
	type user
	type folder
		relations
			define admin: [user with cond_admin]
			define editor: [user]
			define grant: admin and editor
	type org
		relations
			define owner: [user]
	type group
		relations
			define member: [user with cond_member]
	type document
		relations
			define parent: [folder with cond_parent]
			define org: [org]
			define blocked: [folder]
			define from_folder: grant from parent
			define from_org: owner from org
			define from_members: [group#member with cond_members]
			define from_blocked: editor from blocked
			define privileged: (from_folder and from_org) and from_members
			define viewer: privileged but not from_blocked
	condition cond_parent(p_parent: int) {
		p_parent > 0
	}
	condition cond_admin(p_admin: int) {
		p_admin > 0
	}
	condition cond_members(p_members: string) {
		p_members == "active"
	}
	condition cond_member(p_member: string) {
		p_member == "active"
	}`

// TestPlannerIntegrationPostgres_WeightTwoCondLeft drives the LEFT-side conditioned model: each
// hop-1 edge (the TTU tupleset and the userset link) carries a condition, while the hop-2
// relations are unconditioned. Postgres takes the gather (self-join) path and the hop-1
// condition gates the whole intermediate object, so a failed condition on either edge denies
// the intersection.
func TestPlannerIntegrationPostgres_WeightTwoCondLeft(t *testing.T) {
	env := setupPgEnv(t)
	for _, tc := range weightTwoCondLeftCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, env.run(t, tc))
		})
	}
}

// TestPlannerIntegrationPostgres_WeightTwoCondRight drives the RIGHT-side conditioned model:
// the hop-2 relations on the intermediate types carry conditions, the hop-1 edges do not. The
// outer union lets each side's hop-2 condition be proven independently.
func TestPlannerIntegrationPostgres_WeightTwoCondRight(t *testing.T) {
	env := setupPgEnv(t)
	for _, tc := range weightTwoCondRightCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, env.run(t, tc))
		})
	}
}

// TestPlannerIntegrationPostgres_WeightTwoCondBoth drives the BOTH-sides conditioned model:
// every traversal carries a condition on its hop-1 edge and another on its hop-2 relation, and
// the outer intersection denies if any single condition fails.
func TestPlannerIntegrationPostgres_WeightTwoCondBoth(t *testing.T) {
	env := setupPgEnv(t)
	for _, tc := range weightTwoCondBothCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, env.run(t, tc))
		})
	}
}

// TestPlannerIntegrationPostgres_WeightTwoComplexMixed drives the combined fixture: TTU and
// userset hops mixed across union/intersection/exclusion, with conditions on the left, the
// right, and both, plus a hop-2 intersection that must be satisfied by a single folder.
func TestPlannerIntegrationPostgres_WeightTwoComplexMixed(t *testing.T) {
	env := setupPgEnv(t)
	for _, tc := range complexWeightTwoMixedCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, env.run(t, tc))
		})
	}
}

// weightTwoCondLeftCases returns the shared case table for the LEFT-side conditioned model,
// reused by both the Postgres and MySQL suites.
func weightTwoCondLeftCases(t *testing.T) []plannerCase {
	t.Helper()

	// Both hop-1 links are conditioned; the hop-2 grants (folder#admin, group#member) are not.
	links := func() []*openfgav1.TupleKey {
		return []*openfgav1.TupleKey{
			condLink("parent", "folder:f1", "cond_parent"),
			condLink("from_members", "group:g1#member", "cond_members"),
			tuple.NewTupleKey("folder:f1", "admin", "user:alice"),
			tuple.NewTupleKey("group:g1", "member", "user:alice"),
		}
	}

	return []plannerCase{
		{
			// Both hop-1 conditions pass and both hop-2 grants exist → intersection granted.
			name:           "both_left_conditions_pass_grants",
			model:          weightTwoCondLeftModel,
			objectType:     "document",
			relation:       "viewer",
			subject:        "user:alice",
			tuples:         links(),
			requestContext: map[string]any{"p_parent": 1, "p_members": "active"},
			expected:       true,
		},
		{
			// The TTU tupleset condition (cond_parent) fails → the folder hop is gated out → the
			// intersection denies. This is the novel conditioned-TTU-edge path.
			name:           "ttu_left_condition_fails_denies",
			model:          weightTwoCondLeftModel,
			objectType:     "document",
			relation:       "viewer",
			subject:        "user:alice",
			tuples:         links(),
			requestContext: map[string]any{"p_parent": -1, "p_members": "active"},
			expected:       false,
		},
		{
			// The userset link condition (cond_members) fails → the group hop is gated out → denied.
			name:           "userset_left_condition_fails_denies",
			model:          weightTwoCondLeftModel,
			objectType:     "document",
			relation:       "viewer",
			subject:        "user:alice",
			tuples:         links(),
			requestContext: map[string]any{"p_parent": 1, "p_members": "inactive"},
			expected:       false,
		},
	}
}

// weightTwoCondRightCases returns the shared case table for the RIGHT-side conditioned model.
func weightTwoCondRightCases(t *testing.T) []plannerCase {
	t.Helper()

	// Unconditioned hop-1 links; the hop-2 grants carry the conditions.
	ttuLink := func() *openfgav1.TupleKey {
		return tuple.NewTupleKey("document:"+objectID, "parent", "folder:f1")
	}
	usersetLink := func() *openfgav1.TupleKey {
		return tuple.NewTupleKey("document:"+objectID, "from_members", "group:g1#member")
	}

	return []plannerCase{
		{
			// TTU side: folder#admin condition passes → union granted via the folder hop.
			name:       "ttu_right_condition_passes_grants",
			model:      weightTwoCondRightModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				ttuLink(),
				condHop2("folder:f1", "admin", "cond_admin"),
			},
			requestContext: map[string]any{"p_admin": 1, "p_member": "active"},
			expected:       true,
		},
		{
			// Userset side: group#member condition passes → union granted via the group hop.
			name:       "userset_right_condition_passes_grants",
			model:      weightTwoCondRightModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				usersetLink(),
				condHop2("group:g1", "member", "cond_member"),
			},
			requestContext: map[string]any{"p_admin": 1, "p_member": "active"},
			expected:       true,
		},
		{
			// Both hops are wired but both hop-2 conditions fail → union denies.
			name:       "both_right_conditions_fail_denies",
			model:      weightTwoCondRightModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: []*openfgav1.TupleKey{
				ttuLink(),
				usersetLink(),
				condHop2("folder:f1", "admin", "cond_admin"),
				condHop2("group:g1", "member", "cond_member"),
			},
			requestContext: map[string]any{"p_admin": -1, "p_member": "inactive"},
			expected:       false,
		},
	}
}

// weightTwoCondBothCases returns the shared case table for the BOTH-sides conditioned model.
func weightTwoCondBothCases(t *testing.T) []plannerCase {
	t.Helper()

	// Every hop-1 link and hop-2 grant is conditioned.
	links := func() []*openfgav1.TupleKey {
		return []*openfgav1.TupleKey{
			condLink("parent", "folder:f1", "cond_parent"),
			condLink("from_members", "group:g1#member", "cond_members"),
			condHop2("folder:f1", "admin", "cond_admin"),
			condHop2("group:g1", "member", "cond_member"),
		}
	}

	return []plannerCase{
		{
			// All four conditions pass → intersection granted.
			name:       "all_conditions_pass_grants",
			model:      weightTwoCondBothModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples:     links(),
			requestContext: map[string]any{
				"p_parent": 1, "p_members": "active", "p_admin": 1, "p_member": "active",
			},
			expected: true,
		},
		{
			// Left passes everywhere but the TTU's hop-2 condition (cond_admin) fails → denied.
			name:       "left_passes_right_fails_denies",
			model:      weightTwoCondBothModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples:     links(),
			requestContext: map[string]any{
				"p_parent": 1, "p_members": "active", "p_admin": -1, "p_member": "active",
			},
			expected: false,
		},
		{
			// Right passes everywhere but the userset's hop-1 condition (cond_members) fails → denied.
			name:       "right_passes_left_fails_denies",
			model:      weightTwoCondBothModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples:     links(),
			requestContext: map[string]any{
				"p_parent": 1, "p_members": "inactive", "p_admin": 1, "p_member": "active",
			},
			expected: false,
		},
	}
}

// complexWeightTwoMixedCases returns the shared case table for the combined mixed fixture.
func complexWeightTwoMixedCases(t *testing.T) []plannerCase {
	t.Helper()

	// Fixed links: parent (conditioned TTU), org (unconditioned TTU), members (conditioned
	// userset), blocked (unconditioned TTU exclusion arm).
	baseLinks := func() []*openfgav1.TupleKey {
		return []*openfgav1.TupleKey{
			condLink("parent", "folder:f1", "cond_parent"),
			tuple.NewTupleKey("document:"+objectID, "org", "org:o1"),
			condLink("from_members", "group:g1#member", "cond_members"),
			tuple.NewTupleKey("document:"+objectID, "blocked", "folder:b1"),
		}
	}
	with := func(extra ...*openfgav1.TupleKey) []*openfgav1.TupleKey {
		return append(baseLinks(), extra...)
	}
	// allPass satisfies every condition in the model.
	allPass := map[string]any{
		"p_parent": 1, "p_admin": 1, "p_members": "active", "p_member": "active",
	}

	return []plannerCase{
		{
			// from_folder (admin AND editor on f1, conditions pass), from_org (owner on o1),
			// from_members (member on g1, conditions pass), not blocked → granted.
			name:       "all_hops_and_conditions_pass_grants",
			model:      complexWeightTwoMixedModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: with(
				condHop2("folder:f1", "admin", "cond_admin"),
				tuple.NewTupleKey("folder:f1", "editor", "user:alice"),
				tuple.NewTupleKey("org:o1", "owner", "user:alice"),
				condHop2("group:g1", "member", "cond_member"),
			),
			requestContext: allPass,
			expected:       true,
		},
		{
			// The TTU tupleset condition (cond_parent) fails → the folder hop is gated out →
			// from_folder false → outer intersection denies.
			name:       "ttu_left_condition_fails_denies",
			model:      complexWeightTwoMixedModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: with(
				condHop2("folder:f1", "admin", "cond_admin"),
				tuple.NewTupleKey("folder:f1", "editor", "user:alice"),
				tuple.NewTupleKey("org:o1", "owner", "user:alice"),
				condHop2("group:g1", "member", "cond_member"),
			),
			requestContext: map[string]any{
				"p_parent": -1, "p_admin": 1, "p_members": "active", "p_member": "active",
			},
			expected: false,
		},
		{
			// folder#admin's hop-2 condition (cond_admin) fails → grant (admin AND editor) not
			// satisfied on f1 → from_folder false → denied.
			name:       "ttu_right_condition_fails_denies",
			model:      complexWeightTwoMixedModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: with(
				condHop2("folder:f1", "admin", "cond_admin"),
				tuple.NewTupleKey("folder:f1", "editor", "user:alice"),
				tuple.NewTupleKey("org:o1", "owner", "user:alice"),
				condHop2("group:g1", "member", "cond_member"),
			),
			requestContext: map[string]any{
				"p_parent": 1, "p_admin": -1, "p_members": "active", "p_member": "active",
			},
			expected: false,
		},
		{
			// Hop-2 intersection (grant = admin AND editor) split across two parent folders: f1
			// has admin, f2 has editor. No single folder satisfies grant → from_folder false →
			// denied. The per-object-fold correctness case under conditions.
			name:       "hop2_intersection_split_objects_denies",
			model:      complexWeightTwoMixedModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: append(with(
				condHop2("folder:f1", "admin", "cond_admin"),
				tuple.NewTupleKey("folder:f2", "editor", "user:alice"),
				tuple.NewTupleKey("org:o1", "owner", "user:alice"),
				condHop2("group:g1", "member", "cond_member"),
			), condLink("parent", "folder:f2", "cond_parent")),
			requestContext: allPass,
			expected:       false,
		},
		{
			// The userset hop's hop-2 condition (cond_member) fails → from_members false →
			// outer intersection denies even though the folder and org hops hold.
			name:       "userset_right_condition_fails_denies",
			model:      complexWeightTwoMixedModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: with(
				condHop2("folder:f1", "admin", "cond_admin"),
				tuple.NewTupleKey("folder:f1", "editor", "user:alice"),
				tuple.NewTupleKey("org:o1", "owner", "user:alice"),
				condHop2("group:g1", "member", "cond_member"),
			),
			requestContext: map[string]any{
				"p_parent": 1, "p_admin": 1, "p_members": "active", "p_member": "inactive",
			},
			expected: false,
		},
		{
			// from_org fails: no owner on o1 → outer intersection denies (unconditioned control arm).
			name:       "outer_intersection_missing_org_denies",
			model:      complexWeightTwoMixedModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: with(
				condHop2("folder:f1", "admin", "cond_admin"),
				tuple.NewTupleKey("folder:f1", "editor", "user:alice"),
				condHop2("group:g1", "member", "cond_member"),
			),
			requestContext: allPass,
			expected:       false,
		},
		{
			// privileged holds, but the subject is editor on the blocked folder b1, so
			// from_blocked grants and the exclusion denies.
			name:       "privileged_but_blocked_denies",
			model:      complexWeightTwoMixedModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples: with(
				condHop2("folder:f1", "admin", "cond_admin"),
				tuple.NewTupleKey("folder:f1", "editor", "user:alice"),
				tuple.NewTupleKey("org:o1", "owner", "user:alice"),
				condHop2("group:g1", "member", "cond_member"),
				tuple.NewTupleKey("folder:b1", "editor", "user:alice"),
			),
			requestContext: allPass,
			expected:       false,
		},
	}
}

// mergedExclusionCases walks the truth table of the mergedExclusionModel (declared in
// execute_weight2_test.go, same package and build set):
//
//	viewer = (([user] or editor) and owner from parent) but not (blocked and old)
//
// The plan compiles to three units in the unitMulti path — a merged UNION region
// ([user] OR editor), the weight-2 TTU self-join (owner from parent), and a merged INTERSECT
// region (blocked AND old) — folded in process as positive=(union AND ttu),
// result=positive AND NOT negative. These cases prove the merged regions return the correct
// decision when a real database runs their HAVING queries beside the TTU self-join.
//
// The tuples are assembled from independent toggles so each case reads as the exact state it
// seeds: positive is granted only when the parent's owner holds and at least one of
// direct/editor holds, and the result flips to deny whenever both blocked and old hold. Reused
// by both the Postgres and MySQL suites.
func mergedExclusionCases(t *testing.T) []plannerCase {
	t.Helper()

	// parentLink ties document:1 to folder:f1; ownerGrant makes alice owner of that folder, so
	// owner_from_parent (the TTU hop) holds only when both are present.
	parentLink := tuple.NewTupleKey("document:"+objectID, "parent", "folder:f1")
	ownerGrant := tuple.NewTupleKey("folder:f1", "owner", "user:alice")
	direct := grant("direct_or_editor") // the [user] arm of the union (document#direct_or_editor)
	editor := grant("editor")
	blocked := grant("blocked")
	old := grant("old")

	tk := func(keys ...*openfgav1.TupleKey) []*openfgav1.TupleKey {
		return append([]*openfgav1.TupleKey{}, keys...)
	}
	mk := func(name string, expected bool, keys ...*openfgav1.TupleKey) plannerCase {
		return plannerCase{
			name:       name,
			model:      mergedExclusionModel,
			objectType: "document",
			relation:   "viewer",
			subject:    "user:alice",
			tuples:     tk(keys...),
			expected:   expected,
		}
	}

	return []plannerCase{
		// positive never holds without the TTU owner hop, regardless of the union.
		mk("nothing_denies", false),
		mk("union_direct_without_owner_denies", false, direct),
		mk("union_editor_without_owner_denies", false, editor),
		mk("owner_without_union_denies", false, parentLink, ownerGrant),
		mk("parent_without_owner_grant_denies", false, parentLink, direct),
		mk("owner_grant_without_parent_link_denies", false, ownerGrant, direct),

		// positive holds (owner-from-parent AND a union arm) and negative does not → grant.
		mk("positive_via_direct_grants", true, parentLink, ownerGrant, direct),
		mk("positive_via_editor_grants", true, parentLink, ownerGrant, editor),
		mk("positive_via_both_union_arms_grants", true, parentLink, ownerGrant, direct, editor),

		// negative (blocked AND old) requires BOTH; one alone does not negate.
		mk("positive_with_blocked_only_grants", true, parentLink, ownerGrant, direct, blocked),
		mk("positive_with_old_only_grants", true, parentLink, ownerGrant, editor, old),

		// negative fully holds → exclusion denies even though positive is satisfied.
		mk("positive_but_blocked_and_old_denies", false, parentLink, ownerGrant, direct, blocked, old),
		mk("positive_editor_but_blocked_and_old_denies", false, parentLink, ownerGrant, editor, blocked, old),

		// negative holds but positive does not → still deny (nothing to negate from).
		mk("negative_only_denies", false, blocked, old),
	}
}

// TestPlannerIntegrationPostgres_MergedExclusion drives the merged-region exclusion model end to
// end against Postgres, proving the three compiled units (merged union, TTU self-join, merged
// intersect) fold to the correct decision across the model's truth table.
func TestPlannerIntegrationPostgres_MergedExclusion(t *testing.T) {
	env := setupPgEnv(t)
	for _, tc := range mergedExclusionCases(t) {
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
