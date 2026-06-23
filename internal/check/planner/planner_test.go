package planner

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage/adapter/adaptertest"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
)

// plan builds a Plan for the given model and Check coordinates using a render-only
// builder, so the queries can be inspected without a database. The object id is fixed
// ("1") since plan shape does not depend on it.
func plan(t *testing.T, model, objectType, relation, user string) (*Plan, error) {
	t.Helper()
	ts, err := typesystem.New(testutils.MustTransformDSLToProtoWithID(model))
	require.NoError(t, err)
	g := ts.GetWeightedGraph()
	require.NotNil(t, g)

	p := New(adaptertest.New(nil))
	return p.Plan(g, "store1", objectType, "1", relation, user)
}

func TestPlan_WeightOneShapes(t *testing.T) {
	t.Run("single_direct", func(t *testing.T) {
		model := `
			model
				schema 1.1
			type user
			type document
				relations
					define viewer: [user]`
		p, err := plan(t, model, "document", "viewer", "user:alice")
		require.NoError(t, err)

		leaf, ok := p.Root.(*QueryNode)
		require.True(t, ok, "expected a single QueryNode, got %T", p.Root)
		require.Equal(t, 1, leaf.Weight)
		require.Equal(t, "document#viewer", leaf.Label)
	})

	t.Run("computed_userset", func(t *testing.T) {
		// viewer: editor — same object, different relation. Stays a flat leaf.
		model := `
			model
				schema 1.1
			type user
			type document
				relations
					define editor: [user]
					define viewer: editor`
		p, err := plan(t, model, "document", "viewer", "user:alice")
		require.NoError(t, err)

		leaf, ok := p.Root.(*QueryNode)
		require.True(t, ok, "expected a QueryNode, got %T", p.Root)
		// The relation resolves through to editor.
		require.Equal(t, "document#editor", leaf.Label)
	})

	t.Run("same_type_union", func(t *testing.T) {
		model := `
			model
				schema 1.1
			type user
			type document
				relations
					define editor: [user]
					define viewer: [user] or editor`
		p, err := plan(t, model, "document", "viewer", "user:alice")
		require.NoError(t, err)

		cn, ok := p.Root.(*CombineNode)
		require.True(t, ok, "expected a CombineNode, got %T", p.Root)
		require.Equal(t, CombineUnion, cn.Op)
		require.Len(t, cn.Children, 2)
		require.Len(t, collectLeaves(cn), 2)
	})

	t.Run("same_type_intersection", func(t *testing.T) {
		model := `
			model
				schema 1.1
			type user
			type document
				relations
					define editor: [user]
					define viewer: [user] and editor`
		p, err := plan(t, model, "document", "viewer", "user:alice")
		require.NoError(t, err)

		cn, ok := p.Root.(*CombineNode)
		require.True(t, ok, "expected a CombineNode, got %T", p.Root)
		require.Equal(t, CombineIntersect, cn.Op)
		require.Len(t, cn.Children, 2)
	})

	t.Run("same_type_exclusion", func(t *testing.T) {
		model := `
			model
				schema 1.1
			type user
			type document
				relations
					define banned: [user]
					define viewer: [user] but not banned`
		p, err := plan(t, model, "document", "viewer", "user:alice")
		require.NoError(t, err)

		cn, ok := p.Root.(*CombineNode)
		require.True(t, ok, "expected a CombineNode, got %T", p.Root)
		require.Equal(t, CombineExcept, cn.Op)
		require.Len(t, cn.Children, 2)
	})
}

func TestPlan_UnitSelection(t *testing.T) {
	t.Run("condition_free_is_having", func(t *testing.T) {
		// A union of unconditioned relations is condition-free: the database folds it.
		p, err := plan(t, unionModel, "document", "viewer", "user:alice")
		require.NoError(t, err)
		require.True(t, p.Root.ConditionFree())
		require.Equal(t, unitHaving, p.unit.kind)
	})

	t.Run("conditioned_is_gather", func(t *testing.T) {
		// super_admin mixes a conditioned operand with an unconditioned one, so the whole
		// tree gathers candidates for in-process CEL evaluation.
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
		p, err := plan(t, model, "account", "viewer", "user:alice")
		require.NoError(t, err)
		require.False(t, p.Root.ConditionFree())
		require.Equal(t, unitGather, p.unit.kind)
		require.Len(t, p.unit.leaves, 2)
	})
}

func TestPlan_UnreachableSubjectType(t *testing.T) {
	// employee never grants document#viewer, so the Check is trivially false: an empty
	// union root.
	model := `
		model
			schema 1.1
		type user
		type employee
		type document
			relations
				define viewer: [user]`
	p, err := plan(t, model, "document", "viewer", "employee:e1")
	require.NoError(t, err)

	cn, ok := p.Root.(*CombineNode)
	require.True(t, ok, "expected an empty CombineNode, got %T", p.Root)
	require.Empty(t, cn.Children)
}

func TestPlan_UnsupportedWeight(t *testing.T) {
	t.Run("tuple_to_userset", func(t *testing.T) {
		model := `
			model
				schema 1.1
			type user
			type folder
				relations
					define viewer: [user]
			type document
				relations
					define parent: [folder]
					define viewer: viewer from parent`
		_, err := plan(t, model, "document", "viewer", "user:alice")
		require.ErrorIs(t, err, ErrUnsupportedWeight)
	})

	t.Run("userset", func(t *testing.T) {
		model := `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
			type document
				relations
					define viewer: [group#member]`
		_, err := plan(t, model, "document", "viewer", "user:alice")
		require.ErrorIs(t, err, ErrUnsupportedWeight)
	})

	t.Run("recursive_usersets", func(t *testing.T) {
		model := `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user, group#member]`
		_, err := plan(t, model, "group", "member", "user:alice")
		require.ErrorIs(t, err, ErrUnsupportedWeight)
	})
}

func TestPlan_UnknownRelation(t *testing.T) {
	model := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user]`
	_, err := plan(t, model, "document", "missing", "user:alice")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrUnsupportedWeight)
}
