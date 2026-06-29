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

func TestPlan_WeightTwoShapes(t *testing.T) {
	t.Run("ttu", func(t *testing.T) {
		// viewer: admin from parent — a single tuple-to-userset hop. The bound object's
		// `parent` tuples name a folder; the folder's `admin` relation grants the subject.
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
		p, err := plan(t, model, "document", "viewer", "user:alice")
		require.NoError(t, err)

		join, ok := p.Root.(*JoinNode)
		require.True(t, ok, "expected a JoinNode, got %T", p.Root)
		require.True(t, join.IsTTU)
		require.Equal(t, 2, join.Weight)
		require.Equal(t, "parent", join.Hop1Relation)
		require.Empty(t, join.Hop1SubjectRelation, "a TTU hop-1 tuple names an object, not a userset")
		require.Equal(t, "folder", join.IntermediateType)
		hop2Leaf, ok := join.Hop2.(*QueryNode)
		require.True(t, ok, "expected hop-2 to be a single QueryNode, got %T", join.Hop2)
		require.Equal(t, "admin", hop2Leaf.Relation)
		require.Equal(t, unitMulti, p.unit.kind)
		require.Len(t, p.unit.multi, 1)
	})

	t.Run("userset", func(t *testing.T) {
		// viewer: [group#member] — a single userset hop. The bound object's `viewer` tuples
		// name a group userset; the group's `member` relation grants the subject.
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
		p, err := plan(t, model, "document", "viewer", "user:alice")
		require.NoError(t, err)

		join, ok := p.Root.(*JoinNode)
		require.True(t, ok, "expected a JoinNode, got %T", p.Root)
		require.False(t, join.IsTTU)
		require.Equal(t, "viewer", join.Hop1Relation)
		require.Equal(t, "member", join.Hop1SubjectRelation)
		require.Equal(t, "group", join.IntermediateType)
		hop2Leaf, ok := join.Hop2.(*QueryNode)
		require.True(t, ok, "expected hop-2 to be a single QueryNode, got %T", join.Hop2)
		require.Equal(t, "member", hop2Leaf.Relation)
	})

	t.Run("mixed_weight_one_and_two_union", func(t *testing.T) {
		// viewer: [user, group#member] — a weight-1 direct grant unioned with a weight-2
		// userset hop. Each compiles to its own query under the union.
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
		p, err := plan(t, model, "document", "viewer", "user:alice")
		require.NoError(t, err)

		cn, ok := p.Root.(*CombineNode)
		require.True(t, ok, "expected a CombineNode, got %T", p.Root)
		require.Equal(t, CombineUnion, cn.Op)
		require.Len(t, cn.Children, 2)
		require.IsType(t, &QueryNode{}, cn.Children[0])
		require.IsType(t, &JoinNode{}, cn.Children[1])
		require.Equal(t, unitMulti, p.unit.kind)
		require.Len(t, p.unit.multi, 2)
	})

	t.Run("ttu_multiple_intermediate_types", func(t *testing.T) {
		// parent: [folder, org] — admin from parent fans out to one JoinNode per parent
		// type, under a union, so each query stays single-type.
		model := `
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
					define viewer: admin from parent`
		p, err := plan(t, model, "document", "viewer", "user:alice")
		require.NoError(t, err)

		cn, ok := p.Root.(*CombineNode)
		require.True(t, ok, "expected a CombineNode, got %T", p.Root)
		require.Equal(t, CombineUnion, cn.Op)
		require.Len(t, cn.Children, 2)
		types := []string{}
		for _, c := range cn.Children {
			j, ok := c.(*JoinNode)
			require.True(t, ok, "expected a JoinNode, got %T", c)
			types = append(types, j.IntermediateType)
		}
		require.ElementsMatch(t, []string{"folder", "org"}, types)
	})

	t.Run("hop2_union", func(t *testing.T) {
		// admin: [user] or owner — the hop-2 subtree is a union of direct leaves, kept whole
		// on the JoinNode and folded per intermediate object.
		model := `
			model
				schema 1.1
			type user
			type folder
				relations
					define owner: [user]
					define admin: [user] or owner
			type document
				relations
					define parent: [folder]
					define viewer: admin from parent`
		p, err := plan(t, model, "document", "viewer", "user:alice")
		require.NoError(t, err)

		join, ok := p.Root.(*JoinNode)
		require.True(t, ok, "expected a JoinNode, got %T", p.Root)
		cn, ok := join.Hop2.(*CombineNode)
		require.True(t, ok, "expected hop-2 to be a CombineNode, got %T", join.Hop2)
		require.Equal(t, CombineUnion, cn.Op)
		rels := []string{}
		for _, leaf := range collectLeaves(cn) {
			rels = append(rels, leaf.Relation)
		}
		require.ElementsMatch(t, []string{"admin", "owner"}, rels)
	})

	t.Run("hop2_intersection", func(t *testing.T) {
		// admin: a and b — the hop-2 subtree is an intersection, preserved whole so it folds
		// per intermediate object (one folder must have both a and b).
		model := `
			model
				schema 1.1
			type user
			type folder
				relations
					define a: [user]
					define b: [user]
					define admin: a and b
			type document
				relations
					define parent: [folder]
					define viewer: admin from parent`
		p, err := plan(t, model, "document", "viewer", "user:alice")
		require.NoError(t, err)

		join, ok := p.Root.(*JoinNode)
		require.True(t, ok, "expected a JoinNode, got %T", p.Root)
		cn, ok := join.Hop2.(*CombineNode)
		require.True(t, ok, "expected hop-2 to be a CombineNode, got %T", join.Hop2)
		require.Equal(t, CombineIntersect, cn.Op)
		require.Len(t, cn.Children, 2)
	})

	t.Run("hop2_exclusion", func(t *testing.T) {
		// admin: a but not banned — the hop-2 subtree is an exclusion, folded per object.
		model := `
			model
				schema 1.1
			type user
			type folder
				relations
					define a: [user]
					define banned: [user]
					define admin: a but not banned
			type document
				relations
					define parent: [folder]
					define viewer: admin from parent`
		p, err := plan(t, model, "document", "viewer", "user:alice")
		require.NoError(t, err)

		join, ok := p.Root.(*JoinNode)
		require.True(t, ok, "expected a JoinNode, got %T", p.Root)
		cn, ok := join.Hop2.(*CombineNode)
		require.True(t, ok, "expected hop-2 to be a CombineNode, got %T", join.Hop2)
		require.Equal(t, CombineExcept, cn.Op)
		require.Len(t, cn.Children, 2)
	})
}

func TestPlan_UnsupportedWeight(t *testing.T) {
	t.Run("weight_three_ttu", func(t *testing.T) {
		// viewer: admin from parent, where admin is itself a userset hop — the path needs
		// two hops (weight 3), which exceeds this iteration.
		model := `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
			type folder
				relations
					define admin: [group#member]
			type document
				relations
					define parent: [folder]
					define viewer: admin from parent`
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

	t.Run("recursive_ttu", func(t *testing.T) {
		// viewer reaches itself through parent (a tuple cycle): infinite weight.
		model := `
			model
				schema 1.1
			type user
			type document
				relations
					define parent: [document]
					define viewer: [user] or viewer from parent`
		_, err := plan(t, model, "document", "viewer", "user:alice")
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
