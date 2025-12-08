package modelgraph

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/testutils"
)

func TestNew(t *testing.T) {
	t.Run("creates_authorization_model_graph_successfully", func(t *testing.T) {
		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type document
    relations
       define viewer: [group] or viewer from parent
	   define parent: [document, group]
   type group
    relations
	   define viewer: member
	   define member: reader or public
	   define public: [user:*]
	   define reader: [user]
  `)

		graph, err := New(model)
		require.NoError(t, err)
		require.NotNil(t, graph)
		require.Equal(t, model.GetId(), graph.GetModelID())
		require.NotNil(t, graph.WeightedAuthorizationModelGraph)
	})

	t.Run("creates_graph_with_conditions", func(t *testing.T) {
		model := testutils.MustTransformDSLToProtoWithID(`
   model
    schema 1.1
   type user
   type group
	relations
		define viewer: [user with xcond]
   type document
    relations
       define viewer: [group#viewer with ycond]
    condition xcond(x: string) {
  	x == '1'
	}
	 condition ycond(y: string) {
  	y == '1'
	}
  `)

		graph, err := New(model)
		require.NoError(t, err)
		require.NotNil(t, graph)
		require.Len(t, graph.GetConditions(), 2)
		require.Contains(t, graph.GetConditions(), "xcond")
		require.Contains(t, graph.GetConditions(), "ycond")
	})

	t.Run("returns_error_for_invalid_model", func(t *testing.T) {
		model := &openfgav1.AuthorizationModel{
			Id:            "invalid-model",
			SchemaVersion: "1.1",
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"viewer": {
							Userset: &openfgav1.Userset_Union{
								Union: &openfgav1.Usersets{
									Child: []*openfgav1.Userset{},
								},
							},
						},
					},
				},
			},
		}

		graph, err := New(model)
		require.Error(t, err)
		require.Nil(t, graph)
	})
}

func TestWildcardRelationReference(t *testing.T) {
	t.Run("creates_wildcard_relation_reference", func(t *testing.T) {
		ref := WildcardRelationReference("document")
		require.NotNil(t, ref)
		require.Equal(t, "document", ref.GetType())
		require.NotNil(t, ref.GetWildcard())
	})
}

func TestGetDirectEdgeFromNodeForUserType(t *testing.T) {
	model := &openfgav1.AuthorizationModel{
		Id:            "test-model",
		SchemaVersion: "1.1",
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"viewer": {
						Userset: &openfgav1.Userset_This{},
					},
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"viewer": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{Type: "user"},
							},
						},
					},
				},
			},
		},
	}

	graph, err := New(model)
	require.NoError(t, err)

	t.Run("returns_error_when_node_not_found", func(t *testing.T) {
		edge, err := graph.GetDirectEdgeFromNodeForUserType("nonexistent#relation", "user")
		require.Error(t, err)
		require.Equal(t, ErrGraphError, err)
		require.Nil(t, edge)
	})

	t.Run("returns_edge_when_user_type_matches", func(t *testing.T) {
		edge, err := graph.GetDirectEdgeFromNodeForUserType("document#viewer", "user")
		require.NoError(t, err)
		require.NotNil(t, edge)
	})
}

func TestFlattenNodeWithWildcard(t *testing.T) {
	t.Run("wildcard_flattening", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type group
              relations
                define member: [user:*] or admin
                define admin: [user:*]
        `)

		graph, err := New(model)
		require.NoError(t, err)
		require.NotNil(t, graph)

		node, ok := graph.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := graph.FlattenNode(node, "user", true, false)
		require.NoError(t, err)
		require.Len(t, edges, 2)
	})

	t.Run("wildcard_partial_edges", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type group
              relations
                define member: [user] or admin
                define admin: [user:*]
        `)

		graph, err := New(model)
		require.NoError(t, err)
		require.NotNil(t, graph)

		node, ok := graph.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := graph.FlattenNode(node, "user", true, false)
		require.NoError(t, err)
		require.Len(t, edges, 1)
	})

	t.Run("wildcard_no_edges", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type group
              relations
                define member: [user] or admin
                define admin: [user]
        `)

		graph, err := New(model)
		require.NoError(t, err)
		require.NotNil(t, graph)

		node, ok := graph.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := graph.FlattenNode(node, "user", true, false)
		require.NoError(t, err)
		require.Empty(t, edges)
	})

	t.Run("wildcard_with_non_wildcard_exclusion", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type group
              relations
                define member: [user:*] or (admin but not viewer)
                define admin: [user]
				define viewer: [user]
        `)

		graph, err := New(model)
		require.NoError(t, err)
		require.NotNil(t, graph)

		node, ok := graph.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := graph.FlattenNode(node, "user", true, false)
		require.NoError(t, err)
		require.Len(t, edges, 1)
	})

	t.Run("wildcard_with_non_wildcard_intersection", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type group
              relations
                define member: [user:*] or (admin and viewer)
                define admin: [user]
				define viewer: [user]
        `)

		graph, err := New(model)
		require.NoError(t, err)
		require.NotNil(t, graph)

		node, ok := graph.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := graph.FlattenNode(node, "user", true, false)
		require.NoError(t, err)
		require.Len(t, edges, 1)
	})

	t.Run("wildcard_with_wildcard_exclusion", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type group
              relations
                define member: [user:*] or (admin but not viewer)
                define admin: [user:*]
				define viewer: [user:*]
        `)

		graph, err := New(model)
		require.NoError(t, err)
		require.NotNil(t, graph)

		node, ok := graph.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := graph.FlattenNode(node, "user", true, false)
		require.NoError(t, err)
		require.Len(t, edges, 2)
	})

	t.Run("wildcard_with_wildcard_intersection", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type group
              relations
                define member: [user:*] or (admin and viewer)
                define admin: [user:*]
				define viewer: [user:*]
        `)

		graph, err := New(model)
		require.NoError(t, err)
		require.NotNil(t, graph)

		node, ok := graph.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := graph.FlattenNode(node, "user", true, false)
		require.NoError(t, err)
		require.Len(t, edges, 2)
	})

	t.Run("wildcard_with_wildcard_userset", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type group
              relations
                define member: [user:*, group#viewer] or admin
                define admin: [user:*]
				define viewer: [user:*]
        `)

		graph, err := New(model)
		require.NoError(t, err)
		require.NotNil(t, graph)

		node, ok := graph.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := graph.FlattenNode(node, "user", true, false)
		require.NoError(t, err)
		require.Len(t, edges, 3)
	})

	t.Run("wildcard_with_no_wildcard_userset", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type group
              relations
                define member: [user:*, group#viewer]
				define viewer: [user]
        `)

		graph, err := New(model)
		require.NoError(t, err)
		require.NotNil(t, graph)

		node, ok := graph.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := graph.FlattenNode(node, "user", true, false)
		require.NoError(t, err)
		require.Len(t, edges, 1)
	})

	t.Run("wildcard_with_wildcard_ttu", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type group
              relations
                define member: [user:*] or viewer from parent
                define parent: [group]
				define viewer: [user:*]
        `)

		graph, err := New(model)
		require.NoError(t, err)
		require.NotNil(t, graph)

		node, ok := graph.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := graph.FlattenNode(node, "user", true, false)
		require.NoError(t, err)
		require.Len(t, edges, 2)
	})

	t.Run("wildcard_with_no_wildcard_ttu", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		model := testutils.MustTransformDSLToProtoWithID(`
           model
              schema 1.1
            type user
            type group
              relations
                define member: [user:*] or viewer from parent
                define parent: [group]
				define viewer: [user]
        `)

		graph, err := New(model)
		require.NoError(t, err)
		require.NotNil(t, graph)

		node, ok := graph.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := graph.FlattenNode(node, "user", true, false)
		require.NoError(t, err)
		require.Len(t, edges, 1)
	})

	//
	t.Run("wildcard_with_wildcard_userset_recursive", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type group
              relations
                define member: [user:*, group#member] or admin
                define admin: [user:*]
        `)

		graph, err := New(model)
		require.NoError(t, err)
		require.NotNil(t, graph)

		node, ok := graph.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := graph.FlattenNode(node, "user", true, true)
		require.NoError(t, err)
		require.Len(t, edges, 2)
	})

	t.Run("wildcard_with_no_wildcard_userset_recursive", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type group
              relations
                define member: [user, group#member] or admin
				define admin: [user]
        `)

		graph, err := New(model)
		require.NoError(t, err)
		require.NotNil(t, graph)

		node, ok := graph.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := graph.FlattenNode(node, "user", true, true)
		require.NoError(t, err)
		require.Empty(t, edges)
	})

	t.Run("wildcard_with_partial_wildcard_userset_recursive", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type group
              relations
                define member: [user:*, group#member, group#viewer] or admin
				define admin: [user]
				define viewer: [user]
        `)

		graph, err := New(model)
		require.NoError(t, err)
		require.NotNil(t, graph)

		node, ok := graph.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := graph.FlattenNode(node, "user", true, true)
		require.NoError(t, err)
		require.Len(t, edges, 1)
	})

	t.Run("wildcard_with_wildcard_ttu_recursive", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		model := testutils.MustTransformDSLToProtoWithID(`
            model
              schema 1.1
            type user
            type group
              relations
                define member: [user:*] or member from parent or admin
                define parent: [group]
				define admin: [user:*]
        `)

		graph, err := New(model)
		require.NoError(t, err)
		require.NotNil(t, graph)

		node, ok := graph.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := graph.FlattenNode(node, "user", true, true)
		require.NoError(t, err)
		require.Len(t, edges, 2)
	})

	t.Run("wildcard_with_no_wildcard_ttu_recursive", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		model := testutils.MustTransformDSLToProtoWithID(`
           model
              schema 1.1
            type user
            type group
              relations
                define member: [user] or member from parent or admin
                define parent: [group]
				define admin: [user]
        `)

		graph, err := New(model)
		require.NoError(t, err)
		require.NotNil(t, graph)

		node, ok := graph.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := graph.FlattenNode(node, "user", true, true)
		require.NoError(t, err)
		require.Empty(t, edges)
	})

	t.Run("wildcard_with_partial_wildcard_ttu_recursive", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		model := testutils.MustTransformDSLToProtoWithID(`
           model
              schema 1.1
            type user
            type group
              relations
                define member: [user:*] or member from parent or admin
                define parent: [group]
				define admin: [user]
        `)

		graph, err := New(model)
		require.NoError(t, err)
		require.NotNil(t, graph)

		node, ok := graph.GetNodeByID("group#member")
		require.True(t, ok)

		edges, err := graph.FlattenNode(node, "user", true, true)
		require.NoError(t, err)
		require.Len(t, edges, 1)
	})
}
