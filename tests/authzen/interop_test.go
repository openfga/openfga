package authzen_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

// AuthZEN Todo Application Model (from docs/authzen/authzen-todo.fga)
// This model represents the official AuthZEN interoperability Todo application.
const authzenTodoModel = `
	model
		schema 1.1

	type user
		relations
			define can_read_user: [user:*]

	type todo
		relations
			define admin: [user]
			define editor: [user]
			define evil_genius: [user]
			define viewer: [user]

			define can_manage_todo_items : editor or admin or evil_genius

			define can_create_todo: can_manage_todo_items
			define can_read_todos: viewer or can_manage_todo_items

			define parent: [todo]
			define owner: [user]

			define can_delete_todo: (can_manage_todo_items from parent and owner) or admin from parent
			define can_update_todo: (can_manage_todo_items from parent and owner) or evil_genius from parent
`

// AuthZEN Gateway Application Model (from docs/authzen/authzen-gateway.fga)
// This model represents the official AuthZEN interoperability Gateway application.
const authzenGatewayModel = `
	model
		schema 1.1

	type identity

	type role
		relations
			define assignee: [identity]

	type route
		relations
			define GET: [identity:*, role#assignee]
			define POST: [role#assignee]
			define PUT: [role#assignee]
			define DELETE: [role#assignee]
`

// TestAuthZENTodoInterop tests the AuthZEN Todo application interoperability scenarios.
// These tests verify that OpenFGA correctly implements the AuthZEN specification
// using the official Todo application model.
func TestAuthZENTodoInterop(t *testing.T) {
	// Setup the AuthZEN Todo application scenario:
	// - Rick Sanchez (admin) - can do everything
	// - Morty Smith (editor) - can manage todos, including his own
	// - Summer Smith (viewer) - can only read todos
	// - Beth Smith (evil_genius) - can manage todos and update any todo

	t.Run("todo_app_role_assignments", func(t *testing.T) {
		tc := setupTestContext(t, "memory")
		tc.createStore("authzen-todo-interop")
		tc.writeModel(authzenTodoModel)

		// Setup the root todo object (todo:1) with role assignments
		tc.writeTuples([]*openfgav1.TupleKey{
			// Rick is an admin
			{User: "user:rick-sanchez", Relation: "admin", Object: "todo:1"},
			// Morty is an editor
			{User: "user:morty-smith", Relation: "editor", Object: "todo:1"},
			// Summer is a viewer
			{User: "user:summer-smith", Relation: "viewer", Object: "todo:1"},
			// Beth is an evil_genius
			{User: "user:beth-smith", Relation: "evil_genius", Object: "todo:1"},
		})

		// Test role-based permissions
		tests := []struct {
			name     string
			subject  string
			resource string
			action   string
			expected bool
		}{
			// Admin can manage todo items
			{"admin_can_manage_todos", "user:rick-sanchez", "todo:1", "can_manage_todo_items", true},
			{"admin_can_create_todo", "user:rick-sanchez", "todo:1", "can_create_todo", true},
			{"admin_can_read_todos", "user:rick-sanchez", "todo:1", "can_read_todos", true},

			// Editor can manage todo items
			{"editor_can_manage_todos", "user:morty-smith", "todo:1", "can_manage_todo_items", true},
			{"editor_can_create_todo", "user:morty-smith", "todo:1", "can_create_todo", true},
			{"editor_can_read_todos", "user:morty-smith", "todo:1", "can_read_todos", true},

			// Viewer can only read todos
			{"viewer_cannot_manage_todos", "user:summer-smith", "todo:1", "can_manage_todo_items", false},
			{"viewer_cannot_create_todo", "user:summer-smith", "todo:1", "can_create_todo", false},
			{"viewer_can_read_todos", "user:summer-smith", "todo:1", "can_read_todos", true},

			// Evil genius can manage todo items
			{"evil_genius_can_manage_todos", "user:beth-smith", "todo:1", "can_manage_todo_items", true},
			{"evil_genius_can_create_todo", "user:beth-smith", "todo:1", "can_create_todo", true},
			{"evil_genius_can_read_todos", "user:beth-smith", "todo:1", "can_read_todos", true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				resp, err := tc.evaluate(tt.subject, tt.resource, tt.action)
				require.NoError(t, err)
				require.Equal(t, tt.expected, resp.Decision, "expected %s to have %s=%v on %s", tt.subject, tt.action, tt.expected, tt.resource)
			})
		}
	})

	t.Run("todo_item_ownership", func(t *testing.T) {
		tc := setupTestContext(t, "memory")
		tc.createStore("authzen-todo-ownership")
		tc.writeModel(authzenTodoModel)

		// Setup role assignments on root todo
		tc.writeTuples([]*openfgav1.TupleKey{
			// Rick is admin on root todo
			{User: "user:rick-sanchez", Relation: "admin", Object: "todo:1"},
			// Morty is editor on root todo
			{User: "user:morty-smith", Relation: "editor", Object: "todo:1"},
			// Beth is evil_genius on root todo
			{User: "user:beth-smith", Relation: "evil_genius", Object: "todo:1"},

			// Create a specific todo item owned by Morty
			{User: "todo:1", Relation: "parent", Object: "todo:morty-todo-1"},
			{User: "user:morty-smith", Relation: "owner", Object: "todo:morty-todo-1"},

			// Create a specific todo item owned by Beth
			{User: "todo:1", Relation: "parent", Object: "todo:beth-todo-1"},
			{User: "user:beth-smith", Relation: "owner", Object: "todo:beth-todo-1"},
		})

		tests := []struct {
			name     string
			subject  string
			resource string
			action   string
			expected bool
		}{
			// Morty can delete his own todo (editor + owner)
			{"editor_can_delete_own_todo", "user:morty-smith", "todo:morty-todo-1", "can_delete_todo", true},
			// Morty can update his own todo (has can_manage_todo_items from parent + owner)
			{"editor_can_update_own_todo", "user:morty-smith", "todo:morty-todo-1", "can_update_todo", true},

			// Morty cannot delete Beth's todo (editor but not owner)
			{"editor_cannot_delete_others_todo", "user:morty-smith", "todo:beth-todo-1", "can_delete_todo", false},
			// Morty cannot update Beth's todo (editor but not owner, and not evil_genius)
			{"editor_cannot_update_others_todo", "user:morty-smith", "todo:beth-todo-1", "can_update_todo", false},

			// Admin (Rick) can delete any todo
			{"admin_can_delete_any_todo", "user:rick-sanchez", "todo:morty-todo-1", "can_delete_todo", true},
			{"admin_can_delete_any_todo_2", "user:rick-sanchez", "todo:beth-todo-1", "can_delete_todo", true},

			// Evil genius (Beth) can update any todo
			{"evil_genius_can_update_any_todo", "user:beth-smith", "todo:morty-todo-1", "can_update_todo", true},
			{"evil_genius_can_update_own_todo", "user:beth-smith", "todo:beth-todo-1", "can_update_todo", true},

			// Beth can delete her own todo (evil_genius + owner)
			{"evil_genius_can_delete_own_todo", "user:beth-smith", "todo:beth-todo-1", "can_delete_todo", true},
			// Beth cannot delete Morty's todo (evil_genius but not owner)
			{"evil_genius_cannot_delete_others_todo", "user:beth-smith", "todo:morty-todo-1", "can_delete_todo", false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				resp, err := tc.evaluate(tt.subject, tt.resource, tt.action)
				require.NoError(t, err)
				require.Equal(t, tt.expected, resp.Decision, "expected %s to have %s=%v on %s", tt.subject, tt.action, tt.expected, tt.resource)
			})
		}
	})

	t.Run("todo_user_can_read_user", func(t *testing.T) {
		tc := setupTestContext(t, "memory")
		tc.createStore("authzen-todo-can-read-user")
		tc.writeModel(authzenTodoModel)

		// Grant all users the ability to read a specific user's profile
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:*", Relation: "can_read_user", Object: "user:rick-sanchez"},
			{User: "user:*", Relation: "can_read_user", Object: "user:morty-smith"},
		})

		// Any user can read other users' profiles
		tests := []struct {
			name     string
			subject  string
			resource string
			action   string
			expected bool
		}{
			{"any_user_can_read_rick", "user:morty-smith", "user:rick-sanchez", "can_read_user", true},
			{"any_user_can_read_morty", "user:summer-smith", "user:morty-smith", "can_read_user", true},
			{"random_user_can_read_morty", "user:random-user", "user:morty-smith", "can_read_user", true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				resp, err := tc.evaluate(tt.subject, tt.resource, tt.action)
				require.NoError(t, err)
				require.Equal(t, tt.expected, resp.Decision)
			})
		}
	})

	t.Run("todo_evaluations_batch", func(t *testing.T) {
		tc := setupTestContext(t, "memory")
		tc.createStore("authzen-todo-batch")
		tc.writeModel(authzenTodoModel)

		// Setup complete scenario
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:rick-sanchez", Relation: "admin", Object: "todo:1"},
			{User: "user:morty-smith", Relation: "editor", Object: "todo:1"},
			{User: "user:summer-smith", Relation: "viewer", Object: "todo:1"},

			{User: "todo:1", Relation: "parent", Object: "todo:item-1"},
			{User: "user:morty-smith", Relation: "owner", Object: "todo:item-1"},
		})

		// Batch evaluation request
		resp, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: tc.storeID,
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "rick-sanchez"},
					Resource: &authzenv1.Resource{Type: "todo", Id: "1"},
					Action:   &authzenv1.Action{Name: "can_create_todo"},
				},
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "morty-smith"},
					Resource: &authzenv1.Resource{Type: "todo", Id: "item-1"},
					Action:   &authzenv1.Action{Name: "can_delete_todo"},
				},
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "summer-smith"},
					Resource: &authzenv1.Resource{Type: "todo", Id: "1"},
					Action:   &authzenv1.Action{Name: "can_read_todos"},
				},
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "summer-smith"},
					Resource: &authzenv1.Resource{Type: "todo", Id: "1"},
					Action:   &authzenv1.Action{Name: "can_create_todo"},
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.EvaluationResponses, 4)

		// Rick can create todos (admin)
		require.True(t, resp.EvaluationResponses[0].Decision, "Rick should be able to create todos")
		// Morty can delete his own todo
		require.True(t, resp.EvaluationResponses[1].Decision, "Morty should be able to delete his own todo")
		// Summer can read todos
		require.True(t, resp.EvaluationResponses[2].Decision, "Summer should be able to read todos")
		// Summer cannot create todos
		require.False(t, resp.EvaluationResponses[3].Decision, "Summer should not be able to create todos")
	})
}

// TestAuthZENGatewayInterop tests the AuthZEN Gateway application interoperability scenarios.
// These tests verify API access control based on HTTP methods and roles.
func TestAuthZENGatewayInterop(t *testing.T) {
	// Gateway application scenario:
	// - Some GET routes are open to all authenticated users
	// - POST, PUT, DELETE routes require specific roles
	// - Roles: admin, developer, viewer

	t.Run("gateway_public_get_routes", func(t *testing.T) {
		tc := setupTestContext(t, "memory")
		tc.createStore("authzen-gateway-interop")
		tc.writeModel(authzenGatewayModel)

		// Setup: Some GET routes are open to all identities
		tc.writeTuples([]*openfgav1.TupleKey{
			// Public GET routes - open to all
			{User: "identity:*", Relation: "GET", Object: "route:/api/health"},
			{User: "identity:*", Relation: "GET", Object: "route:/api/status"},
			{User: "identity:*", Relation: "GET", Object: "route:/api/public/info"},
		})

		// Any identity can access public GET routes
		tests := []struct {
			name     string
			subject  string
			resource string
			action   string
			expected bool
		}{
			{"any_user_can_get_health", "identity:user-1", "route:/api/health", "GET", true},
			{"any_user_can_get_status", "identity:user-2", "route:/api/status", "GET", true},
			{"any_user_can_get_public_info", "identity:anonymous", "route:/api/public/info", "GET", true},

			// Cannot POST/PUT/DELETE on public routes without role
			{"cannot_post_health", "identity:user-1", "route:/api/health", "POST", false},
			{"cannot_put_status", "identity:user-1", "route:/api/status", "PUT", false},
			{"cannot_delete_public_info", "identity:user-1", "route:/api/public/info", "DELETE", false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				resp, err := tc.evaluate(tt.subject, tt.resource, tt.action)
				require.NoError(t, err)
				require.Equal(t, tt.expected, resp.Decision)
			})
		}
	})

	t.Run("gateway_role_based_access", func(t *testing.T) {
		tc := setupTestContext(t, "memory")
		tc.createStore("authzen-gateway-roles")
		tc.writeModel(authzenGatewayModel)

		// Setup roles
		tc.writeTuples([]*openfgav1.TupleKey{
			// Admin role has full access
			{User: "identity:alice", Relation: "assignee", Object: "role:admin"},

			// Developer role has read/write access
			{User: "identity:bob", Relation: "assignee", Object: "role:developer"},

			// Viewer role has read-only access
			{User: "identity:charlie", Relation: "assignee", Object: "role:viewer"},

			// Route permissions by role
			// /api/users route
			{User: "role:admin#assignee", Relation: "GET", Object: "route:/api/users"},
			{User: "role:admin#assignee", Relation: "POST", Object: "route:/api/users"},
			{User: "role:admin#assignee", Relation: "PUT", Object: "route:/api/users"},
			{User: "role:admin#assignee", Relation: "DELETE", Object: "route:/api/users"},

			{User: "role:developer#assignee", Relation: "GET", Object: "route:/api/users"},
			{User: "role:developer#assignee", Relation: "POST", Object: "route:/api/users"},
			{User: "role:developer#assignee", Relation: "PUT", Object: "route:/api/users"},

			{User: "role:viewer#assignee", Relation: "GET", Object: "route:/api/users"},

			// /api/data route
			{User: "role:admin#assignee", Relation: "GET", Object: "route:/api/data"},
			{User: "role:admin#assignee", Relation: "POST", Object: "route:/api/data"},
			{User: "role:admin#assignee", Relation: "PUT", Object: "route:/api/data"},
			{User: "role:admin#assignee", Relation: "DELETE", Object: "route:/api/data"},

			{User: "role:developer#assignee", Relation: "GET", Object: "route:/api/data"},
			{User: "role:developer#assignee", Relation: "POST", Object: "route:/api/data"},
		})

		tests := []struct {
			name     string
			subject  string
			resource string
			action   string
			expected bool
		}{
			// Admin (Alice) has full access
			{"admin_can_get_users", "identity:alice", "route:/api/users", "GET", true},
			{"admin_can_post_users", "identity:alice", "route:/api/users", "POST", true},
			{"admin_can_put_users", "identity:alice", "route:/api/users", "PUT", true},
			{"admin_can_delete_users", "identity:alice", "route:/api/users", "DELETE", true},

			// Developer (Bob) can read/write but not delete
			{"developer_can_get_users", "identity:bob", "route:/api/users", "GET", true},
			{"developer_can_post_users", "identity:bob", "route:/api/users", "POST", true},
			{"developer_can_put_users", "identity:bob", "route:/api/users", "PUT", true},
			{"developer_cannot_delete_users", "identity:bob", "route:/api/users", "DELETE", false},

			// Viewer (Charlie) can only read
			{"viewer_can_get_users", "identity:charlie", "route:/api/users", "GET", true},
			{"viewer_cannot_post_users", "identity:charlie", "route:/api/users", "POST", false},
			{"viewer_cannot_put_users", "identity:charlie", "route:/api/users", "PUT", false},
			{"viewer_cannot_delete_users", "identity:charlie", "route:/api/users", "DELETE", false},

			// Data route permissions
			{"admin_can_delete_data", "identity:alice", "route:/api/data", "DELETE", true},
			{"developer_can_post_data", "identity:bob", "route:/api/data", "POST", true},
			{"developer_cannot_delete_data", "identity:bob", "route:/api/data", "DELETE", false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				resp, err := tc.evaluate(tt.subject, tt.resource, tt.action)
				require.NoError(t, err)
				require.Equal(t, tt.expected, resp.Decision, "expected %s to have %s=%v on %s", tt.subject, tt.action, tt.expected, tt.resource)
			})
		}
	})

	t.Run("gateway_mixed_public_and_role_access", func(t *testing.T) {
		tc := setupTestContext(t, "memory")
		tc.createStore("authzen-gateway-mixed")
		tc.writeModel(authzenGatewayModel)

		// Setup: Mix of public and role-based access
		tc.writeTuples([]*openfgav1.TupleKey{
			// Admin role
			{User: "identity:admin-user", Relation: "assignee", Object: "role:admin"},

			// Public GET on products (anyone can browse)
			{User: "identity:*", Relation: "GET", Object: "route:/api/products"},

			// Only admins can modify products
			{User: "role:admin#assignee", Relation: "POST", Object: "route:/api/products"},
			{User: "role:admin#assignee", Relation: "PUT", Object: "route:/api/products"},
			{User: "role:admin#assignee", Relation: "DELETE", Object: "route:/api/products"},
		})

		tests := []struct {
			name     string
			subject  string
			resource string
			action   string
			expected bool
		}{
			// Anyone can GET products
			{"anonymous_can_get_products", "identity:anonymous", "route:/api/products", "GET", true},
			{"regular_user_can_get_products", "identity:regular-user", "route:/api/products", "GET", true},
			{"admin_can_get_products", "identity:admin-user", "route:/api/products", "GET", true},

			// Only admin can modify products
			{"anonymous_cannot_post_products", "identity:anonymous", "route:/api/products", "POST", false},
			{"regular_user_cannot_post_products", "identity:regular-user", "route:/api/products", "POST", false},
			{"admin_can_post_products", "identity:admin-user", "route:/api/products", "POST", true},
			{"admin_can_delete_products", "identity:admin-user", "route:/api/products", "DELETE", true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				resp, err := tc.evaluate(tt.subject, tt.resource, tt.action)
				require.NoError(t, err)
				require.Equal(t, tt.expected, resp.Decision)
			})
		}
	})

	t.Run("gateway_evaluations_batch", func(t *testing.T) {
		tc := setupTestContext(t, "memory")
		tc.createStore("authzen-gateway-batch")
		tc.writeModel(authzenGatewayModel)

		// Setup
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "identity:alice", Relation: "assignee", Object: "role:admin"},
			{User: "identity:*", Relation: "GET", Object: "route:/api/public"},
			{User: "role:admin#assignee", Relation: "POST", Object: "route:/api/admin"},
			{User: "role:admin#assignee", Relation: "DELETE", Object: "route:/api/admin"},
		})

		// Batch evaluation for gateway access control
		resp, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: tc.storeID,
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject:  &authzenv1.Subject{Type: "identity", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "route", Id: "/api/public"},
					Action:   &authzenv1.Action{Name: "GET"},
				},
				{
					Subject:  &authzenv1.Subject{Type: "identity", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "route", Id: "/api/admin"},
					Action:   &authzenv1.Action{Name: "POST"},
				},
				{
					Subject:  &authzenv1.Subject{Type: "identity", Id: "bob"},
					Resource: &authzenv1.Resource{Type: "route", Id: "/api/public"},
					Action:   &authzenv1.Action{Name: "GET"},
				},
				{
					Subject:  &authzenv1.Subject{Type: "identity", Id: "bob"},
					Resource: &authzenv1.Resource{Type: "route", Id: "/api/admin"},
					Action:   &authzenv1.Action{Name: "DELETE"},
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.EvaluationResponses, 4)

		// Alice can GET public (wildcard)
		require.True(t, resp.EvaluationResponses[0].Decision, "Alice should be able to GET public route")
		// Alice can POST admin (admin role)
		require.True(t, resp.EvaluationResponses[1].Decision, "Alice should be able to POST to admin route")
		// Bob can GET public (wildcard)
		require.True(t, resp.EvaluationResponses[2].Decision, "Bob should be able to GET public route")
		// Bob cannot DELETE admin (no role)
		require.False(t, resp.EvaluationResponses[3].Decision, "Bob should not be able to DELETE admin route")
	})

	t.Run("gateway_subject_search", func(t *testing.T) {
		tc := setupTestContext(t, "memory")
		tc.createStore("authzen-gateway-search")
		tc.writeModel(authzenGatewayModel)

		// Setup multiple users with different access levels
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "identity:admin1", Relation: "assignee", Object: "role:admin"},
			{User: "identity:admin2", Relation: "assignee", Object: "role:admin"},
			{User: "identity:dev1", Relation: "assignee", Object: "role:developer"},

			{User: "role:admin#assignee", Relation: "DELETE", Object: "route:/api/critical"},
			{User: "role:developer#assignee", Relation: "GET", Object: "route:/api/critical"},
		})

		// Search for identities that can DELETE the critical route
		resp, err := tc.authzenClient.SubjectSearch(context.Background(), &authzenv1.SubjectSearchRequest{
			StoreId:  tc.storeID,
			Resource: &authzenv1.Resource{Type: "route", Id: "/api/critical"},
			Action:   &authzenv1.Action{Name: "DELETE"},
			Subject:  &authzenv1.SubjectFilter{Type: "identity"},
		})
		require.NoError(t, err)
		require.Len(t, resp.Subjects, 2)

		// Verify only admins can delete
		subjectIDs := make(map[string]bool)
		for _, s := range resp.Subjects {
			subjectIDs[s.Id] = true
		}
		require.True(t, subjectIDs["admin1"])
		require.True(t, subjectIDs["admin2"])
	})

	t.Run("gateway_resource_search", func(t *testing.T) {
		tc := setupTestContext(t, "memory")
		tc.createStore("authzen-gateway-resource-search")
		tc.writeModel(authzenGatewayModel)

		// Setup
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "identity:alice", Relation: "assignee", Object: "role:admin"},

			{User: "role:admin#assignee", Relation: "DELETE", Object: "route:/api/users"},
			{User: "role:admin#assignee", Relation: "DELETE", Object: "route:/api/products"},
			{User: "role:admin#assignee", Relation: "DELETE", Object: "route:/api/orders"},
		})

		// Search for routes that Alice can DELETE
		resp, err := tc.authzenClient.ResourceSearch(context.Background(), &authzenv1.ResourceSearchRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "identity", Id: "alice"},
			Action:   &authzenv1.Action{Name: "DELETE"},
			Resource: &authzenv1.Resource{Type: "route"},
		})
		require.NoError(t, err)
		require.Len(t, resp.Resources, 3)

		// Verify all routes are returned
		resourceIDs := make(map[string]bool)
		for _, r := range resp.Resources {
			resourceIDs[r.Id] = true
		}
		require.True(t, resourceIDs["/api/users"])
		require.True(t, resourceIDs["/api/products"])
		require.True(t, resourceIDs["/api/orders"])
	})
}

// TestAuthZENInteropCrossScenarios tests scenarios that combine aspects of both
// the Todo and Gateway applications.
func TestAuthZENInteropCrossScenarios(t *testing.T) {
	t.Run("multiple_stores_isolation", func(t *testing.T) {
		// Create two separate stores - one for Todo, one for Gateway
		// Verify that permissions in one store don't affect the other

		tcTodo := setupTestContext(t, "memory")
		tcTodo.createStore("todo-store")
		tcTodo.writeModel(authzenTodoModel)
		tcTodo.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "admin", Object: "todo:1"},
		})

		tcGateway := setupTestContext(t, "memory")
		tcGateway.createStore("gateway-store")
		tcGateway.writeModel(authzenGatewayModel)
		tcGateway.writeTuples([]*openfgav1.TupleKey{
			{User: "identity:bob", Relation: "assignee", Object: "role:admin"},
			{User: "role:admin#assignee", Relation: "DELETE", Object: "route:/api/test"},
		})

		// Verify Alice is admin in Todo store
		respTodo, err := tcTodo.evaluate("user:alice", "todo:1", "can_create_todo")
		require.NoError(t, err)
		require.True(t, respTodo.Decision)

		// Verify Bob has access in Gateway store
		respGateway, err := tcGateway.evaluate("identity:bob", "route:/api/test", "DELETE")
		require.NoError(t, err)
		require.True(t, respGateway.Decision)
	})
}
