package authzen_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/testutils"
)

func TestEvaluation(t *testing.T) {
	t.Run("basic_permit", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type document
				relations
					define reader: [user]
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "reader", Object: "document:doc1"},
		})

		resp, err := tc.evaluate("user:alice", "document:doc1", "reader")
		require.NoError(t, err)
		require.True(t, resp.GetDecision())
	})

	t.Run("basic_deny", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type document
				relations
					define reader: [user]
		`)
		// No tuples written

		resp, err := tc.evaluate("user:alice", "document:doc1", "reader")
		require.NoError(t, err)
		require.False(t, resp.GetDecision())
	})

	t.Run("with_abac_condition", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type document
				relations
					define reader: [user with office_hours]

			condition office_hours(current_hour: int) {
				current_hour >= 9 && current_hour <= 17
			}
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "reader", Object: "document:doc1", Condition: &openfgav1.RelationshipCondition{Name: "office_hours"}},
		})

		// Test within office hours
		ctx := testutils.MustNewStruct(t, map[string]interface{}{"current_hour": 10})
		resp, err := tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
			Context:  ctx,
		})
		require.NoError(t, err)
		require.True(t, resp.GetDecision())

		// Test outside office hours
		ctx = testutils.MustNewStruct(t, map[string]interface{}{"current_hour": 22})
		resp, err = tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
			Context:  ctx,
		})
		require.NoError(t, err)
		require.False(t, resp.GetDecision())
	})

	t.Run("properties_in_conditions", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type document
				relations
					define reader: [user with department_match]

			condition department_match(subject_department: string, resource_department: string) {
				subject_department == resource_department
			}
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "reader", Object: "document:doc1", Condition: &openfgav1.RelationshipCondition{Name: "department_match"}},
		})

		// Test with matching departments using properties
		resp, err := tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"department": "engineering"}),
			},
			Resource: &authzenv1.Resource{
				Type:       "document",
				Id:         "doc1",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"department": "engineering"}),
			},
			Action: &authzenv1.Action{Name: "reader"},
			Context: testutils.MustNewStruct(t, map[string]interface{}{
				"subject_department":  "engineering",
				"resource_department": "engineering",
			}),
		})
		require.NoError(t, err)
		require.True(t, resp.GetDecision())

		// Test with non-matching departments
		resp, err = tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"department": "engineering"}),
			},
			Resource: &authzenv1.Resource{
				Type:       "document",
				Id:         "doc1",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"department": "sales"}),
			},
			Action: &authzenv1.Action{Name: "reader"},
			Context: testutils.MustNewStruct(t, map[string]interface{}{
				"subject_department":  "engineering",
				"resource_department": "sales",
			}),
		})
		require.NoError(t, err)
		require.False(t, resp.GetDecision())
	})

	t.Run("interop_with_check", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type document
				relations
					define reader: [user]
					define writer: [user]
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "reader", Object: "document:doc1"},
			{User: "user:alice", Relation: "writer", Object: "document:doc1"},
		})

		// AuthZEN Evaluation should match OpenFGA Check for reader
		evalResp, err := tc.evaluate("user:alice", "document:doc1", "reader")
		require.NoError(t, err)

		checkResp, err := tc.openfgaClient.Check(context.Background(), &openfgav1.CheckRequest{
			StoreId: tc.storeID,
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     "user:alice",
				Relation: "reader",
				Object:   "document:doc1",
			},
		})
		require.NoError(t, err)
		require.Equal(t, checkResp.GetAllowed(), evalResp.GetDecision())

		// AuthZEN Evaluation should match OpenFGA Check for writer
		evalResp, err = tc.evaluate("user:alice", "document:doc1", "writer")
		require.NoError(t, err)

		checkResp, err = tc.openfgaClient.Check(context.Background(), &openfgav1.CheckRequest{
			StoreId: tc.storeID,
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     "user:alice",
				Relation: "writer",
				Object:   "document:doc1",
			},
		})
		require.NoError(t, err)
		require.Equal(t, checkResp.GetAllowed(), evalResp.GetDecision())

		// Test deny case - bob has no permissions
		evalResp, err = tc.evaluate("user:bob", "document:doc1", "reader")
		require.NoError(t, err)

		checkResp, err = tc.openfgaClient.Check(context.Background(), &openfgav1.CheckRequest{
			StoreId: tc.storeID,
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     "user:bob",
				Relation: "reader",
				Object:   "document:doc1",
			},
		})
		require.NoError(t, err)
		require.Equal(t, checkResp.GetAllowed(), evalResp.GetDecision())
		require.False(t, evalResp.GetDecision())
	})

	t.Run("hierarchical_permissions", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type folder
				relations
					define owner: [user]
					define viewer: [user] or owner
			type document
				relations
					define parent: [folder]
					define owner: [user]
					define viewer: [user] or owner or viewer from parent
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "owner", Object: "folder:root"},
			{User: "folder:root", Relation: "parent", Object: "document:doc1"},
		})

		// Alice should have viewer access through folder ownership hierarchy
		resp, err := tc.evaluate("user:alice", "document:doc1", "viewer")
		require.NoError(t, err)
		require.True(t, resp.GetDecision())

		// Bob should not have access
		resp, err = tc.evaluate("user:bob", "document:doc1", "viewer")
		require.NoError(t, err)
		require.False(t, resp.GetDecision())
	})

	t.Run("with_groups", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user, group#member]
			type document
				relations
					define viewer: [user, group#member]
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "member", Object: "group:engineering"},
			{User: "group:engineering#member", Relation: "viewer", Object: "document:doc1"},
		})

		// Alice should have access through group membership
		resp, err := tc.evaluate("user:alice", "document:doc1", "viewer")
		require.NoError(t, err)
		require.True(t, resp.GetDecision())

		// Bob should not have access
		resp, err = tc.evaluate("user:bob", "document:doc1", "viewer")
		require.NoError(t, err)
		require.False(t, resp.GetDecision())
	})

	t.Run("exclusion_pattern", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type document
				relations
					define blocked: [user]
					define viewer: [user] but not blocked
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "viewer", Object: "document:doc1"},
			{User: "user:bob", Relation: "viewer", Object: "document:doc1"},
			{User: "user:bob", Relation: "blocked", Object: "document:doc1"},
		})

		// Alice should have access - not blocked
		resp, err := tc.evaluate("user:alice", "document:doc1", "viewer")
		require.NoError(t, err)
		require.True(t, resp.GetDecision())

		// Bob should be denied - blocked takes precedence
		resp, err = tc.evaluate("user:bob", "document:doc1", "viewer")
		require.NoError(t, err)
		require.False(t, resp.GetDecision())
	})

	t.Run("intersection_pattern", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type document
				relations
					define in_allowed_region: [user]
					define has_license: [user]
					define can_access: in_allowed_region and has_license
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "in_allowed_region", Object: "document:doc1"},
			{User: "user:alice", Relation: "has_license", Object: "document:doc1"},
			{User: "user:bob", Relation: "in_allowed_region", Object: "document:doc1"},
			// Bob doesn't have a license
		})

		// Alice should have access - both conditions met
		resp, err := tc.evaluate("user:alice", "document:doc1", "can_access")
		require.NoError(t, err)
		require.True(t, resp.GetDecision())

		// Bob should be denied - only one condition met
		resp, err = tc.evaluate("user:bob", "document:doc1", "can_access")
		require.NoError(t, err)
		require.False(t, resp.GetDecision())
	})

	t.Run("complex_condition_with_multiple_params", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type resource
				relations
					define can_access: [user with access_policy]

			condition access_policy(user_level: int, required_level: int, is_admin: bool) {
				is_admin || user_level >= required_level
			}
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "can_access", Object: "resource:secret", Condition: &openfgav1.RelationshipCondition{Name: "access_policy"}},
			{User: "user:bob", Relation: "can_access", Object: "resource:secret", Condition: &openfgav1.RelationshipCondition{Name: "access_policy"}},
		})

		// Alice is an admin - should have access regardless of level
		ctx := testutils.MustNewStruct(t, map[string]interface{}{
			"user_level":     1,
			"required_level": 5,
			"is_admin":       true,
		})
		resp, err := tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "resource", Id: "secret"},
			Action:   &authzenv1.Action{Name: "can_access"},
			Context:  ctx,
		})
		require.NoError(t, err)
		require.True(t, resp.GetDecision())

		// Bob has sufficient level
		ctx = testutils.MustNewStruct(t, map[string]interface{}{
			"user_level":     7,
			"required_level": 5,
			"is_admin":       false,
		})
		resp, err = tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "bob"},
			Resource: &authzenv1.Resource{Type: "resource", Id: "secret"},
			Action:   &authzenv1.Action{Name: "can_access"},
			Context:  ctx,
		})
		require.NoError(t, err)
		require.True(t, resp.GetDecision())

		// Bob with insufficient level
		ctx = testutils.MustNewStruct(t, map[string]interface{}{
			"user_level":     3,
			"required_level": 5,
			"is_admin":       false,
		})
		resp, err = tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "bob"},
			Resource: &authzenv1.Resource{Type: "resource", Id: "secret"},
			Action:   &authzenv1.Action{Name: "can_access"},
			Context:  ctx,
		})
		require.NoError(t, err)
		require.False(t, resp.GetDecision())
	})

	t.Run("properties_auto_merge_with_underscore_separator", func(t *testing.T) {
		// This test verifies that AuthZEN properties are automatically merged
		// into the OpenFGA context using underscore as separator (e.g., subject_department)
		// because OpenFGA does not allow condition parameters with "." in names.
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type document
				relations
					define reader: [user with same_department]

			condition same_department(subject_department: string, resource_department: string) {
				subject_department == resource_department
			}
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "reader", Object: "document:doc1", Condition: &openfgav1.RelationshipCondition{Name: "same_department"}},
		})

		// Test that subject.properties.department becomes subject_department in context
		// and resource.properties.department becomes resource_department in context
		resp, err := tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"department": "engineering"}),
			},
			Resource: &authzenv1.Resource{
				Type:       "document",
				Id:         "doc1",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"department": "engineering"}),
			},
			Action: &authzenv1.Action{Name: "reader"},
		})
		require.NoError(t, err)
		require.True(t, resp.GetDecision(), "Expected permit when departments match via properties merge")

		// Test non-matching departments
		resp, err = tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"department": "engineering"}),
			},
			Resource: &authzenv1.Resource{
				Type:       "document",
				Id:         "doc1",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"department": "sales"}),
			},
			Action: &authzenv1.Action{Name: "reader"},
		})
		require.NoError(t, err)
		require.False(t, resp.GetDecision(), "Expected deny when departments don't match")
	})

	t.Run("properties_context_precedence", func(t *testing.T) {
		// This test verifies that explicit context values take precedence over properties
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type document
				relations
					define reader: [user with check_level]

			condition check_level(subject_level: int) {
				subject_level >= 5
			}
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "reader", Object: "document:doc1", Condition: &openfgav1.RelationshipCondition{Name: "check_level"}},
		})

		// Test that properties would deny (level=1) but context override permits (level=10)
		resp, err := tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"level": 1}),
			},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
			Context:  testutils.MustNewStruct(t, map[string]interface{}{"subject_level": 10}),
		})
		require.NoError(t, err)
		require.True(t, resp.GetDecision(), "Expected permit because context overrides properties")

		// Verify properties alone would deny
		resp, err = tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"level": 1}),
			},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
		})
		require.NoError(t, err)
		require.False(t, resp.GetDecision(), "Expected deny because properties have level=1 < 5")
	})

	t.Run("action_properties_merge", func(t *testing.T) {
		// This test verifies that action properties are also merged with action_ prefix
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type document
				relations
					define reader: [user with check_method]

			condition check_method(action_method: string) {
				action_method == "GET"
			}
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "reader", Object: "document:doc1", Condition: &openfgav1.RelationshipCondition{Name: "check_method"}},
		})

		// Test action properties merge
		resp, err := tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action: &authzenv1.Action{
				Name:       "reader",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"method": "GET"}),
			},
		})
		require.NoError(t, err)
		require.True(t, resp.GetDecision(), "Expected permit when action.method matches")

		// Test with non-matching method
		resp, err = tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action: &authzenv1.Action{
				Name:       "reader",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"method": "POST"}),
			},
		})
		require.NoError(t, err)
		require.False(t, resp.GetDecision(), "Expected deny when action.method doesn't match")
	})

	// Tests for complex property types (arrays, nested objects) per AuthZEN spec
	// The AuthZEN spec states properties can contain "complex values, such as arrays and objects"

	t.Run("complex_properties_array_of_strings", func(t *testing.T) {
		// This test verifies that array properties are properly mapped to list<string> condition parameters
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type document
				relations
					define reader: [user with check_roles]

			condition check_roles(subject_roles: list<string>) {
				"admin" in subject_roles || "editor" in subject_roles
			}
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "reader", Object: "document:doc1", Condition: &openfgav1.RelationshipCondition{Name: "check_roles"}},
		})

		// Test with roles array containing "admin" - should permit
		resp, err := tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"roles": []interface{}{"admin", "viewer"}}),
			},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
		})
		require.NoError(t, err)
		require.True(t, resp.GetDecision(), "Expected permit when subject has admin role in array")

		// Test with roles array containing "editor" - should permit
		resp, err = tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"roles": []interface{}{"editor"}}),
			},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
		})
		require.NoError(t, err)
		require.True(t, resp.GetDecision(), "Expected permit when subject has editor role in array")

		// Test with roles array without required roles - should deny
		resp, err = tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"roles": []interface{}{"viewer", "guest"}}),
			},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
		})
		require.NoError(t, err)
		require.False(t, resp.GetDecision(), "Expected deny when subject lacks required roles")
	})

	t.Run("complex_properties_map_string", func(t *testing.T) {
		// This test verifies that map properties with string values work correctly
		// OpenFGA map<T> has string keys and values of type T
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type document
				relations
					define reader: [user with check_metadata]

			condition check_metadata(subject_metadata: map<string>) {
				subject_metadata["role"] == "admin"
			}
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "reader", Object: "document:doc1", Condition: &openfgav1.RelationshipCondition{Name: "check_metadata"}},
		})

		// Test with metadata map containing admin role - should permit
		resp, err := tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{
				Type: "user",
				Id:   "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{
					"metadata": map[string]interface{}{
						"role":       "admin",
						"department": "engineering",
					},
				}),
			},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
		})
		require.NoError(t, err)
		require.True(t, resp.GetDecision(), "Expected permit when metadata role is admin")

		// Test with different role - should deny
		resp, err = tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{
				Type: "user",
				Id:   "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{
					"metadata": map[string]interface{}{
						"role":       "viewer",
						"department": "sales",
					},
				}),
			},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
		})
		require.NoError(t, err)
		require.False(t, resp.GetDecision(), "Expected deny when metadata role is not admin")
	})

	t.Run("complex_properties_map_int", func(t *testing.T) {
		// This test verifies that map properties with int values work correctly
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type document
				relations
					define reader: [user with check_limits]

			condition check_limits(resource_limits: map<int>) {
				resource_limits["max_views"] > 0 && resource_limits["max_views"] <= 100
			}
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "reader", Object: "document:doc1", Condition: &openfgav1.RelationshipCondition{Name: "check_limits"}},
		})

		// Test with valid limits - should permit
		resp, err := tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{
				Type: "document",
				Id:   "doc1",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{
					"limits": map[string]interface{}{
						"max_views":     float64(50),
						"max_downloads": float64(10),
					},
				}),
			},
			Action: &authzenv1.Action{Name: "reader"},
		})
		require.NoError(t, err)
		require.True(t, resp.GetDecision(), "Expected permit when max_views is within range")

		// Test with limit exceeding threshold - should deny
		resp, err = tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{
				Type: "document",
				Id:   "doc1",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{
					"limits": map[string]interface{}{
						"max_views":     float64(200),
						"max_downloads": float64(10),
					},
				}),
			},
			Action: &authzenv1.Action{Name: "reader"},
		})
		require.NoError(t, err)
		require.False(t, resp.GetDecision(), "Expected deny when max_views exceeds threshold")
	})

	t.Run("complex_properties_array_size_check", func(t *testing.T) {
		// This test verifies that array size checks work with list properties
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type document
				relations
					define reader: [user with check_permissions_count]

			condition check_permissions_count(subject_permissions: list<string>) {
				size(subject_permissions) >= 2
			}
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "reader", Object: "document:doc1", Condition: &openfgav1.RelationshipCondition{Name: "check_permissions_count"}},
		})

		// Test with enough permissions - should permit
		resp, err := tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"permissions": []interface{}{"read", "write", "delete"}}),
			},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
		})
		require.NoError(t, err)
		require.True(t, resp.GetDecision(), "Expected permit when subject has >= 2 permissions")

		// Test with insufficient permissions - should deny
		resp, err = tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"permissions": []interface{}{"read"}}),
			},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
		})
		require.NoError(t, err)
		require.False(t, resp.GetDecision(), "Expected deny when subject has < 2 permissions")
	})

	t.Run("complex_properties_resource_array", func(t *testing.T) {
		// This test verifies that resource array properties work correctly
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type server
				relations
					define can_connect: [user with check_port]

			condition check_port(resource_allowed_ports: list<int>, action_port: int) {
				action_port in resource_allowed_ports
			}
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "can_connect", Object: "server:prod-1", Condition: &openfgav1.RelationshipCondition{Name: "check_port"}},
		})

		// Test connecting to allowed port - should permit
		resp, err := tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{
				Type:       "server",
				Id:         "prod-1",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"allowed_ports": []interface{}{float64(80), float64(443), float64(8080)}}),
			},
			Action: &authzenv1.Action{
				Name:       "can_connect",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"port": float64(443)}),
			},
		})
		require.NoError(t, err)
		require.True(t, resp.GetDecision(), "Expected permit when connecting to allowed port")

		// Test connecting to disallowed port - should deny
		resp, err = tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{
				Type:       "server",
				Id:         "prod-1",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"allowed_ports": []interface{}{float64(80), float64(443), float64(8080)}}),
			},
			Action: &authzenv1.Action{
				Name:       "can_connect",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"port": float64(22)}),
			},
		})
		require.NoError(t, err)
		require.False(t, resp.GetDecision(), "Expected deny when connecting to disallowed port")
	})

	t.Run("complex_properties_combined_types", func(t *testing.T) {
		// This test verifies that multiple complex property types work together
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type folder
				relations
					define can_upload: [user with upload_policy]

			condition upload_policy(subject_tags: list<string>, resource_allowed_users: list<string>, action_max_size: int) {
				"verified" in subject_tags && size(resource_allowed_users) > 0 && action_max_size > 0
			}
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "can_upload", Object: "folder:shared", Condition: &openfgav1.RelationshipCondition{Name: "upload_policy"}},
		})

		// Test with all conditions met - should permit
		resp, err := tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{
				Type: "user",
				Id:   "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{
					"tags": []interface{}{"vip", "verified"},
				}),
			},
			Resource: &authzenv1.Resource{
				Type: "folder",
				Id:   "shared",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{
					"allowed_users": []interface{}{"alice", "bob", "charlie"},
				}),
			},
			Action: &authzenv1.Action{
				Name: "can_upload",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{
					"max_size": float64(10485760),
				}),
			},
		})
		require.NoError(t, err)
		require.True(t, resp.GetDecision(), "Expected permit when all conditions are met")

		// Test without "verified" tag - should deny
		resp, err = tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{
				Type: "user",
				Id:   "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{
					"tags": []interface{}{"vip", "guest"},
				}),
			},
			Resource: &authzenv1.Resource{
				Type: "folder",
				Id:   "shared",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{
					"allowed_users": []interface{}{"alice", "bob"},
				}),
			},
			Action: &authzenv1.Action{
				Name: "can_upload",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{
					"max_size": float64(10485760),
				}),
			},
		})
		require.NoError(t, err)
		require.False(t, resp.GetDecision(), "Expected deny when subject lacks 'verified' tag")
	})

	t.Run("complex_properties_empty_array", func(t *testing.T) {
		// This test verifies that empty arrays are handled correctly
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type document
				relations
					define reader: [user with check_empty_roles]

			condition check_empty_roles(subject_roles: list<string>) {
				size(subject_roles) == 0
			}
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "reader", Object: "document:doc1", Condition: &openfgav1.RelationshipCondition{Name: "check_empty_roles"}},
		})

		// Test with empty roles array - should permit
		resp, err := tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"roles": []interface{}{}}),
			},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
		})
		require.NoError(t, err)
		require.True(t, resp.GetDecision(), "Expected permit when roles array is empty")

		// Test with non-empty roles array - should deny
		resp, err = tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"roles": []interface{}{"admin"}}),
			},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
		})
		require.NoError(t, err)
		require.False(t, resp.GetDecision(), "Expected deny when roles array is not empty")
	})

	// AuthZEN spec section 3: Feature disabled should return error
	t.Run("requires_experimental_flag", func(t *testing.T) {
		tc := setupTestContextWithExperimentals(t, []string{})
		// Create a store to get a valid store ID
		resp, err := tc.openfgaClient.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)

		_, err = tc.authzenClient.Evaluation(context.Background(), &authzenv1.EvaluationRequest{
			StoreId:  resp.GetId(),
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "AuthZEN endpoints are experimental")
	})
}
