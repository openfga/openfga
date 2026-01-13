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
		tc := setupTestContext(t, "memory")
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
		require.True(t, resp.Decision)
	})

	t.Run("basic_deny", func(t *testing.T) {
		tc := setupTestContext(t, "memory")
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
		require.False(t, resp.Decision)
	})

	t.Run("with_abac_condition", func(t *testing.T) {
		tc := setupTestContext(t, "memory")
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
		require.True(t, resp.Decision)

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
		require.False(t, resp.Decision)
	})

	t.Run("properties_in_conditions", func(t *testing.T) {
		tc := setupTestContext(t, "memory")
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
		require.True(t, resp.Decision)

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
		require.False(t, resp.Decision)
	})

	t.Run("interop_with_check", func(t *testing.T) {
		tc := setupTestContext(t, "memory")
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
		require.Equal(t, checkResp.Allowed, evalResp.Decision)

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
		require.Equal(t, checkResp.Allowed, evalResp.Decision)

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
		require.Equal(t, checkResp.Allowed, evalResp.Decision)
		require.False(t, evalResp.Decision)
	})

	t.Run("hierarchical_permissions", func(t *testing.T) {
		tc := setupTestContext(t, "memory")
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
		require.True(t, resp.Decision)

		// Bob should not have access
		resp, err = tc.evaluate("user:bob", "document:doc1", "viewer")
		require.NoError(t, err)
		require.False(t, resp.Decision)
	})

	t.Run("with_groups", func(t *testing.T) {
		tc := setupTestContext(t, "memory")
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
		require.True(t, resp.Decision)

		// Bob should not have access
		resp, err = tc.evaluate("user:bob", "document:doc1", "viewer")
		require.NoError(t, err)
		require.False(t, resp.Decision)
	})

	t.Run("exclusion_pattern", func(t *testing.T) {
		tc := setupTestContext(t, "memory")
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
		require.True(t, resp.Decision)

		// Bob should be denied - blocked takes precedence
		resp, err = tc.evaluate("user:bob", "document:doc1", "viewer")
		require.NoError(t, err)
		require.False(t, resp.Decision)
	})

	t.Run("intersection_pattern", func(t *testing.T) {
		tc := setupTestContext(t, "memory")
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
		require.True(t, resp.Decision)

		// Bob should be denied - only one condition met
		resp, err = tc.evaluate("user:bob", "document:doc1", "can_access")
		require.NoError(t, err)
		require.False(t, resp.Decision)
	})

	t.Run("complex_condition_with_multiple_params", func(t *testing.T) {
		tc := setupTestContext(t, "memory")
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
		require.True(t, resp.Decision)

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
		require.True(t, resp.Decision)

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
		require.False(t, resp.Decision)
	})
}
