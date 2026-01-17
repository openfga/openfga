package authzen_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/testutils"
)

func TestEvaluations(t *testing.T) {
	t.Run("batch_evaluations", func(t *testing.T) {
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
			// alice is not a writer
		})

		resp, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{Action: &authzenv1.Action{Name: "reader"}},
				{Action: &authzenv1.Action{Name: "writer"}},
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 2)
		require.True(t, resp.GetEvaluationResponses()[0].GetDecision())  // reader - allowed
		require.False(t, resp.GetEvaluationResponses()[1].GetDecision()) // writer - denied
	})

	t.Run("default_value_inheritance", func(t *testing.T) {
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
			{User: "user:alice", Relation: "reader", Object: "document:doc2"},
		})

		// All evaluations inherit subject and action from top level
		resp, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "reader"},
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}},
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc2"}},
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc3"}}, // Not allowed
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 3)
		require.True(t, resp.GetEvaluationResponses()[0].GetDecision())
		require.True(t, resp.GetEvaluationResponses()[1].GetDecision())
		require.False(t, resp.GetEvaluationResponses()[2].GetDecision())
	})

	t.Run("semantic_execute_all_default", func(t *testing.T) {
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
			// doc2 not allowed
			{User: "user:alice", Relation: "reader", Object: "document:doc3"},
		})

		// Default behavior (execute_all) - all evaluations should be processed
		resp, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "reader"},
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}}, // Allowed
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc2"}}, // Denied
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc3"}}, // Allowed
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 3) // All evaluations processed
		require.True(t, resp.GetEvaluationResponses()[0].GetDecision())
		require.False(t, resp.GetEvaluationResponses()[1].GetDecision())
		require.True(t, resp.GetEvaluationResponses()[2].GetDecision())
	})

	t.Run("semantic_execute_all_explicit", func(t *testing.T) {
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
			// doc2 not allowed
			{User: "user:alice", Relation: "reader", Object: "document:doc3"},
		})

		// Explicit execute_all - all evaluations should be processed
		resp, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "reader"},
			Options: &authzenv1.EvaluationsOptions{
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_execute_all,
			},
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}}, // Allowed
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc2"}}, // Denied
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc3"}}, // Allowed
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 3) // All evaluations processed
		require.True(t, resp.GetEvaluationResponses()[0].GetDecision())
		require.False(t, resp.GetEvaluationResponses()[1].GetDecision())
		require.True(t, resp.GetEvaluationResponses()[2].GetDecision())
	})

	t.Run("semantic_deny_on_first_deny", func(t *testing.T) {
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
			// doc2 not allowed
			{User: "user:alice", Relation: "reader", Object: "document:doc3"},
		})

		resp, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "reader"},
			Options: &authzenv1.EvaluationsOptions{
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_deny_on_first_deny,
			},
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}}, // Allowed
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc2"}}, // Denied - should stop here
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc3"}}, // Would be allowed but not evaluated
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 2) // Should only have 2 responses
		require.True(t, resp.GetEvaluationResponses()[0].GetDecision())
		require.False(t, resp.GetEvaluationResponses()[1].GetDecision())
	})

	t.Run("semantic_deny_on_first_deny_first_item_denied", func(t *testing.T) {
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
			{User: "user:alice", Relation: "reader", Object: "document:doc2"},
			{User: "user:alice", Relation: "reader", Object: "document:doc3"},
		})

		resp, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "reader"},
			Options: &authzenv1.EvaluationsOptions{
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_deny_on_first_deny,
			},
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}}, // Denied - should stop here
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc2"}}, // Would be allowed but not evaluated
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc3"}}, // Would be allowed but not evaluated
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 1) // Should only have 1 response
		require.False(t, resp.GetEvaluationResponses()[0].GetDecision())
	})

	t.Run("semantic_deny_on_first_deny_all_permitted", func(t *testing.T) {
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
			{User: "user:alice", Relation: "reader", Object: "document:doc2"},
			{User: "user:alice", Relation: "reader", Object: "document:doc3"},
		})

		resp, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "reader"},
			Options: &authzenv1.EvaluationsOptions{
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_deny_on_first_deny,
			},
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}},
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc2"}},
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc3"}},
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 3) // All should be evaluated
		require.True(t, resp.GetEvaluationResponses()[0].GetDecision())
		require.True(t, resp.GetEvaluationResponses()[1].GetDecision())
		require.True(t, resp.GetEvaluationResponses()[2].GetDecision())
	})

	t.Run("semantic_permit_on_first_permit", func(t *testing.T) {
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
			// doc1 not allowed
			{User: "user:alice", Relation: "reader", Object: "document:doc2"},
			{User: "user:alice", Relation: "reader", Object: "document:doc3"},
		})

		resp, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "reader"},
			Options: &authzenv1.EvaluationsOptions{
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_permit_on_first_permit,
			},
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}}, // Denied
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc2"}}, // Allowed - should stop here
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc3"}}, // Would be allowed but not evaluated
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 2) // Should only have 2 responses
		require.False(t, resp.GetEvaluationResponses()[0].GetDecision())
		require.True(t, resp.GetEvaluationResponses()[1].GetDecision())
	})

	t.Run("semantic_permit_on_first_permit_first_item_permitted", func(t *testing.T) {
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
			{User: "user:alice", Relation: "reader", Object: "document:doc2"},
		})

		resp, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "reader"},
			Options: &authzenv1.EvaluationsOptions{
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_permit_on_first_permit,
			},
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}}, // Allowed - should stop here
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc2"}}, // Would be allowed but not evaluated
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc3"}}, // Would be denied but not evaluated
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 1) // Should only have 1 response
		require.True(t, resp.GetEvaluationResponses()[0].GetDecision())
	})

	t.Run("semantic_permit_on_first_permit_all_denied", func(t *testing.T) {
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
		// No tuples - all denied

		resp, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "reader"},
			Options: &authzenv1.EvaluationsOptions{
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_permit_on_first_permit,
			},
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}},
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc2"}},
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc3"}},
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 3) // All should be evaluated
		require.False(t, resp.GetEvaluationResponses()[0].GetDecision())
		require.False(t, resp.GetEvaluationResponses()[1].GetDecision())
		require.False(t, resp.GetEvaluationResponses()[2].GetDecision())
	})

	t.Run("response_ordering", func(t *testing.T) {
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

		// Order: deny, permit, deny, permit, deny
		resp, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "reader"},
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc0"}}, // deny
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}}, // permit
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc2"}}, // deny
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}}, // permit (duplicate)
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc3"}}, // deny
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 5)
		require.False(t, resp.GetEvaluationResponses()[0].GetDecision())
		require.True(t, resp.GetEvaluationResponses()[1].GetDecision())
		require.False(t, resp.GetEvaluationResponses()[2].GetDecision())
		require.True(t, resp.GetEvaluationResponses()[3].GetDecision())
		require.False(t, resp.GetEvaluationResponses()[4].GetDecision())
	})

	t.Run("mixed_inheritance_per_item", func(t *testing.T) {
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
			{User: "user:bob", Relation: "writer", Object: "document:doc1"},
		})

		// Subject inherited, but action and resource specified per item
		resp, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "reader"},
				},
				{
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "writer"},
				},
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "bob"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "writer"},
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 3)
		require.True(t, resp.GetEvaluationResponses()[0].GetDecision())  // alice is reader
		require.False(t, resp.GetEvaluationResponses()[1].GetDecision()) // alice is not writer
		require.True(t, resp.GetEvaluationResponses()[2].GetDecision())  // bob is writer
	})

	t.Run("empty_evaluations_list", func(t *testing.T) {
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

		// Empty evaluations list returns an error because BatchCheck requires at least one check
		_, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId:     tc.storeID,
			Subject:     &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:      &authzenv1.Action{Name: "reader"},
			Evaluations: []*authzenv1.EvaluationsItemRequest{},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "batch check requires at least one check")
	})

	t.Run("single_evaluation", func(t *testing.T) {
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

		resp, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "reader"},
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}},
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 1)
		require.True(t, resp.GetEvaluationResponses()[0].GetDecision())
	})

	t.Run("multiple_subjects_different_resources", func(t *testing.T) {
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
			{User: "user:bob", Relation: "reader", Object: "document:doc2"},
		})

		resp, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: tc.storeID,
			Action:  &authzenv1.Action{Name: "reader"},
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
				},
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc2"},
				},
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "bob"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
				},
				{
					Subject:  &authzenv1.Subject{Type: "user", Id: "bob"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc2"},
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 4)
		require.True(t, resp.GetEvaluationResponses()[0].GetDecision())  // alice can read doc1
		require.False(t, resp.GetEvaluationResponses()[1].GetDecision()) // alice cannot read doc2
		require.False(t, resp.GetEvaluationResponses()[2].GetDecision()) // bob cannot read doc1
		require.True(t, resp.GetEvaluationResponses()[3].GetDecision())  // bob can read doc2
	})

	// AuthZEN spec section 7.1.1: Context is inherited from top-level defaults
	t.Run("context_inheritance_from_default", func(t *testing.T) {
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
			{User: "user:alice", Relation: "reader", Object: "document:doc2", Condition: &openfgav1.RelationshipCondition{Name: "office_hours"}},
		})

		// Default context with office hours - all evaluations should inherit this
		resp, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "reader"},
			Context: testutils.MustNewStruct(t, map[string]interface{}{"current_hour": 10}),
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}},
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc2"}},
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 2)
		require.True(t, resp.GetEvaluationResponses()[0].GetDecision(), "doc1 should be allowed with inherited context")
		require.True(t, resp.GetEvaluationResponses()[1].GetDecision(), "doc2 should be allowed with inherited context")

		// Outside office hours - all should be denied
		resp, err = tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "reader"},
			Context: testutils.MustNewStruct(t, map[string]interface{}{"current_hour": 22}),
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}},
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc2"}},
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 2)
		require.False(t, resp.GetEvaluationResponses()[0].GetDecision(), "doc1 should be denied outside office hours")
		require.False(t, resp.GetEvaluationResponses()[1].GetDecision(), "doc2 should be denied outside office hours")
	})

	// AuthZEN spec section 7.1.1: Per-evaluation context overrides default context
	t.Run("context_override_per_evaluation", func(t *testing.T) {
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
			{User: "user:alice", Relation: "reader", Object: "document:doc2", Condition: &openfgav1.RelationshipCondition{Name: "office_hours"}},
		})

		// Default context is outside office hours, but second evaluation overrides with valid hours
		resp, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "reader"},
			Context: testutils.MustNewStruct(t, map[string]interface{}{"current_hour": 22}), // Default: outside hours
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}}, // Uses default context
				{
					Resource: &authzenv1.Resource{Type: "document", Id: "doc2"},
					Context:  testutils.MustNewStruct(t, map[string]interface{}{"current_hour": 10}), // Override: within hours
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetEvaluationResponses(), 2)
		require.False(t, resp.GetEvaluationResponses()[0].GetDecision(), "doc1 should be denied with default context (hour=22)")
		require.True(t, resp.GetEvaluationResponses()[1].GetDecision(), "doc2 should be allowed with overridden context (hour=10)")
	})

	// AuthZEN spec section 7.2.1: Per-evaluation errors
	t.Run("per_evaluation_errors", func(t *testing.T) {
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

		// Mix valid and invalid requests in deny_on_first_deny semantic
		// Invalid type should cause an error for that specific evaluation
		resp, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "reader"},
			Options: &authzenv1.EvaluationsOptions{
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_deny_on_first_deny,
			},
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}},               // Valid - allowed
				{Resource: &authzenv1.Resource{Type: "invalid_type_not_in_model", Id: "x"}}, // Invalid type - error
			},
		})
		require.NoError(t, err) // Overall request succeeds
		require.Len(t, resp.GetEvaluationResponses(), 2)
		require.True(t, resp.GetEvaluationResponses()[0].GetDecision())  // First is allowed
		require.False(t, resp.GetEvaluationResponses()[1].GetDecision()) // Error results in false decision
		// Per spec, error info is in context.error
		require.NotNil(t, resp.GetEvaluationResponses()[1].GetContext())
		require.NotNil(t, resp.GetEvaluationResponses()[1].GetContext().GetError())
	})

	// AuthZEN spec section 3: Feature disabled should return error
	t.Run("requires_experimental_flag", func(t *testing.T) {
		tc := setupTestContextWithExperimentals(t, []string{})
		// Create a store to get a valid store ID
		resp, err := tc.openfgaClient.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)

		_, err = tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: resp.GetId(),
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "reader"},
			Evaluations: []*authzenv1.EvaluationsItemRequest{
				{Resource: &authzenv1.Resource{Type: "document", Id: "doc1"}},
			},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "AuthZEN endpoints are experimental")
	})
}
