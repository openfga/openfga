package authzen_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
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

		// Default behavior (EXECUTE_ALL) - all evaluations should be processed
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

		// Explicit EXECUTE_ALL - all evaluations should be processed
		resp, err := tc.authzenClient.Evaluations(context.Background(), &authzenv1.EvaluationsRequest{
			StoreId: tc.storeID,
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "reader"},
			Options: &authzenv1.EvaluationsOptions{
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_EXECUTE_ALL,
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
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_DENY_ON_FIRST_DENY,
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
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_DENY_ON_FIRST_DENY,
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
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_DENY_ON_FIRST_DENY,
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
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_PERMIT_ON_FIRST_PERMIT,
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
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_PERMIT_ON_FIRST_PERMIT,
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
				EvaluationsSemantic: authzenv1.EvaluationsSemantic_PERMIT_ON_FIRST_PERMIT,
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
}
