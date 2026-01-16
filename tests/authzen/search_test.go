package authzen_test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

func TestSubjectSearch(t *testing.T) {
	t.Run("basic_subject_search", func(t *testing.T) {
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
			{User: "user:bob", Relation: "reader", Object: "document:doc1"},
			{User: "user:charlie", Relation: "reader", Object: "document:doc1"},
		})

		resp, err := tc.authzenClient.SubjectSearch(context.Background(), &authzenv1.SubjectSearchRequest{
			StoreId:  tc.storeID,
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetSubjects(), 3)

		// Verify all expected subjects are returned
		subjectIDs := make([]string, len(resp.GetSubjects()))
		for i, s := range resp.GetSubjects() {
			subjectIDs[i] = s.GetId()
		}
		sort.Strings(subjectIDs)
		require.Contains(t, subjectIDs, "alice")
		require.Contains(t, subjectIDs, "bob")
		require.Contains(t, subjectIDs, "charlie")
	})

	t.Run("empty_result", func(t *testing.T) {
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

		resp, err := tc.authzenClient.SubjectSearch(context.Background(), &authzenv1.SubjectSearchRequest{
			StoreId:  tc.storeID,
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
		})
		require.NoError(t, err)
		require.Empty(t, resp.GetSubjects())
	})

	t.Run("returns_all_results_ignores_page_parameter", func(t *testing.T) {
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

		// Create 10 users with reader access
		tuples := make([]*openfgav1.TupleKey, 10)
		for i := 0; i < 10; i++ {
			tuples[i] = &openfgav1.TupleKey{
				User:     fmt.Sprintf("user:user%02d", i),
				Relation: "reader",
				Object:   "document:doc1",
			}
		}
		tc.writeTuples(tuples)

		// Request with page limit - should be ignored, all results returned
		limit := uint32(3)
		resp, err := tc.authzenClient.SubjectSearch(context.Background(), &authzenv1.SubjectSearchRequest{
			StoreId:  tc.storeID,
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
			Page:     &authzenv1.PageRequest{Limit: &limit},
		})
		require.NoError(t, err)
		// All 10 users should be returned despite limit of 3 (pagination not supported)
		require.Len(t, resp.GetSubjects(), 10)
		// No Page response when pagination is not supported
		require.Nil(t, resp.GetPage())

		// Verify all expected subjects are returned
		subjectIDs := make(map[string]bool)
		for _, s := range resp.GetSubjects() {
			subjectIDs[s.GetId()] = true
		}
		for i := 0; i < 10; i++ {
			require.True(t, subjectIDs[fmt.Sprintf("user%02d", i)])
		}
	})

	t.Run("with_subject_type_filter", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type service_account
			type document
				relations
					define reader: [user, service_account]
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "reader", Object: "document:doc1"},
			{User: "user:bob", Relation: "reader", Object: "document:doc1"},
			{User: "service_account:sa1", Relation: "reader", Object: "document:doc1"},
		})

		// Search with subject type filter
		resp, err := tc.authzenClient.SubjectSearch(context.Background(), &authzenv1.SubjectSearchRequest{
			StoreId:  tc.storeID,
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "reader"},
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetSubjects(), 2)

		for _, s := range resp.GetSubjects() {
			require.Equal(t, "user", s.GetType())
		}
	})
}

func TestResourceSearch(t *testing.T) {
	t.Run("basic_resource_search", func(t *testing.T) {
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

		resp, err := tc.authzenClient.ResourceSearch(context.Background(), &authzenv1.ResourceSearchRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "reader"},
			Resource: &authzenv1.Resource{Type: "document"},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetResources(), 3)

		// Verify all expected resources are returned
		resourceIDs := make([]string, len(resp.GetResources()))
		for i, r := range resp.GetResources() {
			resourceIDs[i] = r.GetId()
		}
		sort.Strings(resourceIDs)
		require.Equal(t, []string{"doc1", "doc2", "doc3"}, resourceIDs)
	})

	t.Run("empty_result", func(t *testing.T) {
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

		resp, err := tc.authzenClient.ResourceSearch(context.Background(), &authzenv1.ResourceSearchRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "reader"},
			Resource: &authzenv1.Resource{Type: "document"},
		})
		require.NoError(t, err)
		require.Empty(t, resp.GetResources())
	})

	t.Run("transitive_relationships_via_group", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
			type document
				relations
					define reader: [user, group#member]
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "member", Object: "group:engineering"},
			{User: "group:engineering#member", Relation: "reader", Object: "document:doc1"},
			{User: "group:engineering#member", Relation: "reader", Object: "document:doc2"},
		})

		resp, err := tc.authzenClient.ResourceSearch(context.Background(), &authzenv1.ResourceSearchRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "reader"},
			Resource: &authzenv1.Resource{Type: "document"},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetResources(), 2)

		resourceIDs := make([]string, len(resp.GetResources()))
		for i, r := range resp.GetResources() {
			resourceIDs[i] = r.GetId()
		}
		sort.Strings(resourceIDs)
		require.Equal(t, []string{"doc1", "doc2"}, resourceIDs)
	})

	t.Run("transitive_relationships_via_parent", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type folder
				relations
					define viewer: [user]
			type document
				relations
					define parent: [folder]
					define viewer: [user] or viewer from parent
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "viewer", Object: "folder:root"},
			{User: "folder:root", Relation: "parent", Object: "document:doc1"},
			{User: "folder:root", Relation: "parent", Object: "document:doc2"},
		})

		resp, err := tc.authzenClient.ResourceSearch(context.Background(), &authzenv1.ResourceSearchRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "viewer"},
			Resource: &authzenv1.Resource{Type: "document"},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetResources(), 2)
	})

	t.Run("returns_all_results_ignores_page_parameter", func(t *testing.T) {
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

		// Create 10 documents with alice as reader
		tuples := make([]*openfgav1.TupleKey, 10)
		for i := 0; i < 10; i++ {
			tuples[i] = &openfgav1.TupleKey{
				User:     "user:alice",
				Relation: "reader",
				Object:   fmt.Sprintf("document:doc%02d", i),
			}
		}
		tc.writeTuples(tuples)

		// Request with page limit - should be ignored, all results returned
		limit := uint32(3)
		resp, err := tc.authzenClient.ResourceSearch(context.Background(), &authzenv1.ResourceSearchRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "reader"},
			Resource: &authzenv1.Resource{Type: "document"},
			Page:     &authzenv1.PageRequest{Limit: &limit},
		})
		require.NoError(t, err)
		// All 10 documents should be returned despite limit of 3 (pagination not supported)
		require.Len(t, resp.GetResources(), 10)
		// No Page response when pagination is not supported
		require.Nil(t, resp.GetPage())

		// Verify all expected resources are returned
		resourceIDs := make(map[string]bool)
		for _, r := range resp.GetResources() {
			resourceIDs[r.GetId()] = true
		}
		for i := 0; i < 10; i++ {
			require.True(t, resourceIDs[fmt.Sprintf("doc%02d", i)])
		}
	})

	t.Run("multiple_resource_types", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type document
				relations
					define reader: [user]
			type folder
				relations
					define reader: [user]
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "reader", Object: "document:doc1"},
			{User: "user:alice", Relation: "reader", Object: "folder:folder1"},
		})

		// Search for documents only
		resp, err := tc.authzenClient.ResourceSearch(context.Background(), &authzenv1.ResourceSearchRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "reader"},
			Resource: &authzenv1.Resource{Type: "document"},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetResources(), 1)
		require.Equal(t, "document", resp.GetResources()[0].GetType())
		require.Equal(t, "doc1", resp.GetResources()[0].GetId())
	})
}

func TestActionSearch(t *testing.T) {
	t.Run("basic_action_search", func(t *testing.T) {
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
					define owner: [user]
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "reader", Object: "document:doc1"},
			{User: "user:alice", Relation: "writer", Object: "document:doc1"},
			// alice is not owner
		})

		resp, err := tc.authzenClient.ActionSearch(context.Background(), &authzenv1.ActionSearchRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetActions(), 2) // reader and writer

		actionNames := make([]string, len(resp.GetActions()))
		for i, a := range resp.GetActions() {
			actionNames[i] = a.GetName()
		}
		sort.Strings(actionNames)
		require.Equal(t, []string{"reader", "writer"}, actionNames)
	})

	t.Run("no_permissions", func(t *testing.T) {
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
		// No tuples written

		resp, err := tc.authzenClient.ActionSearch(context.Background(), &authzenv1.ActionSearchRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
		})
		require.NoError(t, err)
		require.Empty(t, resp.GetActions())
	})

	t.Run("all_permissions", func(t *testing.T) {
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
					define owner: [user]
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "reader", Object: "document:doc1"},
			{User: "user:alice", Relation: "writer", Object: "document:doc1"},
			{User: "user:alice", Relation: "owner", Object: "document:doc1"},
		})

		resp, err := tc.authzenClient.ActionSearch(context.Background(), &authzenv1.ActionSearchRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetActions(), 3)

		actionNames := make([]string, len(resp.GetActions()))
		for i, a := range resp.GetActions() {
			actionNames[i] = a.GetName()
		}
		sort.Strings(actionNames)
		require.Equal(t, []string{"owner", "reader", "writer"}, actionNames)
	})

	t.Run("transitive_permission", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type document
				relations
					define owner: [user]
					define writer: [user] or owner
					define reader: [user] or writer
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "owner", Object: "document:doc1"},
		})

		// Alice should have all permissions through ownership hierarchy
		resp, err := tc.authzenClient.ActionSearch(context.Background(), &authzenv1.ActionSearchRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetActions(), 3)

		actionNames := make([]string, len(resp.GetActions()))
		for i, a := range resp.GetActions() {
			actionNames[i] = a.GetName()
		}
		sort.Strings(actionNames)
		require.Equal(t, []string{"owner", "reader", "writer"}, actionNames)
	})

	t.Run("with_group_membership", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
			type document
				relations
					define reader: [user, group#member]
					define writer: [user, group#member]
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "member", Object: "group:engineering"},
			{User: "group:engineering#member", Relation: "reader", Object: "document:doc1"},
			// alice is not writer
		})

		resp, err := tc.authzenClient.ActionSearch(context.Background(), &authzenv1.ActionSearchRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetActions(), 1)
		require.Equal(t, "reader", resp.GetActions()[0].GetName())
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
					define reader: [user] but not blocked
					define writer: [user]
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "reader", Object: "document:doc1"},
			{User: "user:alice", Relation: "writer", Object: "document:doc1"},
			{User: "user:alice", Relation: "blocked", Object: "document:doc1"},
		})

		// Alice should have blocked and writer - reader is excluded because she's blocked
		resp, err := tc.authzenClient.ActionSearch(context.Background(), &authzenv1.ActionSearchRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetActions(), 2)

		actionNames := make(map[string]bool)
		for _, a := range resp.GetActions() {
			actionNames[a.GetName()] = true
		}
		require.True(t, actionNames["blocked"])
		require.True(t, actionNames["writer"])
		require.False(t, actionNames["reader"]) // reader should NOT be included due to exclusion
	})
}

func TestTransitiveRelationships(t *testing.T) {
	t.Run("nested_group_membership", func(t *testing.T) {
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
					define reader: [group#member]
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			// Alice is member of team-a
			{User: "user:alice", Relation: "member", Object: "group:team-a"},
			// team-a is member of engineering
			{User: "group:team-a#member", Relation: "member", Object: "group:engineering"},
			// engineering has reader access
			{User: "group:engineering#member", Relation: "reader", Object: "document:doc1"},
		})

		// Alice should have reader access through nested groups
		resp, err := tc.authzenClient.ResourceSearch(context.Background(), &authzenv1.ResourceSearchRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "reader"},
			Resource: &authzenv1.Resource{Type: "document"},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetResources(), 1)
		require.Equal(t, "doc1", resp.GetResources()[0].GetId())
	})

	t.Run("folder_hierarchy", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type folder
				relations
					define parent: [folder]
					define viewer: [user] or viewer from parent
			type document
				relations
					define parent: [folder]
					define viewer: [user] or viewer from parent
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			// Alice has viewer on root folder
			{User: "user:alice", Relation: "viewer", Object: "folder:root"},
			// projects is child of root
			{User: "folder:root", Relation: "parent", Object: "folder:projects"},
			// doc1 is in projects folder
			{User: "folder:projects", Relation: "parent", Object: "document:doc1"},
		})

		// Alice should have viewer access to doc1 through folder hierarchy
		resp, err := tc.authzenClient.ResourceSearch(context.Background(), &authzenv1.ResourceSearchRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "viewer"},
			Resource: &authzenv1.Resource{Type: "document"},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetResources(), 1)
		require.Equal(t, "doc1", resp.GetResources()[0].GetId())
	})

	t.Run("org_hierarchy", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")
		tc.writeModel(`
			model
				schema 1.1
			type user
			type organization
				relations
					define admin: [user]
					define member: [user] or admin
			type team
				relations
					define parent_org: [organization]
					define admin: [user] or admin from parent_org
					define member: [user] or member from parent_org or admin
			type project
				relations
					define parent_team: [team]
					define viewer: [user] or admin from parent_team
		`)
		tc.writeTuples([]*openfgav1.TupleKey{
			{User: "user:alice", Relation: "admin", Object: "organization:acme"},
			{User: "organization:acme", Relation: "parent_org", Object: "team:engineering"},
			{User: "team:engineering", Relation: "parent_team", Object: "project:project1"},
		})

		// Alice should have viewer access to project through org admin hierarchy
		resp, err := tc.authzenClient.ResourceSearch(context.Background(), &authzenv1.ResourceSearchRequest{
			StoreId:  tc.storeID,
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "viewer"},
			Resource: &authzenv1.Resource{Type: "project"},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetResources(), 1)
		require.Equal(t, "project1", resp.GetResources()[0].GetId())
	})
}
