package commands

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/testutils"
)

func TestSubjectSearchQuery(t *testing.T) {
	t.Run("basic_search", func(t *testing.T) {
		mockListUsers := func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
			return &openfgav1.ListUsersResponse{
				Users: []*openfgav1.User{
					{User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "user", Id: "alice"}}},
					{User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "user", Id: "bob"}}},
				},
			}, nil
		}

		query := NewSubjectSearchQuery(WithListUsersFunc(mockListUsers))

		req := &authzenv1.SubjectSearchRequest{
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.GetSubjects(), 2)
		require.Equal(t, "alice", resp.GetSubjects()[0].GetId())
		require.Equal(t, "user", resp.GetSubjects()[0].GetType())
		require.Equal(t, "bob", resp.GetSubjects()[1].GetId())
		require.Equal(t, "user", resp.GetSubjects()[1].GetType())
		// No Page response when pagination is not supported
		require.Nil(t, resp.GetPage())
	})

	t.Run("returns_all_results_ignores_page_parameter", func(t *testing.T) {
		// Create 10 users - all should be returned even with page limit
		users := make([]*openfgav1.User, 10)
		for i := 0; i < 10; i++ {
			users[i] = &openfgav1.User{
				User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "user", Id: fmt.Sprintf("user%d", i)}},
			}
		}

		mockListUsers := func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
			return &openfgav1.ListUsersResponse{Users: users}, nil
		}

		query := NewSubjectSearchQuery(WithListUsersFunc(mockListUsers))

		req := &authzenv1.SubjectSearchRequest{
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Page:     &authzenv1.PageRequest{Limit: uint32Ptr(3)}, // Page parameter is ignored
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		// All 10 users should be returned despite limit of 3
		require.Len(t, resp.GetSubjects(), 10)
		// No Page response when pagination is not supported
		require.Nil(t, resp.GetPage())
	})

	// Test that subject type is now required
	t.Run("subject_type_required", func(t *testing.T) {
		mockListUsers := func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
			return &openfgav1.ListUsersResponse{Users: []*openfgav1.User{}}, nil
		}

		query := NewSubjectSearchQuery(WithListUsersFunc(mockListUsers))

		req := &authzenv1.SubjectSearchRequest{
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			// No subject specified - should error
		}

		_, err := query.Execute(context.Background(), req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "subject type is required")
	})

	t.Run("empty_results", func(t *testing.T) {
		mockListUsers := func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
			return &openfgav1.ListUsersResponse{Users: []*openfgav1.User{}}, nil
		}

		query := NewSubjectSearchQuery(WithListUsersFunc(mockListUsers))

		req := &authzenv1.SubjectSearchRequest{
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Empty(t, resp.GetSubjects())
		// No Page response when pagination is not supported
		require.Nil(t, resp.GetPage())
	})

	t.Run("properties_to_context_resource", func(t *testing.T) {
		var capturedReq *openfgav1.ListUsersRequest
		mockListUsers := func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
			capturedReq = req
			return &openfgav1.ListUsersResponse{}, nil
		}

		query := NewSubjectSearchQuery(WithListUsersFunc(mockListUsers))

		req := &authzenv1.SubjectSearchRequest{
			Subject: &authzenv1.SubjectFilter{Type: "user"},
			Resource: &authzenv1.Resource{
				Type:       "document",
				Id:         "doc1",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"level": "secret"}),
			},
			Action:  &authzenv1.Action{Name: "read"},
			StoreId: "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, capturedReq.GetContext())
		require.Equal(t, "secret", capturedReq.GetContext().AsMap()["resource_level"])
	})

	t.Run("properties_to_context_subject", func(t *testing.T) {
		var capturedReq *openfgav1.ListUsersRequest
		mockListUsers := func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
			capturedReq = req
			return &openfgav1.ListUsersResponse{}, nil
		}

		query := NewSubjectSearchQuery(WithListUsersFunc(mockListUsers))

		req := &authzenv1.SubjectSearchRequest{
			Subject: &authzenv1.SubjectFilter{
				Type:       "user",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"department": "engineering"}),
			},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, capturedReq.GetContext())
		require.Equal(t, "engineering", capturedReq.GetContext().AsMap()["subject_department"])
	})

	t.Run("properties_to_context_combined", func(t *testing.T) {
		var capturedReq *openfgav1.ListUsersRequest
		mockListUsers := func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
			capturedReq = req
			return &openfgav1.ListUsersResponse{}, nil
		}

		query := NewSubjectSearchQuery(WithListUsersFunc(mockListUsers))

		req := &authzenv1.SubjectSearchRequest{
			Subject: &authzenv1.SubjectFilter{
				Type:       "user",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"role": "admin"}),
			},
			Resource: &authzenv1.Resource{
				Type:       "document",
				Id:         "doc1",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"classification": "top-secret"}),
			},
			Action:  &authzenv1.Action{Name: "read"},
			StoreId: "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, capturedReq.GetContext())
		ctxMap := capturedReq.GetContext().AsMap()
		require.Equal(t, "admin", ctxMap["subject_role"])
		require.Equal(t, "top-secret", ctxMap["resource_classification"])
	})

	t.Run("subject_type_filtering", func(t *testing.T) {
		var capturedReq *openfgav1.ListUsersRequest
		mockListUsers := func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
			capturedReq = req
			return &openfgav1.ListUsersResponse{
				Users: []*openfgav1.User{
					{User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "employee", Id: "alice"}}},
				},
			}, nil
		}

		query := NewSubjectSearchQuery(WithListUsersFunc(mockListUsers))

		req := &authzenv1.SubjectSearchRequest{
			Subject:  &authzenv1.SubjectFilter{Type: "employee"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.GetSubjects(), 1)
		require.Equal(t, "employee", resp.GetSubjects()[0].GetType())
		require.Equal(t, "alice", resp.GetSubjects()[0].GetId())

		// Verify user filter was applied
		require.Len(t, capturedReq.GetUserFilters(), 1)
		require.Equal(t, "employee", capturedReq.GetUserFilters()[0].GetType())
	})

	t.Run("wildcard_subject_handling", func(t *testing.T) {
		mockListUsers := func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
			return &openfgav1.ListUsersResponse{
				Users: []*openfgav1.User{
					{User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "user", Id: "alice"}}},
					{User: &openfgav1.User_Wildcard{Wildcard: &openfgav1.TypedWildcard{Type: "user"}}},
				},
			}, nil
		}

		query := NewSubjectSearchQuery(WithListUsersFunc(mockListUsers))

		req := &authzenv1.SubjectSearchRequest{
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.GetSubjects(), 2)

		// Verify both subjects are present (order may vary)
		subjectIDs := make(map[string]bool)
		for _, s := range resp.GetSubjects() {
			require.Equal(t, "user", s.GetType())
			subjectIDs[s.GetId()] = true
		}
		require.True(t, subjectIDs["alice"])
		require.True(t, subjectIDs["*"]) // Wildcard user
	})

	t.Run("returns_all_results_with_large_dataset", func(t *testing.T) {
		// Create 100 users - all should be returned
		users := make([]*openfgav1.User, 100)
		for i := 0; i < 100; i++ {
			users[i] = &openfgav1.User{
				User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "user", Id: fmt.Sprintf("user%d", i)}},
			}
		}

		mockListUsers := func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
			return &openfgav1.ListUsersResponse{Users: users}, nil
		}

		query := NewSubjectSearchQuery(WithListUsersFunc(mockListUsers))

		req := &authzenv1.SubjectSearchRequest{
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.GetSubjects(), 100)
		// No Page response when pagination is not supported
		require.Nil(t, resp.GetPage())
	})

	t.Run("request_passes_store_and_model_id", func(t *testing.T) {
		var capturedReq *openfgav1.ListUsersRequest
		mockListUsers := func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
			capturedReq = req
			return &openfgav1.ListUsersResponse{}, nil
		}

		query := NewSubjectSearchQuery(WithListUsersFunc(mockListUsers))

		req := &authzenv1.SubjectSearchRequest{
			Subject:              &authzenv1.SubjectFilter{Type: "user"},
			Resource:             &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:               &authzenv1.Action{Name: "read"},
			StoreId:              "01HVMMBCMGZNT3SED4CT2KA89Q",
			AuthorizationModelId: "01HVMMBD123456789ABCDEFGH",
		}

		_, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, "01HVMMBCMGZNT3SED4CT2KA89Q", capturedReq.GetStoreId())
		require.Equal(t, "01HVMMBD123456789ABCDEFGH", capturedReq.GetAuthorizationModelId())
	})

	t.Run("request_passes_object_and_relation", func(t *testing.T) {
		var capturedReq *openfgav1.ListUsersRequest
		mockListUsers := func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
			capturedReq = req
			return &openfgav1.ListUsersResponse{}, nil
		}

		query := NewSubjectSearchQuery(WithListUsersFunc(mockListUsers))

		req := &authzenv1.SubjectSearchRequest{
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
			Resource: &authzenv1.Resource{Type: "folder", Id: "folder123"},
			Action:   &authzenv1.Action{Name: "write"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, "folder", capturedReq.GetObject().GetType())
		require.Equal(t, "folder123", capturedReq.GetObject().GetId())
		require.Equal(t, "write", capturedReq.GetRelation())
	})

	t.Run("listusers_error_propagation", func(t *testing.T) {
		mockListUsers := func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
			return nil, fmt.Errorf("internal error: database connection failed")
		}

		query := NewSubjectSearchQuery(WithListUsersFunc(mockListUsers))

		req := &authzenv1.SubjectSearchRequest{
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "ListUsers failed")
		require.Contains(t, err.Error(), "database connection failed")
	})

	// These tests verify that subject type is required (changed behavior)
	t.Run("error_when_no_subject_type", func(t *testing.T) {
		mockListUsers := func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
			return &openfgav1.ListUsersResponse{}, nil
		}

		query := NewSubjectSearchQuery(WithListUsersFunc(mockListUsers))

		req := &authzenv1.SubjectSearchRequest{
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			// No subject specified - should error
		}

		_, err := query.Execute(context.Background(), req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "subject type is required")
	})

	t.Run("error_when_subject_has_empty_type", func(t *testing.T) {
		mockListUsers := func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
			return &openfgav1.ListUsersResponse{}, nil
		}

		query := NewSubjectSearchQuery(WithListUsersFunc(mockListUsers))

		req := &authzenv1.SubjectSearchRequest{
			Subject:  &authzenv1.SubjectFilter{Type: ""}, // Empty type - should error
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "subject type is required")
	})
}

func uint32Ptr(v uint32) *uint32 {
	return &v
}
