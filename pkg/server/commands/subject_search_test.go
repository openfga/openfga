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
		require.Len(t, resp.Subjects, 2)
		require.Equal(t, "alice", resp.Subjects[0].Id)
		require.Equal(t, "user", resp.Subjects[0].Type)
		require.Equal(t, "bob", resp.Subjects[1].Id)
		require.Equal(t, "user", resp.Subjects[1].Type)
	})

	t.Run("pagination_initial_request", func(t *testing.T) {
		// Create 10 users
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
			Page:     &authzenv1.PageRequest{Limit: uint32Ptr(3)},
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.Subjects, 3)
		require.NotEmpty(t, resp.Page.NextToken)
		require.Equal(t, uint32(3), resp.Page.Count)
		require.Equal(t, uint32(10), *resp.Page.Total)
		// Verify first page subjects
		require.Equal(t, "user0", resp.Subjects[0].Id)
		require.Equal(t, "user1", resp.Subjects[1].Id)
		require.Equal(t, "user2", resp.Subjects[2].Id)
	})

	t.Run("pagination_continuation", func(t *testing.T) {
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

		// First request
		req := &authzenv1.SubjectSearchRequest{
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Page:     &authzenv1.PageRequest{Limit: uint32Ptr(3)},
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.NotEmpty(t, resp.Page.NextToken)

		// Continue with token
		req.Page.Token = &resp.Page.NextToken
		resp2, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp2.Subjects, 3)
		require.Equal(t, "user3", resp2.Subjects[0].Id)
		require.Equal(t, "user4", resp2.Subjects[1].Id)
		require.Equal(t, "user5", resp2.Subjects[2].Id)
	})

	t.Run("pagination_last_page", func(t *testing.T) {
		users := make([]*openfgav1.User, 5)
		for i := 0; i < 5; i++ {
			users[i] = &openfgav1.User{
				User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "user", Id: fmt.Sprintf("user%d", i)}},
			}
		}

		mockListUsers := func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
			return &openfgav1.ListUsersResponse{Users: users}, nil
		}

		query := NewSubjectSearchQuery(WithListUsersFunc(mockListUsers))

		// First request - get 3 of 5
		req := &authzenv1.SubjectSearchRequest{
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Page:     &authzenv1.PageRequest{Limit: uint32Ptr(3)},
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.Subjects, 3)
		require.NotEmpty(t, resp.Page.NextToken)

		// Second request - get remaining 2
		req.Page.Token = &resp.Page.NextToken
		resp2, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp2.Subjects, 2)
		require.Empty(t, resp2.Page.NextToken) // No more pages
		require.Equal(t, "user3", resp2.Subjects[0].Id)
		require.Equal(t, "user4", resp2.Subjects[1].Id)
	})

	t.Run("pagination_invalid_token", func(t *testing.T) {
		mockListUsers := func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
			return &openfgav1.ListUsersResponse{}, nil
		}

		query := NewSubjectSearchQuery(WithListUsersFunc(mockListUsers))

		invalidToken := "not-valid-base64-!@#"
		req := &authzenv1.SubjectSearchRequest{
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Page:     &authzenv1.PageRequest{Token: &invalidToken},
		}

		_, err := query.Execute(context.Background(), req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid pagination token")
	})

	t.Run("pagination_invalid_json_token", func(t *testing.T) {
		mockListUsers := func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
			return &openfgav1.ListUsersResponse{}, nil
		}

		query := NewSubjectSearchQuery(WithListUsersFunc(mockListUsers))

		// Valid base64 but invalid JSON
		invalidToken := "bm90LXZhbGlkLWpzb24=" // "not-valid-json" in base64
		req := &authzenv1.SubjectSearchRequest{
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Page:     &authzenv1.PageRequest{Token: &invalidToken},
		}

		_, err := query.Execute(context.Background(), req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid pagination token")
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
		require.Empty(t, resp.Subjects)
		require.Empty(t, resp.Page.NextToken)
		require.Equal(t, uint32(0), resp.Page.Count)
		require.Equal(t, uint32(0), *resp.Page.Total)
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
		require.NotNil(t, capturedReq.Context)
		require.Equal(t, "secret", capturedReq.Context.AsMap()["resource.level"])
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
		require.NotNil(t, capturedReq.Context)
		require.Equal(t, "engineering", capturedReq.Context.AsMap()["subject.department"])
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
		require.NotNil(t, capturedReq.Context)
		ctxMap := capturedReq.Context.AsMap()
		require.Equal(t, "admin", ctxMap["subject.role"])
		require.Equal(t, "top-secret", ctxMap["resource.classification"])
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
		require.Len(t, resp.Subjects, 1)
		require.Equal(t, "employee", resp.Subjects[0].Type)
		require.Equal(t, "alice", resp.Subjects[0].Id)

		// Verify user filter was applied
		require.Len(t, capturedReq.UserFilters, 1)
		require.Equal(t, "employee", capturedReq.UserFilters[0].Type)
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
		require.Len(t, resp.Subjects, 2)

		// Verify both subjects are present (order may vary due to sorting)
		subjectIDs := make(map[string]bool)
		for _, s := range resp.Subjects {
			require.Equal(t, "user", s.Type)
			subjectIDs[s.Id] = true
		}
		require.True(t, subjectIDs["alice"])
		require.True(t, subjectIDs["*"]) // Wildcard user
	})

	t.Run("limit_default", func(t *testing.T) {
		// Create 100 users to test default limit
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
			// No page limit specified - should use DefaultSearchLimit
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, DefaultSearchLimit, len(resp.Subjects))
		require.NotEmpty(t, resp.Page.NextToken)
	})

	t.Run("limit_max_enforcement", func(t *testing.T) {
		// Create more users than max limit
		users := make([]*openfgav1.User, MaxSearchLimit+100)
		for i := 0; i < MaxSearchLimit+100; i++ {
			users[i] = &openfgav1.User{
				User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "user", Id: fmt.Sprintf("user%d", i)}},
			}
		}

		mockListUsers := func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
			return &openfgav1.ListUsersResponse{Users: users}, nil
		}

		query := NewSubjectSearchQuery(WithListUsersFunc(mockListUsers))

		// Request more than max limit
		req := &authzenv1.SubjectSearchRequest{
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Page:     &authzenv1.PageRequest{Limit: uint32Ptr(MaxSearchLimit + 500)},
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, MaxSearchLimit, len(resp.Subjects))
		require.NotEmpty(t, resp.Page.NextToken)
	})

	t.Run("limit_zero_uses_default", func(t *testing.T) {
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
			Page:     &authzenv1.PageRequest{Limit: uint32Ptr(0)},
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, DefaultSearchLimit, len(resp.Subjects))
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
		require.Equal(t, "01HVMMBCMGZNT3SED4CT2KA89Q", capturedReq.StoreId)
		require.Equal(t, "01HVMMBD123456789ABCDEFGH", capturedReq.AuthorizationModelId)
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
		require.Equal(t, "folder", capturedReq.Object.Type)
		require.Equal(t, "folder123", capturedReq.Object.Id)
		require.Equal(t, "write", capturedReq.Relation)
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

	t.Run("pagination_offset_beyond_results", func(t *testing.T) {
		users := make([]*openfgav1.User, 3)
		for i := 0; i < 3; i++ {
			users[i] = &openfgav1.User{
				User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "user", Id: fmt.Sprintf("user%d", i)}},
			}
		}

		mockListUsers := func(ctx context.Context, req *openfgav1.ListUsersRequest) (*openfgav1.ListUsersResponse, error) {
			return &openfgav1.ListUsersResponse{Users: users}, nil
		}

		query := NewSubjectSearchQuery(WithListUsersFunc(mockListUsers))

		// Create a token with offset beyond the result set
		token := encodePaginationToken(&PaginationToken{Offset: 100})
		req := &authzenv1.SubjectSearchRequest{
			Subject:  &authzenv1.SubjectFilter{Type: "user"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Page:     &authzenv1.PageRequest{Token: &token},
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Empty(t, resp.Subjects)
		require.Empty(t, resp.Page.NextToken)
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
