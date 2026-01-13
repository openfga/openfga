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

// mockStreamedListObjectsFunc creates a StreamedListObjectsFunc that sends the given objects.
func mockStreamedListObjectsFunc(objects []string, errToReturn error) StreamedListObjectsFunc {
	return func(req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) error {
		if errToReturn != nil {
			return errToReturn
		}
		for _, obj := range objects {
			if err := srv.Send(&openfgav1.StreamedListObjectsResponse{Object: obj}); err != nil {
				return err
			}
		}
		return nil
	}
}

// mockStreamedListObjectsFuncWithCapture creates a StreamedListObjectsFunc that captures the request.
func mockStreamedListObjectsFuncWithCapture(objects []string, capturedReq **openfgav1.StreamedListObjectsRequest) StreamedListObjectsFunc {
	return func(req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) error {
		*capturedReq = req
		for _, obj := range objects {
			if err := srv.Send(&openfgav1.StreamedListObjectsResponse{Object: obj}); err != nil {
				return err
			}
		}
		return nil
	}
}

func TestResourceSearchQuery(t *testing.T) {
	t.Run("basic_search", func(t *testing.T) {
		mockFn := mockStreamedListObjectsFunc([]string{"document:doc1", "document:doc2", "document:doc3"}, nil)

		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.Resources, 3)
		// Verify resources exist (order-independent)
		resourceIDs := make(map[string]bool)
		for _, r := range resp.Resources {
			resourceIDs[r.Id] = true
			require.Equal(t, "document", r.Type)
		}
		require.True(t, resourceIDs["doc1"])
		require.True(t, resourceIDs["doc2"])
		require.True(t, resourceIDs["doc3"])
	})

	t.Run("pagination_initial_request", func(t *testing.T) {
		objects := make([]string, 10)
		for i := 0; i < 10; i++ {
			objects[i] = fmt.Sprintf("document:doc%d", i)
		}

		mockFn := mockStreamedListObjectsFunc(objects, nil)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		limit := uint32(3)
		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Page:     &authzenv1.PageRequest{Limit: &limit},
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.Resources, 3)
		require.NotEmpty(t, resp.Page.NextToken)
		require.Equal(t, uint32(3), resp.Page.Count)
		// Note: Total is not available with early termination streaming
	})

	t.Run("pagination_continuation", func(t *testing.T) {
		objects := make([]string, 10)
		for i := 0; i < 10; i++ {
			objects[i] = fmt.Sprintf("document:doc%d", i)
		}

		mockFn := mockStreamedListObjectsFunc(objects, nil)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		limit := uint32(3)
		// First request
		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Page:     &authzenv1.PageRequest{Limit: &limit},
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.NotEmpty(t, resp.Page.NextToken)

		// Continue with token
		req.Page.Token = &resp.Page.NextToken
		resp2, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp2.Resources, 3)
		// Verify resources exist (order-independent) - should be different from first page
		resourceIDs := make(map[string]bool)
		for _, r := range resp2.Resources {
			resourceIDs[r.Id] = true
		}
		// Just verify we got 3 different resources
		require.Len(t, resourceIDs, 3)
	})

	t.Run("pagination_last_page", func(t *testing.T) {
		objects := make([]string, 5)
		for i := 0; i < 5; i++ {
			objects[i] = fmt.Sprintf("document:doc%d", i)
		}

		mockFn := mockStreamedListObjectsFunc(objects, nil)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		limit := uint32(3)
		// First request - get 3 of 5
		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Page:     &authzenv1.PageRequest{Limit: &limit},
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.Resources, 3)
		require.NotEmpty(t, resp.Page.NextToken)

		// Second request - get remaining 2
		req.Page.Token = &resp.Page.NextToken
		resp2, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp2.Resources, 2)
		require.Empty(t, resp2.Page.NextToken) // No more pages
	})

	t.Run("pagination_invalid_token", func(t *testing.T) {
		mockFn := mockStreamedListObjectsFunc([]string{}, nil)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		invalidToken := "not-valid-base64-!@#"
		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Page:     &authzenv1.PageRequest{Token: &invalidToken},
		}

		_, err := query.Execute(context.Background(), req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid pagination token")
	})

	t.Run("pagination_invalid_json_token", func(t *testing.T) {
		mockFn := mockStreamedListObjectsFunc([]string{}, nil)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		// Valid base64 but invalid JSON
		invalidToken := "bm90LXZhbGlkLWpzb24=" // "not-valid-json" in base64
		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Page:     &authzenv1.PageRequest{Token: &invalidToken},
		}

		_, err := query.Execute(context.Background(), req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid pagination token")
	})

	t.Run("resource_type_required", func(t *testing.T) {
		mockFn := mockStreamedListObjectsFunc([]string{}, nil)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		req := &authzenv1.ResourceSearchRequest{
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "read"},
			// No resource type specified
			StoreId: "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "resource type is required")
	})

	t.Run("resource_type_empty", func(t *testing.T) {
		mockFn := mockStreamedListObjectsFunc([]string{}, nil)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: ""}, // Empty type
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "resource type is required")
	})

	t.Run("empty_results", func(t *testing.T) {
		mockFn := mockStreamedListObjectsFunc([]string{}, nil)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Empty(t, resp.Resources)
		require.Empty(t, resp.Page.NextToken)
		require.Equal(t, uint32(0), resp.Page.Count)
		// Note: Total is not available with early termination streaming
	})

	t.Run("properties_to_context_subject", func(t *testing.T) {
		var capturedReq *openfgav1.StreamedListObjectsRequest
		mockFn := mockStreamedListObjectsFuncWithCapture([]string{}, &capturedReq)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		req := &authzenv1.ResourceSearchRequest{
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"department": "engineering"}),
			},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, capturedReq.Context)
		require.Equal(t, "engineering", capturedReq.Context.AsMap()["subject.department"])
	})

	t.Run("properties_to_context_resource", func(t *testing.T) {
		var capturedReq *openfgav1.StreamedListObjectsRequest
		mockFn := mockStreamedListObjectsFuncWithCapture([]string{}, &capturedReq)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		req := &authzenv1.ResourceSearchRequest{
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:  &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{
				Type:       "document",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"classification": "secret"}),
			},
			StoreId: "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, capturedReq.Context)
		require.Equal(t, "secret", capturedReq.Context.AsMap()["resource.classification"])
	})

	t.Run("properties_to_context_combined", func(t *testing.T) {
		var capturedReq *openfgav1.StreamedListObjectsRequest
		mockFn := mockStreamedListObjectsFuncWithCapture([]string{}, &capturedReq)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		req := &authzenv1.ResourceSearchRequest{
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"role": "admin"}),
			},
			Action: &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{
				Type:       "document",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"level": "top-secret"}),
			},
			StoreId: "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, capturedReq.Context)
		ctxMap := capturedReq.Context.AsMap()
		require.Equal(t, "admin", ctxMap["subject.role"])
		require.Equal(t, "top-secret", ctxMap["resource.level"])
	})

	t.Run("limit_default", func(t *testing.T) {
		// Create 100 objects to test default limit
		objects := make([]string, 100)
		for i := 0; i < 100; i++ {
			objects[i] = fmt.Sprintf("document:doc%d", i)
		}

		mockFn := mockStreamedListObjectsFunc(objects, nil)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			// No page limit specified - should use DefaultSearchLimit
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, DefaultSearchLimit, len(resp.Resources))
		require.NotEmpty(t, resp.Page.NextToken)
	})

	t.Run("limit_max_enforcement", func(t *testing.T) {
		// Create more objects than max limit
		objects := make([]string, MaxSearchLimit+100)
		for i := 0; i < MaxSearchLimit+100; i++ {
			objects[i] = fmt.Sprintf("document:doc%d", i)
		}

		mockFn := mockStreamedListObjectsFunc(objects, nil)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		// Request more than max limit
		limit := uint32(MaxSearchLimit + 500)
		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Page:     &authzenv1.PageRequest{Limit: &limit},
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, MaxSearchLimit, len(resp.Resources))
		require.NotEmpty(t, resp.Page.NextToken)
	})

	t.Run("limit_zero_uses_default", func(t *testing.T) {
		objects := make([]string, 100)
		for i := 0; i < 100; i++ {
			objects[i] = fmt.Sprintf("document:doc%d", i)
		}

		mockFn := mockStreamedListObjectsFunc(objects, nil)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		limit := uint32(0)
		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Page:     &authzenv1.PageRequest{Limit: &limit},
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, DefaultSearchLimit, len(resp.Resources))
	})

	t.Run("request_passes_store_and_model_id", func(t *testing.T) {
		var capturedReq *openfgav1.StreamedListObjectsRequest
		mockFn := mockStreamedListObjectsFuncWithCapture([]string{}, &capturedReq)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		req := &authzenv1.ResourceSearchRequest{
			Subject:              &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:               &authzenv1.Action{Name: "read"},
			Resource:             &authzenv1.Resource{Type: "document"},
			StoreId:              "01HVMMBCMGZNT3SED4CT2KA89Q",
			AuthorizationModelId: "01HVMMBD123456789ABCDEFGH",
		}

		_, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, "01HVMMBCMGZNT3SED4CT2KA89Q", capturedReq.StoreId)
		require.Equal(t, "01HVMMBD123456789ABCDEFGH", capturedReq.AuthorizationModelId)
	})

	t.Run("request_passes_user_and_relation", func(t *testing.T) {
		var capturedReq *openfgav1.StreamedListObjectsRequest
		mockFn := mockStreamedListObjectsFuncWithCapture([]string{}, &capturedReq)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "employee", Id: "bob"},
			Action:   &authzenv1.Action{Name: "write"},
			Resource: &authzenv1.Resource{Type: "folder"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, "employee:bob", capturedReq.User)
		require.Equal(t, "write", capturedReq.Relation)
		require.Equal(t, "folder", capturedReq.Type)
	})

	t.Run("streamedlistobjects_error_propagation", func(t *testing.T) {
		mockFn := mockStreamedListObjectsFunc(nil, fmt.Errorf("internal error: database connection failed"))
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "StreamedListObjects failed")
		require.Contains(t, err.Error(), "database connection failed")
	})

	t.Run("pagination_offset_beyond_results", func(t *testing.T) {
		objects := make([]string, 3)
		for i := 0; i < 3; i++ {
			objects[i] = fmt.Sprintf("document:doc%d", i)
		}

		mockFn := mockStreamedListObjectsFunc(objects, nil)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		// Create a token with offset beyond the result set
		token := encodePaginationToken(&PaginationToken{Offset: 100})
		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Page:     &authzenv1.PageRequest{Token: &token},
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Empty(t, resp.Resources)
		require.Empty(t, resp.Page.NextToken)
	})

	t.Run("object_id_parsing_valid", func(t *testing.T) {
		mockFn := mockStreamedListObjectsFunc([]string{
			"document:simple-id",
			"folder:id-with-dash",
			"resource:id_with_underscore",
			"item:123456",
			"file:path/to/file",
		}, nil)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.Resources, 5)

		// Verify all resources are present (order is sorted by type:id)
		resourceMap := make(map[string]string)
		for _, r := range resp.Resources {
			resourceMap[r.Type+":"+r.Id] = r.Type
		}
		require.Contains(t, resourceMap, "document:simple-id")
		require.Contains(t, resourceMap, "folder:id-with-dash")
		require.Contains(t, resourceMap, "resource:id_with_underscore")
		require.Contains(t, resourceMap, "item:123456")
		require.Contains(t, resourceMap, "file:path/to/file")
	})

	t.Run("object_id_with_colons_in_id", func(t *testing.T) {
		mockFn := mockStreamedListObjectsFunc([]string{"document:id:with:colons"}, nil)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.Resources, 1)
		require.Equal(t, "document", resp.Resources[0].Type)
		require.Equal(t, "id:with:colons", resp.Resources[0].Id)
	})

	t.Run("malformed_object_id_skipped", func(t *testing.T) {
		mockFn := mockStreamedListObjectsFunc([]string{
			"document:valid-id",
			"invalid_no_colon", // This should be skipped
			"document:another-valid",
		}, nil)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.Resources, 2)

		// Verify both valid IDs are present (order is sorted)
		resourceIDs := make(map[string]bool)
		for _, r := range resp.Resources {
			resourceIDs[r.Id] = true
		}
		require.True(t, resourceIDs["valid-id"])
		require.True(t, resourceIDs["another-valid"])
	})

	t.Run("different_resource_types", func(t *testing.T) {
		mockFn := mockStreamedListObjectsFunc([]string{"folder:folder1", "folder:folder2"}, nil)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "viewer"},
			Resource: &authzenv1.Resource{Type: "folder"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.Resources, 2)
		require.Equal(t, "folder", resp.Resources[0].Type)
		require.Equal(t, "folder1", resp.Resources[0].Id)
		require.Equal(t, "folder", resp.Resources[1].Type)
		require.Equal(t, "folder2", resp.Resources[1].Id)
	})

	t.Run("nil_objects_response", func(t *testing.T) {
		// Empty slice simulates nil objects
		mockFn := mockStreamedListObjectsFunc([]string{}, nil)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Empty(t, resp.Resources)
		require.Empty(t, resp.Page.NextToken)
		require.Equal(t, uint32(0), resp.Page.Count)
	})
}
