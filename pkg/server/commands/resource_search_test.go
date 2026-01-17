package commands

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

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
		require.Len(t, resp.GetResults(), 3)
		// Verify resources exist (order-independent)
		resourceIDs := make(map[string]bool)
		for _, r := range resp.GetResults() {
			resourceIDs[r.GetId()] = true
			require.Equal(t, "document", r.GetType())
		}
		require.True(t, resourceIDs["doc1"])
		require.True(t, resourceIDs["doc2"])
		require.True(t, resourceIDs["doc3"])
		// No Page response when pagination is not supported
		require.Nil(t, resp.GetPage())
	})

	t.Run("returns_all_results_ignores_page_parameter", func(t *testing.T) {
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
			Page:     &authzenv1.PageRequest{Limit: &limit}, // Page parameter is ignored
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		// All 10 resources should be returned despite limit of 3
		require.Len(t, resp.GetResults(), 10)
		// No Page response when pagination is not supported
		require.Nil(t, resp.GetPage())
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
		require.Empty(t, resp.GetResults())
		// No Page response when pagination is not supported
		require.Nil(t, resp.GetPage())
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
		require.NotNil(t, capturedReq.GetContext())
		require.Equal(t, "engineering", capturedReq.GetContext().AsMap()["subject_department"])
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
		require.NotNil(t, capturedReq.GetContext())
		require.Equal(t, "secret", capturedReq.GetContext().AsMap()["resource_classification"])
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
		require.NotNil(t, capturedReq.GetContext())
		ctxMap := capturedReq.GetContext().AsMap()
		require.Equal(t, "admin", ctxMap["subject_role"])
		require.Equal(t, "top-secret", ctxMap["resource_level"])
	})

	t.Run("returns_all_results_with_large_dataset", func(t *testing.T) {
		// Create 100 objects - all should be returned
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
		}

		resp, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, resp.GetResults(), 100)
		// No Page response when pagination is not supported
		require.Nil(t, resp.GetPage())
	})

	t.Run("request_passes_store_and_model_id", func(t *testing.T) {
		var capturedReq *openfgav1.StreamedListObjectsRequest
		mockFn := mockStreamedListObjectsFuncWithCapture([]string{}, &capturedReq)
		query := NewResourceSearchQuery(
			WithStreamedListObjectsFunc(mockFn),
			WithResourceSearchAuthorizationModelID("01HVMMBD123456789ABCDEFGH"),
		)

		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, "01HVMMBCMGZNT3SED4CT2KA89Q", capturedReq.GetStoreId())
		require.Equal(t, "01HVMMBD123456789ABCDEFGH", capturedReq.GetAuthorizationModelId())
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
		require.Equal(t, "employee:bob", capturedReq.GetUser())
		require.Equal(t, "write", capturedReq.GetRelation())
		require.Equal(t, "folder", capturedReq.GetType())
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
		require.Len(t, resp.GetResults(), 5)

		// Verify all resources are present
		resourceMap := make(map[string]string)
		for _, r := range resp.GetResults() {
			resourceMap[r.GetType()+":"+r.GetId()] = r.GetType()
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
		require.Len(t, resp.GetResults(), 1)
		require.Equal(t, "document", resp.GetResults()[0].GetType())
		require.Equal(t, "id:with:colons", resp.GetResults()[0].GetId())
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
		require.Len(t, resp.GetResults(), 2)

		// Verify both valid IDs are present
		resourceIDs := make(map[string]bool)
		for _, r := range resp.GetResults() {
			resourceIDs[r.GetId()] = true
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
		require.Len(t, resp.GetResults(), 2)
		// Verify all are folder type
		for _, r := range resp.GetResults() {
			require.Equal(t, "folder", r.GetType())
		}
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
		require.Empty(t, resp.GetResults())
		// No Page response when pagination is not supported
		require.Nil(t, resp.GetPage())
	})

	t.Run("error_from_properties_merge", func(t *testing.T) {
		// Test error propagation from MergePropertiesToContext
		mockFn := mockStreamedListObjectsFunc([]string{}, nil)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		// Create an invalid struct that will cause merge error
		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: &authzenv1.Resource{Type: "document"},
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
			Context:  &structpb.Struct{Fields: map[string]*structpb.Value{}},
		}

		_, err := query.Execute(context.Background(), req)
		// Should succeed - empty context is valid
		require.NoError(t, err)
	})

	t.Run("missing_resource_type", func(t *testing.T) {
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

	t.Run("nil_resource", func(t *testing.T) {
		mockFn := mockStreamedListObjectsFunc([]string{}, nil)
		query := NewResourceSearchQuery(WithStreamedListObjectsFunc(mockFn))

		req := &authzenv1.ResourceSearchRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Action:   &authzenv1.Action{Name: "read"},
			Resource: nil, // Nil resource
			StoreId:  "01HVMMBCMGZNT3SED4CT2KA89Q",
		}

		_, err := query.Execute(context.Background(), req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "resource type is required")
	})

	t.Run("streamed_list_objects_error", func(t *testing.T) {
		// Test error propagation from StreamedListObjects
		mockFn := mockStreamedListObjectsFunc(nil, fmt.Errorf("database connection failed"))
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

	t.Run("malformed_object_ids_all_skipped", func(t *testing.T) {
		mockFn := mockStreamedListObjectsFunc([]string{
			"no_colon_here",
			"another_invalid",
			"still_no_colon",
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
		require.Empty(t, resp.GetResults())
	})
}
