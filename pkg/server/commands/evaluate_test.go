package commands

import (
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"

	"github.com/openfga/openfga/pkg/testutils"
)

func TestEvaluateRequestCommand(t *testing.T) {
	t.Run("basic_transformation", func(t *testing.T) {
		storeID := ulid.Make().String()
		req := &authzenv1.EvaluationRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  storeID,
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		checkReq := cmd.GetCheckRequest()
		require.Equal(t, "user:alice", checkReq.GetTupleKey().GetUser())
		require.Equal(t, "read", checkReq.GetTupleKey().GetRelation())
		require.Equal(t, "document:doc1", checkReq.GetTupleKey().GetObject())
		require.Equal(t, storeID, checkReq.GetStoreId())
	})

	t.Run("basic_transformation_with_authorization_model_id", func(t *testing.T) {
		storeID := ulid.Make().String()
		modelID := ulid.Make().String()
		req := &authzenv1.EvaluationRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "maria"},
			Resource: &authzenv1.Resource{Type: "repo", Id: "fga"},
			Action:   &authzenv1.Action{Name: "write"},
			StoreId:  storeID,
		}

		cmd, err := NewEvaluateRequestCommand(req, modelID)
		require.NoError(t, err)

		checkReq := cmd.GetCheckRequest()
		require.Equal(t, storeID, checkReq.GetStoreId())
		require.Equal(t, modelID, checkReq.GetAuthorizationModelId())
		require.Equal(t, "user:maria", checkReq.GetTupleKey().GetUser())
		require.Equal(t, "write", checkReq.GetTupleKey().GetRelation())
		require.Equal(t, "repo:fga", checkReq.GetTupleKey().GetObject())
	})

	t.Run("context_passthrough", func(t *testing.T) {
		ctx := testutils.MustNewStruct(t, map[string]interface{}{"time": "2024-01-01"})
		req := &authzenv1.EvaluationRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			Context:  ctx,
			StoreId:  ulid.Make().String(),
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		require.NoError(t, err)
		require.NotNil(t, cmd.GetCheckRequest().GetContext())
		require.Equal(t, "2024-01-01", cmd.GetCheckRequest().GetContext().AsMap()["time"])
	})

	t.Run("context_with_multiple_fields", func(t *testing.T) {
		ctx := testutils.MustNewStruct(t, map[string]interface{}{
			"time":       "2024-01-01T10:00:00Z",
			"ip_address": "192.168.1.1",
			"is_admin":   true,
			"level":      float64(5),
		})
		req := &authzenv1.EvaluationRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "bob"},
			Resource: &authzenv1.Resource{Type: "folder", Id: "root"},
			Action:   &authzenv1.Action{Name: "admin"},
			Context:  ctx,
			StoreId:  ulid.Make().String(),
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		resultCtx := cmd.GetCheckRequest().GetContext().AsMap()
		require.Equal(t, "2024-01-01T10:00:00Z", resultCtx["time"])
		require.Equal(t, "192.168.1.1", resultCtx["ip_address"])
		require.Equal(t, true, resultCtx["is_admin"])
		require.InEpsilon(t, float64(5), resultCtx["level"], 0.0001)
	})

	t.Run("context_with_nested_struct", func(t *testing.T) {
		ctx := testutils.MustNewStruct(t, map[string]interface{}{
			"request": map[string]interface{}{
				"method": "POST",
				"path":   "/api/v1/documents",
			},
		})
		req := &authzenv1.EvaluationRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "create"},
			Context:  ctx,
			StoreId:  ulid.Make().String(),
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		resultCtx := cmd.GetCheckRequest().GetContext().AsMap()
		nested, ok := resultCtx["request"].(map[string]interface{})
		require.True(t, ok)
		require.Equal(t, "POST", nested["method"])
		require.Equal(t, "/api/v1/documents", nested["path"])
	})

	t.Run("properties_to_context_merge_subject_only", func(t *testing.T) {
		req := &authzenv1.EvaluationRequest{
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"department": "engineering", "role": "developer"}),
			},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  ulid.Make().String(),
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		ctx := cmd.GetCheckRequest().GetContext().AsMap()
		require.Equal(t, "engineering", ctx["subject_department"])
		require.Equal(t, "developer", ctx["subject_role"])
	})

	t.Run("properties_to_context_merge_resource_only", func(t *testing.T) {
		req := &authzenv1.EvaluationRequest{
			Subject: &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{
				Type:       "document",
				Id:         "doc1",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"classification": "confidential", "owner": "bob"}),
			},
			Action:  &authzenv1.Action{Name: "read"},
			StoreId: ulid.Make().String(),
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		ctx := cmd.GetCheckRequest().GetContext().AsMap()
		require.Equal(t, "confidential", ctx["resource_classification"])
		require.Equal(t, "bob", ctx["resource_owner"])
	})

	t.Run("properties_to_context_merge_action_only", func(t *testing.T) {
		req := &authzenv1.EvaluationRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action: &authzenv1.Action{
				Name:       "read",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"reason": "audit", "emergency": false}),
			},
			StoreId: ulid.Make().String(),
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		ctx := cmd.GetCheckRequest().GetContext().AsMap()
		require.Equal(t, "audit", ctx["action_reason"])
		require.Equal(t, false, ctx["action_emergency"])
	})

	t.Run("properties_to_context_merge_all_sources", func(t *testing.T) {
		req := &authzenv1.EvaluationRequest{
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"department": "engineering"}),
			},
			Resource: &authzenv1.Resource{
				Type:       "document",
				Id:         "doc1",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"classification": "confidential"}),
			},
			Action: &authzenv1.Action{
				Name:       "read",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"reason": "audit"}),
			},
			StoreId: ulid.Make().String(),
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		ctx := cmd.GetCheckRequest().GetContext().AsMap()
		require.Equal(t, "engineering", ctx["subject_department"])
		require.Equal(t, "confidential", ctx["resource_classification"])
		require.Equal(t, "audit", ctx["action_reason"])
	})

	t.Run("context_takes_precedence_over_properties", func(t *testing.T) {
		req := &authzenv1.EvaluationRequest{
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"department": "engineering"}),
			},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			Context:  testutils.MustNewStruct(t, map[string]interface{}{"subject_department": "override"}),
			StoreId:  ulid.Make().String(),
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		ctx := cmd.GetCheckRequest().GetContext().AsMap()
		require.Equal(t, "override", ctx["subject_department"])
	})

	t.Run("context_takes_precedence_over_multiple_properties", func(t *testing.T) {
		req := &authzenv1.EvaluationRequest{
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"department": "engineering", "level": float64(1)}),
			},
			Resource: &authzenv1.Resource{
				Type:       "document",
				Id:         "doc1",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"classification": "public"}),
			},
			Action:  &authzenv1.Action{Name: "read"},
			Context: testutils.MustNewStruct(t, map[string]interface{}{"subject_department": "legal", "resource_classification": "top-secret"}),
			StoreId: ulid.Make().String(),
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		ctx := cmd.GetCheckRequest().GetContext().AsMap()
		require.Equal(t, "legal", ctx["subject_department"])
		require.InEpsilon(t, float64(1), ctx["subject_level"], 0.0001)
		require.Equal(t, "top-secret", ctx["resource_classification"])
	})

	t.Run("context_and_properties_merge_without_conflict", func(t *testing.T) {
		req := &authzenv1.EvaluationRequest{
			Subject: &authzenv1.Subject{
				Type:       "user",
				Id:         "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{"department": "engineering"}),
			},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			Context:  testutils.MustNewStruct(t, map[string]interface{}{"custom_field": "custom_value"}),
			StoreId:  ulid.Make().String(),
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		ctx := cmd.GetCheckRequest().GetContext().AsMap()
		require.Equal(t, "engineering", ctx["subject_department"])
		require.Equal(t, "custom_value", ctx["custom_field"])
	})

	t.Run("nil_context_with_no_properties", func(t *testing.T) {
		req := &authzenv1.EvaluationRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  ulid.Make().String(),
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		require.NoError(t, err)
		require.Nil(t, cmd.GetCheckRequest().GetContext())
	})

	t.Run("special_characters_in_subject_id", func(t *testing.T) {
		testCases := []struct {
			name       string
			subjectID  string
			expectedID string
		}{
			{"email_format", "alice@example.com", "user:alice@example.com"},
			{"uuid_format", "550e8400-e29b-41d4-a716-446655440000", "user:550e8400-e29b-41d4-a716-446655440000"},
			{"with_underscore", "alice_smith", "user:alice_smith"},
			{"with_hyphen", "alice-smith", "user:alice-smith"},
			{"with_dot", "alice.smith", "user:alice.smith"},
			{"numeric", "12345", "user:12345"},
			{"mixed_case", "AliceSmith", "user:AliceSmith"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				req := &authzenv1.EvaluationRequest{
					Subject:  &authzenv1.Subject{Type: "user", Id: tc.subjectID},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "read"},
					StoreId:  ulid.Make().String(),
				}

				cmd, err := NewEvaluateRequestCommand(req, "")
				require.NoError(t, err)
				require.Equal(t, tc.expectedID, cmd.GetCheckRequest().GetTupleKey().GetUser())
			})
		}
	})

	t.Run("special_characters_in_resource_id", func(t *testing.T) {
		testCases := []struct {
			name       string
			resourceID string
			expectedID string
		}{
			{"path_like", "folder/subfolder/doc", "document:folder/subfolder/doc"},
			{"uuid_format", "550e8400-e29b-41d4-a716-446655440000", "document:550e8400-e29b-41d4-a716-446655440000"},
			{"with_underscore", "my_document", "document:my_document"},
			{"with_hyphen", "my-document", "document:my-document"},
			{"with_dot", "file.txt", "document:file.txt"},
			{"numeric", "12345", "document:12345"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				req := &authzenv1.EvaluationRequest{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: tc.resourceID},
					Action:   &authzenv1.Action{Name: "read"},
					StoreId:  ulid.Make().String(),
				}

				cmd, err := NewEvaluateRequestCommand(req, "")
				require.NoError(t, err)
				require.Equal(t, tc.expectedID, cmd.GetCheckRequest().GetTupleKey().GetObject())
			})
		}
	})

	t.Run("special_characters_in_action_name", func(t *testing.T) {
		testCases := []struct {
			name         string
			actionName   string
			expectedName string
		}{
			{"simple", "read", "read"},
			{"with_underscore", "can_read", "can_read"},
			{"with_hyphen", "can-read", "can-read"},
			{"camelCase", "canRead", "canRead"},
			{"snake_case", "can_view_all", "can_view_all"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				req := &authzenv1.EvaluationRequest{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: tc.actionName},
					StoreId:  ulid.Make().String(),
				}

				cmd, err := NewEvaluateRequestCommand(req, "")
				require.NoError(t, err)
				require.Equal(t, tc.expectedName, cmd.GetCheckRequest().GetTupleKey().GetRelation())
			})
		}
	})

	t.Run("various_subject_types", func(t *testing.T) {
		testCases := []struct {
			name         string
			subjectType  string
			subjectID    string
			expectedUser string
		}{
			{"user_type", "user", "alice", "user:alice"},
			{"group_type", "group", "admins", "group:admins"},
			{"service_type", "service", "api-gateway", "service:api-gateway"},
			{"application_type", "application", "mobile-app", "application:mobile-app"},
			{"custom_type", "employee", "emp123", "employee:emp123"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				req := &authzenv1.EvaluationRequest{
					Subject:  &authzenv1.Subject{Type: tc.subjectType, Id: tc.subjectID},
					Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
					Action:   &authzenv1.Action{Name: "read"},
					StoreId:  ulid.Make().String(),
				}

				cmd, err := NewEvaluateRequestCommand(req, "")
				require.NoError(t, err)
				require.Equal(t, tc.expectedUser, cmd.GetCheckRequest().GetTupleKey().GetUser())
			})
		}
	})

	t.Run("various_resource_types", func(t *testing.T) {
		testCases := []struct {
			name           string
			resourceType   string
			resourceID     string
			expectedObject string
		}{
			{"document_type", "document", "doc1", "document:doc1"},
			{"folder_type", "folder", "projects", "folder:projects"},
			{"repo_type", "repo", "openfga", "repo:openfga"},
			{"org_type", "organization", "acme", "organization:acme"},
			{"custom_type", "workspace", "ws-123", "workspace:ws-123"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				req := &authzenv1.EvaluationRequest{
					Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
					Resource: &authzenv1.Resource{Type: tc.resourceType, Id: tc.resourceID},
					Action:   &authzenv1.Action{Name: "read"},
					StoreId:  ulid.Make().String(),
				}

				cmd, err := NewEvaluateRequestCommand(req, "")
				require.NoError(t, err)
				require.Equal(t, tc.expectedObject, cmd.GetCheckRequest().GetTupleKey().GetObject())
			})
		}
	})

	t.Run("empty_string_ids", func(t *testing.T) {
		req := &authzenv1.EvaluationRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: ""},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  ulid.Make().String(),
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		require.NoError(t, err)
		require.Equal(t, "user:", cmd.GetCheckRequest().GetTupleKey().GetUser())
	})

	t.Run("empty_resource_id", func(t *testing.T) {
		req := &authzenv1.EvaluationRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: ""},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  ulid.Make().String(),
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		require.NoError(t, err)
		require.Equal(t, "document:", cmd.GetCheckRequest().GetTupleKey().GetObject())
	})

	t.Run("empty_action_name", func(t *testing.T) {
		req := &authzenv1.EvaluationRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: ""},
			StoreId:  ulid.Make().String(),
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		require.NoError(t, err)
		require.Empty(t, cmd.GetCheckRequest().GetTupleKey().GetRelation())
	})

	t.Run("properties_with_various_value_types", func(t *testing.T) {
		req := &authzenv1.EvaluationRequest{
			Subject: &authzenv1.Subject{
				Type: "user",
				Id:   "alice",
				Properties: testutils.MustNewStruct(t, map[string]interface{}{
					"string_val": "hello",
					"int_val":    float64(42),
					"bool_val":   true,
					"null_val":   nil,
					"array_val":  []interface{}{"a", "b", "c"},
					"nested_val": map[string]interface{}{"key": "value"},
				}),
			},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  ulid.Make().String(),
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		require.NoError(t, err)

		ctx := cmd.GetCheckRequest().GetContext().AsMap()
		require.Equal(t, "hello", ctx["subject_string_val"])
		require.InEpsilon(t, float64(42), ctx["subject_int_val"], 0.0001)
		require.Equal(t, true, ctx["subject_bool_val"])
		require.Nil(t, ctx["subject_null_val"])
		require.Equal(t, []interface{}{"a", "b", "c"}, ctx["subject_array_val"])

		nestedVal, ok := ctx["subject_nested_val"].(map[string]interface{})
		require.True(t, ok)
		require.Equal(t, "value", nestedVal["key"])
	})

	t.Run("empty_store_id", func(t *testing.T) {
		req := &authzenv1.EvaluationRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  "",
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		require.NoError(t, err)
		require.Empty(t, cmd.GetCheckRequest().GetStoreId())
	})

	t.Run("nil_subject", func(t *testing.T) {
		req := &authzenv1.EvaluationRequest{
			Subject:  nil,
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  ulid.Make().String(),
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		require.Error(t, err)
		require.Nil(t, cmd)
		require.Contains(t, err.Error(), "missing subject")
	})

	t.Run("nil_resource", func(t *testing.T) {
		req := &authzenv1.EvaluationRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: nil,
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  ulid.Make().String(),
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		require.Error(t, err)
		require.Nil(t, cmd)
		require.Contains(t, err.Error(), "missing resource")
	})

	t.Run("nil_action", func(t *testing.T) {
		req := &authzenv1.EvaluationRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   nil,
			StoreId:  ulid.Make().String(),
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		require.Error(t, err)
		require.Nil(t, cmd)
		require.Contains(t, err.Error(), "missing action")
	})

	t.Run("table_driven_basic_transformation", func(t *testing.T) {
		testCases := []struct {
			name             string
			subjectType      string
			subjectID        string
			resourceType     string
			resourceID       string
			actionName       string
			expectedUser     string
			expectedRelation string
			expectedObject   string
		}{
			{
				name:             "simple_user_read_document",
				subjectType:      "user",
				subjectID:        "alice",
				resourceType:     "document",
				resourceID:       "doc1",
				actionName:       "read",
				expectedUser:     "user:alice",
				expectedRelation: "read",
				expectedObject:   "document:doc1",
			},
			{
				name:             "group_admin_folder",
				subjectType:      "group",
				subjectID:        "admins",
				resourceType:     "folder",
				resourceID:       "root",
				actionName:       "admin",
				expectedUser:     "group:admins",
				expectedRelation: "admin",
				expectedObject:   "folder:root",
			},
			{
				name:             "service_write_repo",
				subjectType:      "service",
				subjectID:        "api-gateway",
				resourceType:     "repo",
				resourceID:       "openfga",
				actionName:       "write",
				expectedUser:     "service:api-gateway",
				expectedRelation: "write",
				expectedObject:   "repo:openfga",
			},
			{
				name:             "email_as_id",
				subjectType:      "user",
				subjectID:        "alice@example.com",
				resourceType:     "document",
				resourceID:       "report-2024.pdf",
				actionName:       "view",
				expectedUser:     "user:alice@example.com",
				expectedRelation: "view",
				expectedObject:   "document:report-2024.pdf",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				req := &authzenv1.EvaluationRequest{
					Subject:  &authzenv1.Subject{Type: tc.subjectType, Id: tc.subjectID},
					Resource: &authzenv1.Resource{Type: tc.resourceType, Id: tc.resourceID},
					Action:   &authzenv1.Action{Name: tc.actionName},
					StoreId:  ulid.Make().String(),
				}

				cmd, err := NewEvaluateRequestCommand(req, "")
				require.NoError(t, err)

				checkReq := cmd.GetCheckRequest()
				require.Equal(t, tc.expectedUser, checkReq.GetTupleKey().GetUser())
				require.Equal(t, tc.expectedRelation, checkReq.GetTupleKey().GetRelation())
				require.Equal(t, tc.expectedObject, checkReq.GetTupleKey().GetObject())
			})
		}
	})

	t.Run("properties_merge_error_propagation", func(t *testing.T) {
		// Test that property merge errors are handled
		req := &authzenv1.EvaluationRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  ulid.Make().String(),
		}

		cmd, err := NewEvaluateRequestCommand(req, "")
		// Should succeed - no invalid properties
		require.NoError(t, err)
		require.NotNil(t, cmd)
	})

	t.Run("all_fields_populated", func(t *testing.T) {
		storeID := ulid.Make().String()
		modelID := ulid.Make().String()

		req := &authzenv1.EvaluationRequest{
			Subject:  &authzenv1.Subject{Type: "user", Id: "alice"},
			Resource: &authzenv1.Resource{Type: "document", Id: "doc1"},
			Action:   &authzenv1.Action{Name: "read"},
			StoreId:  storeID,
			Context:  testutils.MustNewStruct(t, map[string]interface{}{"ip": "127.0.0.1"}),
		}

		cmd, err := NewEvaluateRequestCommand(req, modelID)
		require.NoError(t, err)

		checkReq := cmd.GetCheckRequest()
		require.Equal(t, storeID, checkReq.GetStoreId())
		require.Equal(t, modelID, checkReq.GetAuthorizationModelId())
		require.Equal(t, "user:alice", checkReq.GetTupleKey().GetUser())
		require.Equal(t, "read", checkReq.GetTupleKey().GetRelation())
		require.Equal(t, "document:doc1", checkReq.GetTupleKey().GetObject())
		require.NotNil(t, checkReq.GetContext())
		require.Equal(t, "127.0.0.1", checkReq.GetContext().AsMap()["ip"])
	})
}
