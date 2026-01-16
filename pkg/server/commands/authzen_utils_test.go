package commands

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
)

func TestMergePropertiesToContext(t *testing.T) {
	t.Run("returns_nil_when_all_inputs_are_nil", func(t *testing.T) {
		result, err := MergePropertiesToContext(nil, nil, nil, nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("returns_nil_when_all_inputs_have_no_properties", func(t *testing.T) {
		subject := &authzenv1.Subject{Type: "user", Id: "alice"}
		resource := &authzenv1.Resource{Type: "document", Id: "doc1"}
		action := &authzenv1.Action{Name: "read"}

		result, err := MergePropertiesToContext(nil, subject, resource, action)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("merges_subject_properties_with_prefix", func(t *testing.T) {
		subjectProps, err := structpb.NewStruct(map[string]interface{}{
			"department": "engineering",
			"level":      "senior",
		})
		require.NoError(t, err)

		subject := &authzenv1.Subject{
			Type:       "user",
			Id:         "alice",
			Properties: subjectProps,
		}

		result, err := MergePropertiesToContext(nil, subject, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		resultMap := result.AsMap()
		require.Equal(t, "engineering", resultMap["subject_department"])
		require.Equal(t, "senior", resultMap["subject_level"])
	})

	t.Run("merges_resource_properties_with_prefix", func(t *testing.T) {
		resourceProps, err := structpb.NewStruct(map[string]interface{}{
			"classification": "confidential",
			"owner":          "bob",
		})
		require.NoError(t, err)

		resource := &authzenv1.Resource{
			Type:       "document",
			Id:         "doc1",
			Properties: resourceProps,
		}

		result, err := MergePropertiesToContext(nil, nil, resource, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		resultMap := result.AsMap()
		require.Equal(t, "confidential", resultMap["resource_classification"])
		require.Equal(t, "bob", resultMap["resource_owner"])
	})

	t.Run("merges_action_properties_with_prefix", func(t *testing.T) {
		actionProps, err := structpb.NewStruct(map[string]interface{}{
			"requires_approval": true,
			"max_size":          float64(1000),
		})
		require.NoError(t, err)

		action := &authzenv1.Action{
			Name:       "write",
			Properties: actionProps,
		}

		result, err := MergePropertiesToContext(nil, nil, nil, action)
		require.NoError(t, err)
		require.NotNil(t, result)

		resultMap := result.AsMap()
		require.Equal(t, true, resultMap["action_requires_approval"])
		require.InDelta(t, float64(1000), resultMap["action_max_size"], 0.001)
	})

	t.Run("request_context_takes_precedence", func(t *testing.T) {
		subjectProps, err := structpb.NewStruct(map[string]interface{}{
			"department": "engineering",
		})
		require.NoError(t, err)

		subject := &authzenv1.Subject{
			Type:       "user",
			Id:         "alice",
			Properties: subjectProps,
		}

		requestContext, err := structpb.NewStruct(map[string]interface{}{
			"subject_department": "overridden_value",
			"custom_field":       "custom_value",
		})
		require.NoError(t, err)

		result, err := MergePropertiesToContext(requestContext, subject, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		resultMap := result.AsMap()
		// Request context should override subject property
		require.Equal(t, "overridden_value", resultMap["subject_department"])
		require.Equal(t, "custom_value", resultMap["custom_field"])
	})

	t.Run("merges_all_sources_with_correct_precedence", func(t *testing.T) {
		subjectProps, err := structpb.NewStruct(map[string]interface{}{
			"role": "admin",
		})
		require.NoError(t, err)

		resourceProps, err := structpb.NewStruct(map[string]interface{}{
			"type": "secret",
		})
		require.NoError(t, err)

		actionProps, err := structpb.NewStruct(map[string]interface{}{
			"audit": true,
		})
		require.NoError(t, err)

		requestContext, err := structpb.NewStruct(map[string]interface{}{
			"ip_address": "192.168.1.1",
		})
		require.NoError(t, err)

		subject := &authzenv1.Subject{Type: "user", Id: "alice", Properties: subjectProps}
		resource := &authzenv1.Resource{Type: "document", Id: "doc1", Properties: resourceProps}
		action := &authzenv1.Action{Name: "read", Properties: actionProps}

		result, err := MergePropertiesToContext(requestContext, subject, resource, action)
		require.NoError(t, err)
		require.NotNil(t, result)

		resultMap := result.AsMap()
		require.Equal(t, "admin", resultMap["subject_role"])
		require.Equal(t, "secret", resultMap["resource_type"])
		require.Equal(t, true, resultMap["action_audit"])
		require.Equal(t, "192.168.1.1", resultMap["ip_address"])
	})

	t.Run("handles_request_context_only", func(t *testing.T) {
		requestContext, err := structpb.NewStruct(map[string]interface{}{
			"timestamp": "2024-01-01T00:00:00Z",
		})
		require.NoError(t, err)

		result, err := MergePropertiesToContext(requestContext, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		resultMap := result.AsMap()
		require.Equal(t, "2024-01-01T00:00:00Z", resultMap["timestamp"])
	})

	// Tests for complex types (arrays, nested objects) per AuthZEN spec
	// The spec states properties can contain "complex values, such as arrays and objects"

	t.Run("handles_array_of_strings", func(t *testing.T) {
		subjectProps, err := structpb.NewStruct(map[string]interface{}{
			"roles": []interface{}{"admin", "editor", "viewer"},
		})
		require.NoError(t, err)

		subject := &authzenv1.Subject{
			Type:       "user",
			Id:         "alice",
			Properties: subjectProps,
		}

		result, err := MergePropertiesToContext(nil, subject, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		resultMap := result.AsMap()
		roles, ok := resultMap["subject_roles"].([]interface{})
		require.True(t, ok, "expected subject_roles to be an array")
		require.Len(t, roles, 3)
		require.Equal(t, "admin", roles[0])
		require.Equal(t, "editor", roles[1])
		require.Equal(t, "viewer", roles[2])
	})

	t.Run("handles_array_of_numbers", func(t *testing.T) {
		resourceProps, err := structpb.NewStruct(map[string]interface{}{
			"allowed_ports": []interface{}{float64(80), float64(443), float64(8080)},
		})
		require.NoError(t, err)

		resource := &authzenv1.Resource{
			Type:       "server",
			Id:         "prod-1",
			Properties: resourceProps,
		}

		result, err := MergePropertiesToContext(nil, nil, resource, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		resultMap := result.AsMap()
		ports, ok := resultMap["resource_allowed_ports"].([]interface{})
		require.True(t, ok, "expected resource_allowed_ports to be an array")
		require.Len(t, ports, 3)
		require.InDelta(t, float64(80), ports[0], 0.001)
		require.InDelta(t, float64(443), ports[1], 0.001)
		require.InDelta(t, float64(8080), ports[2], 0.001)
	})

	t.Run("handles_nested_object", func(t *testing.T) {
		// Example from AuthZEN spec: library_record with title and isbn
		resourceProps, err := structpb.NewStruct(map[string]interface{}{
			"library_record": map[string]interface{}{
				"title": "AuthZEN in Action",
				"isbn":  "978-0593383322",
			},
		})
		require.NoError(t, err)

		resource := &authzenv1.Resource{
			Type:       "book",
			Id:         "123",
			Properties: resourceProps,
		}

		result, err := MergePropertiesToContext(nil, nil, resource, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		resultMap := result.AsMap()
		libraryRecord, ok := resultMap["resource_library_record"].(map[string]interface{})
		require.True(t, ok, "expected resource_library_record to be an object")
		require.Equal(t, "AuthZEN in Action", libraryRecord["title"])
		require.Equal(t, "978-0593383322", libraryRecord["isbn"])
	})

	t.Run("handles_deeply_nested_objects", func(t *testing.T) {
		// Test dictionaries of dictionaries
		subjectProps, err := structpb.NewStruct(map[string]interface{}{
			"organization": map[string]interface{}{
				"name": "Acme Corp",
				"address": map[string]interface{}{
					"street":  "123 Main St",
					"city":    "San Francisco",
					"country": "USA",
				},
				"metadata": map[string]interface{}{
					"created_at": "2024-01-01",
					"tier":       "enterprise",
				},
			},
		})
		require.NoError(t, err)

		subject := &authzenv1.Subject{
			Type:       "user",
			Id:         "alice",
			Properties: subjectProps,
		}

		result, err := MergePropertiesToContext(nil, subject, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		resultMap := result.AsMap()
		org, ok := resultMap["subject_organization"].(map[string]interface{})
		require.True(t, ok, "expected subject_organization to be an object")
		require.Equal(t, "Acme Corp", org["name"])

		address, ok := org["address"].(map[string]interface{})
		require.True(t, ok, "expected address to be a nested object")
		require.Equal(t, "123 Main St", address["street"])
		require.Equal(t, "San Francisco", address["city"])
		require.Equal(t, "USA", address["country"])

		metadata, ok := org["metadata"].(map[string]interface{})
		require.True(t, ok, "expected metadata to be a nested object")
		require.Equal(t, "2024-01-01", metadata["created_at"])
		require.Equal(t, "enterprise", metadata["tier"])
	})

	t.Run("handles_array_of_objects", func(t *testing.T) {
		subjectProps, err := structpb.NewStruct(map[string]interface{}{
			"permissions": []interface{}{
				map[string]interface{}{
					"resource": "documents",
					"actions":  []interface{}{"read", "write"},
				},
				map[string]interface{}{
					"resource": "reports",
					"actions":  []interface{}{"read"},
				},
			},
		})
		require.NoError(t, err)

		subject := &authzenv1.Subject{
			Type:       "user",
			Id:         "alice",
			Properties: subjectProps,
		}

		result, err := MergePropertiesToContext(nil, subject, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		resultMap := result.AsMap()
		permissions, ok := resultMap["subject_permissions"].([]interface{})
		require.True(t, ok, "expected subject_permissions to be an array")
		require.Len(t, permissions, 2)

		perm1, ok := permissions[0].(map[string]interface{})
		require.True(t, ok, "expected first permission to be an object")
		require.Equal(t, "documents", perm1["resource"])
		actions1, ok := perm1["actions"].([]interface{})
		require.True(t, ok, "expected actions to be an array")
		require.Equal(t, []interface{}{"read", "write"}, actions1)

		perm2, ok := permissions[1].(map[string]interface{})
		require.True(t, ok, "expected second permission to be an object")
		require.Equal(t, "reports", perm2["resource"])
	})

	t.Run("handles_mixed_complex_types", func(t *testing.T) {
		// Mix of all complex types in one property set
		subjectProps, err := structpb.NewStruct(map[string]interface{}{
			"tags": []interface{}{"vip", "verified"},
			"profile": map[string]interface{}{
				"display_name": "Alice Smith",
				"settings": map[string]interface{}{
					"notifications": true,
					"theme":         "dark",
				},
			},
		})
		require.NoError(t, err)

		resourceProps, err := structpb.NewStruct(map[string]interface{}{
			"acl": []interface{}{
				map[string]interface{}{"principal": "user:alice", "permission": "owner"},
				map[string]interface{}{"principal": "group:admins", "permission": "admin"},
			},
		})
		require.NoError(t, err)

		actionProps, err := structpb.NewStruct(map[string]interface{}{
			"constraints": map[string]interface{}{
				"max_file_size": float64(10485760),
				"allowed_types": []interface{}{"pdf", "docx", "txt"},
			},
		})
		require.NoError(t, err)

		subject := &authzenv1.Subject{Type: "user", Id: "alice", Properties: subjectProps}
		resource := &authzenv1.Resource{Type: "folder", Id: "shared", Properties: resourceProps}
		action := &authzenv1.Action{Name: "upload", Properties: actionProps}

		result, err := MergePropertiesToContext(nil, subject, resource, action)
		require.NoError(t, err)
		require.NotNil(t, result)

		resultMap := result.AsMap()

		// Verify subject properties
		tags, ok := resultMap["subject_tags"].([]interface{})
		require.True(t, ok)
		require.Equal(t, []interface{}{"vip", "verified"}, tags)

		profile, ok := resultMap["subject_profile"].(map[string]interface{})
		require.True(t, ok)
		require.Equal(t, "Alice Smith", profile["display_name"])

		// Verify resource properties
		acl, ok := resultMap["resource_acl"].([]interface{})
		require.True(t, ok)
		require.Len(t, acl, 2)

		// Verify action properties
		constraints, ok := resultMap["action_constraints"].(map[string]interface{})
		require.True(t, ok)
		require.InDelta(t, float64(10485760), constraints["max_file_size"], 0.001)
		allowedTypes, ok := constraints["allowed_types"].([]interface{})
		require.True(t, ok)
		require.Equal(t, []interface{}{"pdf", "docx", "txt"}, allowedTypes)
	})

	t.Run("handles_null_values_in_properties", func(t *testing.T) {
		subjectProps, err := structpb.NewStruct(map[string]interface{}{
			"department":  "engineering",
			"manager":     nil,
			"preferences": nil,
		})
		require.NoError(t, err)

		subject := &authzenv1.Subject{
			Type:       "user",
			Id:         "alice",
			Properties: subjectProps,
		}

		result, err := MergePropertiesToContext(nil, subject, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		resultMap := result.AsMap()
		require.Equal(t, "engineering", resultMap["subject_department"])
		require.Nil(t, resultMap["subject_manager"])
		require.Nil(t, resultMap["subject_preferences"])
	})

	t.Run("handles_empty_array", func(t *testing.T) {
		subjectProps, err := structpb.NewStruct(map[string]interface{}{
			"roles": []interface{}{},
		})
		require.NoError(t, err)

		subject := &authzenv1.Subject{
			Type:       "user",
			Id:         "alice",
			Properties: subjectProps,
		}

		result, err := MergePropertiesToContext(nil, subject, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		resultMap := result.AsMap()
		roles, ok := resultMap["subject_roles"].([]interface{})
		require.True(t, ok)
		require.Empty(t, roles)
	})

	t.Run("handles_empty_object", func(t *testing.T) {
		subjectProps, err := structpb.NewStruct(map[string]interface{}{
			"metadata": map[string]interface{}{},
		})
		require.NoError(t, err)

		subject := &authzenv1.Subject{
			Type:       "user",
			Id:         "alice",
			Properties: subjectProps,
		}

		result, err := MergePropertiesToContext(nil, subject, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		resultMap := result.AsMap()
		metadata, ok := resultMap["subject_metadata"].(map[string]interface{})
		require.True(t, ok)
		require.Empty(t, metadata)
	})
}
