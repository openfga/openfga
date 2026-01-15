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
}
