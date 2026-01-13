package authzen_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	"github.com/openfga/openfga/internal/build"
)

// TestGetConfiguration tests the AuthZEN GetConfiguration (metadata/discovery) endpoint.
// This endpoint returns PDP information, endpoint URLs, and capabilities.
func TestGetConfiguration(t *testing.T) {
	t.Run("returns_pdp_info", func(t *testing.T) {
		tc := setupTestContext(t, "memory")

		resp, err := tc.authzenClient.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{})
		require.NoError(t, err)

		require.NotNil(t, resp.PolicyDecisionPoint)
		require.Equal(t, "OpenFGA", resp.PolicyDecisionPoint.Name)
		require.Equal(t, build.Version, resp.PolicyDecisionPoint.Version)
		require.NotEmpty(t, resp.PolicyDecisionPoint.Description)
	})

	t.Run("returns_endpoints", func(t *testing.T) {
		tc := setupTestContext(t, "memory")

		resp, err := tc.authzenClient.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{})
		require.NoError(t, err)

		require.NotNil(t, resp.AccessEndpoints)
		require.Contains(t, resp.AccessEndpoints.Evaluation, "/access/v1/evaluation")
		require.Contains(t, resp.AccessEndpoints.Evaluations, "/access/v1/evaluations")
		require.Contains(t, resp.AccessEndpoints.SubjectSearch, "/access/v1/search/subject")
		require.Contains(t, resp.AccessEndpoints.ResourceSearch, "/access/v1/search/resource")
		require.Contains(t, resp.AccessEndpoints.ActionSearch, "/access/v1/search/action")
	})

	t.Run("returns_capabilities", func(t *testing.T) {
		tc := setupTestContext(t, "memory")

		resp, err := tc.authzenClient.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{})
		require.NoError(t, err)

		require.NotEmpty(t, resp.Capabilities)
		require.Contains(t, resp.Capabilities, "evaluation")
		require.Contains(t, resp.Capabilities, "evaluations")
		require.Contains(t, resp.Capabilities, "subject_search")
		require.Contains(t, resp.Capabilities, "resource_search")
		require.Contains(t, resp.Capabilities, "action_search")
	})

	t.Run("does_not_require_experimental_flag", func(t *testing.T) {
		// GetConfiguration should work even without the experimental flag
		// This is a discovery endpoint
		tc := setupTestContext(t, "memory")

		resp, err := tc.authzenClient.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}
