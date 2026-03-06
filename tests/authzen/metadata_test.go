package authzen_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/build"
)

// TestGetConfiguration tests the AuthZEN GetConfiguration (metadata/discovery) endpoint.
// This endpoint returns PDP information, endpoint URLs, and capabilities.
func TestGetConfiguration(t *testing.T) {
	t.Run("returns_pdp_info", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")

		resp, err := tc.authzenClient.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{StoreId: tc.storeID})
		require.NoError(t, err)

		require.NotNil(t, resp.GetPolicyDecisionPoint())
		require.Equal(t, "OpenFGA", resp.GetPolicyDecisionPoint().GetName())
		require.Equal(t, build.Version, resp.GetPolicyDecisionPoint().GetVersion())
		require.NotEmpty(t, resp.GetPolicyDecisionPoint().GetDescription())
	})

	t.Run("returns_endpoints", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")

		resp, err := tc.authzenClient.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{StoreId: tc.storeID})
		require.NoError(t, err)

		require.NotNil(t, resp.GetAccessEndpoints())
		require.Contains(t, resp.GetAccessEndpoints().GetEvaluation(), "/access/v1/evaluation")
		require.Contains(t, resp.GetAccessEndpoints().GetEvaluations(), "/access/v1/evaluations")
		require.Contains(t, resp.GetAccessEndpoints().GetSubjectSearch(), "/access/v1/search/subject")
		require.Contains(t, resp.GetAccessEndpoints().GetResourceSearch(), "/access/v1/search/resource")
		require.Contains(t, resp.GetAccessEndpoints().GetActionSearch(), "/access/v1/search/action")
	})

	t.Run("returns_capabilities", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")

		resp, err := tc.authzenClient.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{StoreId: tc.storeID})
		require.NoError(t, err)

		require.NotEmpty(t, resp.GetCapabilities())
		require.Contains(t, resp.GetCapabilities(), "evaluation")
		require.Contains(t, resp.GetCapabilities(), "evaluations")
		require.Contains(t, resp.GetCapabilities(), "subject_search")
		require.Contains(t, resp.GetCapabilities(), "resource_search")
		require.Contains(t, resp.GetCapabilities(), "action_search")
	})

	t.Run("requires_experimental_flag", func(t *testing.T) {
		// GetConfiguration requires the experimental flag like all AuthZEN endpoints
		tc := setupTestContextWithExperimentals(t, []string{})

		resp, err := tc.openfgaClient.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)

		_, err = tc.authzenClient.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{StoreId: resp.GetId()})
		require.Error(t, err)
		require.Contains(t, err.Error(), "AuthZEN endpoints are experimental")
	})
}
