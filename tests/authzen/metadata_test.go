package authzen_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

// TestGetConfiguration tests the AuthZEN GetConfiguration (metadata/discovery) endpoint.
// This endpoint returns a PDP identifier URL, flat endpoint URLs, and capabilities per the AuthZEN spec.
func TestGetConfiguration(t *testing.T) {
	t.Run("returns_pdp_identifier", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")

		resp, err := tc.authzenClient.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{StoreId: tc.storeID})
		require.NoError(t, err)

		// policy_decision_point is a URL string per AuthZEN spec
		require.NotEmpty(t, resp.GetPolicyDecisionPoint())
		require.Contains(t, resp.GetPolicyDecisionPoint(), tc.storeID)
	})

	t.Run("returns_endpoints", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")

		resp, err := tc.authzenClient.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{StoreId: tc.storeID})
		require.NoError(t, err)

		// Flat endpoint URL fields per AuthZEN spec
		require.Contains(t, resp.GetAccessEvaluationEndpoint(), "/access/v1/evaluation")
		require.Contains(t, resp.GetAccessEvaluationsEndpoint(), "/access/v1/evaluations")
		require.Contains(t, resp.GetSearchSubjectEndpoint(), "/access/v1/search/subject")
		require.Contains(t, resp.GetSearchResourceEndpoint(), "/access/v1/search/resource")
		require.Contains(t, resp.GetSearchActionEndpoint(), "/access/v1/search/action")
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
