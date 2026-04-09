package authzen_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

func mustGetConfigurationHTTP(t *testing.T, url string, headers map[string]string) map[string]any {
	t.Helper()

	req, err := http.NewRequest("GET", url, nil)
	require.NoError(t, err)
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)

	var result map[string]any
	require.NoError(t, json.Unmarshal(body, &result))

	return result
}

// TestGetConfiguration tests the AuthZEN GetConfiguration (metadata/discovery) endpoint.
// This endpoint returns a PDP identifier URL, flat endpoint URLs, and capabilities per the AuthZEN spec.
func TestGetConfiguration(t *testing.T) {
	t.Run("returns_pdp_identifier", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")

		resp, err := tc.authzenClient.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{StoreId: tc.storeID})
		require.NoError(t, err)

		// policy_decision_point is a URL string per AuthZEN spec
		require.Equal(t, "http://openfga.example/stores/"+tc.storeID, resp.GetPolicyDecisionPoint())
	})

	t.Run("returns_endpoints", func(t *testing.T) {
		tc := setupTestContext(t)
		tc.createStore("test-store")

		resp, err := tc.authzenClient.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{StoreId: tc.storeID})
		require.NoError(t, err)

		// Flat endpoint URL fields per AuthZEN spec
		require.Equal(t, "http://openfga.example/stores/"+tc.storeID+"/access/v1/evaluation", resp.GetAccessEvaluationEndpoint())
		require.Equal(t, "http://openfga.example/stores/"+tc.storeID+"/access/v1/evaluations", resp.GetAccessEvaluationsEndpoint())
		require.Equal(t, "http://openfga.example/stores/"+tc.storeID+"/access/v1/search/subject", resp.GetSearchSubjectEndpoint())
		require.Equal(t, "http://openfga.example/stores/"+tc.storeID+"/access/v1/search/resource", resp.GetSearchResourceEndpoint())
		require.Equal(t, "http://openfga.example/stores/"+tc.storeID+"/access/v1/search/action", resp.GetSearchActionEndpoint())
	})

	t.Run("requires_experimental_flag", func(t *testing.T) {
		// GetConfiguration requires the experimental flag like all AuthZEN endpoints
		tc := setupTestContextWithExperimentals(t, []string{})

		resp, err := tc.openfgaClient.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: testStoreName})
		require.NoError(t, err)

		_, err = tc.authzenClient.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{StoreId: resp.GetId()})
		require.Error(t, err)
		require.Contains(t, err.Error(), "AuthZEN endpoints are experimental")
	})
}

// TestGetConfigurationHTTPHeadersIgnored verifies that discovery metadata is
// anchored to the configured base URL instead of request-supplied host headers.
func TestGetConfigurationHTTPHeadersIgnored(t *testing.T) {
	tc := setupTestContextWithStore(t)

	configURL := "http://" + tc.httpAddr + "/.well-known/authzen-configuration/" + tc.storeID

	t.Run("host_header_poisoning_is_ignored", func(t *testing.T) {
		result := mustGetConfigurationHTTP(t, configURL, map[string]string{
			"X-Forwarded-Proto": "http",
			"X-Forwarded-Host":  "attacker.example",
			"Host":              "attacker.example",
		})

		evalEndpoint, ok := result["access_evaluation_endpoint"].(string)
		require.True(t, ok)
		require.Equal(t, "http://openfga.example/stores/"+tc.storeID+"/access/v1/evaluation", evalEndpoint)
	})
}
