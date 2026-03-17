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

		resp, err := tc.openfgaClient.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: testStoreName})
		require.NoError(t, err)

		_, err = tc.authzenClient.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{StoreId: resp.GetId()})
		require.Error(t, err)
		require.Contains(t, err.Error(), "AuthZEN endpoints are experimental")
	})
}

// TestGetConfigurationHTTPHeaderForwarding verifies that X-Forwarded-Proto is
// correctly forwarded through grpc-gateway to getBaseURLFromContext.
//
// Grpc-gateway's annotateContext has built-in special handling for
// X-Forwarded-For and X-Forwarded-Host (they bypass the header matcher and
// are always forwarded as gRPC metadata). However, X-Forwarded-Proto is NOT
// included in that special handling, and DefaultHeaderMatcher only forwards
// IANA permanent headers (Accept, Cookie, Host, etc.) and Grpc-Metadata-*
// prefixed headers. Without an explicit match in WithIncomingHeaderMatcher,
// X-Forwarded-Proto would be silently dropped and the scheme would always
// default to "https".
//
// These tests send real HTTP requests through grpc-gateway to verify the
// header reaches the server and affects the returned URLs.
func TestGetConfigurationHTTPHeaderForwarding(t *testing.T) {
	tc := setupTestContextWithStore(t)

	configURL := "http://" + tc.httpAddr + "/.well-known/authzen-configuration/" + tc.storeID

	t.Run("x_forwarded_proto_http_is_forwarded", func(t *testing.T) {
		result := mustGetConfigurationHTTP(t, configURL, map[string]string{
			"X-Forwarded-Proto": "http",
			"X-Forwarded-Host":  "api.example.com",
		})

		// The evaluation endpoint URL must start with http://, proving
		// X-Forwarded-Proto was forwarded through grpc-gateway.
		evalEndpoint, ok := result["access_evaluation_endpoint"].(string)
		require.True(t, ok)
		require.Contains(t, evalEndpoint, "http://api.example.com/")
	})

	t.Run("x_forwarded_proto_https_is_forwarded", func(t *testing.T) {
		result := mustGetConfigurationHTTP(t, configURL, map[string]string{
			"X-Forwarded-Proto": "https",
			"X-Forwarded-Host":  "api.example.com",
		})

		evalEndpoint, ok := result["access_evaluation_endpoint"].(string)
		require.True(t, ok)
		require.Contains(t, evalEndpoint, "https://api.example.com/")
	})

	t.Run("without_x_forwarded_proto_defaults_to_https", func(t *testing.T) {
		result := mustGetConfigurationHTTP(t, configURL, map[string]string{
			"X-Forwarded-Host": "api.example.com",
		})

		evalEndpoint, ok := result["access_evaluation_endpoint"].(string)
		require.True(t, ok)
		require.Contains(t, evalEndpoint, "https://api.example.com/")
	})
}
