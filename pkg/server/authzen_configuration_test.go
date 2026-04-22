package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc/metadata"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/cmd/util"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
)

func TestNormalizeAuthzenBaseURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		err      string
	}{
		{
			name:     "accepts_https_url",
			input:    "https://good.com",
			expected: "https://good.com",
		},
		{
			name:     "accepts_path_prefix_and_trims_trailing_slash",
			input:    "https://good.com/openfga/",
			expected: "https://good.com/openfga",
		},
		{
			name:  "rejects_relative_url",
			input: "/openfga",
			err:   "scheme must be http or https",
		},
		{
			name:  "rejects_non_http_scheme",
			input: "javascript://evil.example",
			err:   "scheme must be http or https",
		},
		{
			name:  "rejects_query_string",
			input: "https://good.com?q=x",
			err:   "URL must not include a query string or fragment",
		},
		{
			name:  "rejects_user_info",
			input: "https://user:pass@good.com",
			err:   "URL must not include user info",
		},
		{
			name:  "rejects_comma_separated_hosts",
			input: "https://good.com,evil.com",
			err:   "URL must contain exactly one host",
		},
		{
			name:  "rejects_fragment",
			input: "https://good.com#frag",
			err:   "URL must not include a query string or fragment",
		},
		{
			name:     "accepts_http_url",
			input:    "http://localhost:8080",
			expected: "http://localhost:8080",
		},
		{
			name:     "empty_input_returns_empty",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := serverconfig.NormalizeAuthzenBaseURL(tt.input)
			if tt.err != "" {
				require.EqualError(t, err, tt.err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestGetConfiguration(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	newServerAndStore := func(t *testing.T) (*Server, string) {
		t.Helper()
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithAuthzenBaseURL("https://pdp.example.com"),
			WithExperimentals(serverconfig.ExperimentalAuthZen),
		)
		t.Cleanup(s.Close)
		resp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		return s, resp.GetId()
	}

	t.Run("returns_pdp_identifier", func(t *testing.T) {
		s, storeID := newServerAndStore(t)

		md := metadata.New(map[string]string{
			":authority": "pdp.example.com",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		resp, err := s.GetConfiguration(ctx, &authzenv1.GetConfigurationRequest{StoreId: storeID})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, "https://pdp.example.com/stores/"+storeID, resp.GetPolicyDecisionPoint())
	})

	t.Run("returns_absolute_urls_from_configured_base_url", func(t *testing.T) {
		s, storeID := newServerAndStore(t)

		resp, err := s.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{StoreId: storeID})
		require.NoError(t, err)

		require.Equal(t, "https://pdp.example.com/stores/"+storeID+"/access/v1/evaluation", resp.GetAccessEvaluationEndpoint())
		require.Equal(t, "https://pdp.example.com/stores/"+storeID+"/access/v1/evaluations", resp.GetAccessEvaluationsEndpoint())
		require.Equal(t, "https://pdp.example.com/stores/"+storeID+"/access/v1/search/subject", resp.GetSearchSubjectEndpoint())
		require.Equal(t, "https://pdp.example.com/stores/"+storeID+"/access/v1/search/resource", resp.GetSearchResourceEndpoint())
		require.Equal(t, "https://pdp.example.com/stores/"+storeID+"/access/v1/search/action", resp.GetSearchActionEndpoint())
	})

	t.Run("ignores_request_host_headers", func(t *testing.T) {
		s, storeID := newServerAndStore(t)

		md := metadata.New(map[string]string{
			"x-forwarded-host":  "attacker.example",
			"x-forwarded-proto": "http",
			":authority":        "attacker.example",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		resp, err := s.GetConfiguration(ctx, &authzenv1.GetConfigurationRequest{StoreId: storeID})
		require.NoError(t, err)
		require.Equal(t, "https://pdp.example.com/stores/"+storeID+"/access/v1/evaluation", resp.GetAccessEvaluationEndpoint())
	})

	t.Run("returns_error_when_base_url_is_not_configured", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalAuthZen),
		)
		t.Cleanup(s.Close)
		storeResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)

		resp, err := s.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{StoreId: storeResp.GetId()})
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "AuthZEN discovery base URL is not configured")
	})
}
