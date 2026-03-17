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

func TestGetBaseURLFromContext(t *testing.T) {
	tests := []struct {
		name     string
		md       metadata.MD
		expected string
	}{
		{
			name:     "comma_separated_hosts_takes_first",
			md:       metadata.New(map[string]string{"x-forwarded-host": "good.com, evil.com"}),
			expected: "https://good.com",
		},
		{
			name:     "comma_separated_hosts_trims_whitespace",
			md:       metadata.New(map[string]string{"x-forwarded-host": "  good.com , evil.com"}),
			expected: "https://good.com",
		},
		{
			name:     "rejects_host_with_path",
			md:       metadata.New(map[string]string{"x-forwarded-host": "evil.com/path"}),
			expected: "",
		},
		{
			name:     "rejects_host_with_query",
			md:       metadata.New(map[string]string{"x-forwarded-host": "evil.com?q=x"}),
			expected: "",
		},
		{
			name:     "rejects_host_with_fragment",
			md:       metadata.New(map[string]string{"x-forwarded-host": "evil.com#frag"}),
			expected: "",
		},
		{
			name:     "accepts_host_with_port",
			md:       metadata.New(map[string]string{":authority": "example.com:8080"}),
			expected: "https://example.com:8080",
		},
		{
			name:     "rejects_empty_host_after_trim",
			md:       metadata.New(map[string]string{"x-forwarded-host": "  "}),
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := metadata.NewIncomingContext(context.Background(), tt.md)
			result := getBaseURLFromContext(ctx)
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

	t.Run("returns_absolute_urls_from_request_context", func(t *testing.T) {
		s, storeID := newServerAndStore(t)

		md := metadata.New(map[string]string{
			":authority": "pdp.example.com",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		resp, err := s.GetConfiguration(ctx, &authzenv1.GetConfigurationRequest{StoreId: storeID})
		require.NoError(t, err)

		require.Equal(t, "https://pdp.example.com/stores/"+storeID+"/access/v1/evaluation", resp.GetAccessEvaluationEndpoint())
		require.Equal(t, "https://pdp.example.com/stores/"+storeID+"/access/v1/evaluations", resp.GetAccessEvaluationsEndpoint())
		require.Equal(t, "https://pdp.example.com/stores/"+storeID+"/access/v1/search/subject", resp.GetSearchSubjectEndpoint())
		require.Equal(t, "https://pdp.example.com/stores/"+storeID+"/access/v1/search/resource", resp.GetSearchResourceEndpoint())
		require.Equal(t, "https://pdp.example.com/stores/"+storeID+"/access/v1/search/action", resp.GetSearchActionEndpoint())
	})

	t.Run("returns_absolute_urls_with_forwarded_headers", func(t *testing.T) {
		s, storeID := newServerAndStore(t)

		md := metadata.New(map[string]string{
			"x-forwarded-host":  "api.mycompany.com",
			"x-forwarded-proto": "https",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		resp, err := s.GetConfiguration(ctx, &authzenv1.GetConfigurationRequest{StoreId: storeID})
		require.NoError(t, err)
		require.Equal(t, "https://api.mycompany.com/stores/"+storeID+"/access/v1/evaluation", resp.GetAccessEvaluationEndpoint())
	})

	t.Run("returns_error_when_no_host_context", func(t *testing.T) {
		s, storeID := newServerAndStore(t)

		resp, err := s.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{StoreId: storeID})
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "unable to determine base URL")
	})

	t.Run("scheme_handling", func(t *testing.T) {
		s, storeID := newServerAndStore(t)

		tests := []struct {
			name           string
			proto          string
			expectedScheme string
		}{
			{"accepts_http_scheme", "http", "http"},
			{"rejects_invalid_scheme_defaults_to_https", "javascript", "https"},
			{"scheme_validation_is_case_insensitive", "HTTPS", "https"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				md := metadata.New(map[string]string{
					"x-forwarded-host":  "api.mycompany.com",
					"x-forwarded-proto": tt.proto,
				})
				ctx := metadata.NewIncomingContext(context.Background(), md)

				resp, err := s.GetConfiguration(ctx, &authzenv1.GetConfigurationRequest{StoreId: storeID})
				require.NoError(t, err)
				require.Equal(t, tt.expectedScheme+"://api.mycompany.com/stores/"+storeID+"/access/v1/evaluation", resp.GetAccessEvaluationEndpoint())
			})
		}
	})
}
