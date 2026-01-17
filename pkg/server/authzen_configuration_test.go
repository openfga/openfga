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
	"github.com/openfga/openfga/internal/build"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
)

func TestGetConfiguration(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	t.Run("feature_flag_disabled", func(t *testing.T) {
		// Server without AuthZEN experimental enabled
		s := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		storeID := createStoreResp.GetId()

		resp, err := s.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{StoreId: storeID})
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "experimental")
	})

	t.Run("returns_pdp_metadata", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		storeID := createStoreResp.GetId()

		resp, err := s.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{StoreId: storeID})
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Check PDP info
		require.NotNil(t, resp.GetPolicyDecisionPoint())
		require.Equal(t, "OpenFGA", resp.GetPolicyDecisionPoint().GetName())
		require.Equal(t, build.Version, resp.GetPolicyDecisionPoint().GetVersion())
		require.NotEmpty(t, resp.GetPolicyDecisionPoint().GetDescription())
		require.Contains(t, resp.GetPolicyDecisionPoint().GetDescription(), "OpenFGA")
		require.Contains(t, resp.GetPolicyDecisionPoint().GetDescription(), "AuthZEN")
	})

	t.Run("returns_absolute_urls_from_request_context", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		storeID := createStoreResp.GetId()

		// Create context with metadata simulating an HTTP request
		md := metadata.New(map[string]string{
			":authority": "pdp.example.com",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		resp, err := s.GetConfiguration(ctx, &authzenv1.GetConfigurationRequest{StoreId: storeID})
		require.NoError(t, err)

		// Verify absolute URLs per AuthZEN spec
		require.NotNil(t, resp.GetAccessEndpoints())
		require.Equal(t, "https://pdp.example.com/stores/"+storeID+"/access/v1/evaluation", resp.GetAccessEndpoints().GetEvaluation())
		require.Equal(t, "https://pdp.example.com/stores/"+storeID+"/access/v1/evaluations", resp.GetAccessEndpoints().GetEvaluations())
		require.Equal(t, "https://pdp.example.com/stores/"+storeID+"/access/v1/search/subject", resp.GetAccessEndpoints().GetSubjectSearch())
		require.Equal(t, "https://pdp.example.com/stores/"+storeID+"/access/v1/search/resource", resp.GetAccessEndpoints().GetResourceSearch())
		require.Equal(t, "https://pdp.example.com/stores/"+storeID+"/access/v1/search/action", resp.GetAccessEndpoints().GetActionSearch())
	})

	t.Run("returns_absolute_urls_with_forwarded_headers", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		storeID := createStoreResp.GetId()

		// Create context with x-forwarded headers (common behind load balancers)
		md := metadata.New(map[string]string{
			"x-forwarded-host":  "api.mycompany.com",
			"x-forwarded-proto": "https",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		resp, err := s.GetConfiguration(ctx, &authzenv1.GetConfigurationRequest{StoreId: storeID})
		require.NoError(t, err)

		// Verify absolute URLs use forwarded headers
		require.Equal(t, "https://api.mycompany.com/stores/"+storeID+"/access/v1/evaluation", resp.GetAccessEndpoints().GetEvaluation())
	})

	t.Run("returns_path_only_when_no_host_context", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		storeID := createStoreResp.GetId()

		// No metadata context - falls back to path-only
		resp, err := s.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{StoreId: storeID})
		require.NoError(t, err)

		// Verify path-only when no host available
		require.NotNil(t, resp.GetAccessEndpoints())
		require.Equal(t, "/stores/"+storeID+"/access/v1/evaluation", resp.GetAccessEndpoints().GetEvaluation())
	})

	t.Run("returns_capabilities", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		storeID := createStoreResp.GetId()

		resp, err := s.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{StoreId: storeID})
		require.NoError(t, err)

		require.NotEmpty(t, resp.GetCapabilities())
		require.Len(t, resp.GetCapabilities(), 5)
		require.Contains(t, resp.GetCapabilities(), "evaluation")
		require.Contains(t, resp.GetCapabilities(), "evaluations")
		require.Contains(t, resp.GetCapabilities(), "subject_search")
		require.Contains(t, resp.GetCapabilities(), "resource_search")
		require.Contains(t, resp.GetCapabilities(), "action_search")
	})

	t.Run("authzen_spec_compliance", func(t *testing.T) {
		s := MustNewServerWithOpts(
			WithDatastore(ds),
			WithExperimentals(serverconfig.ExperimentalEnableAuthZen),
		)
		t.Cleanup(s.Close)

		createStoreResp, err := s.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		storeID := createStoreResp.GetId()

		// Create context with host metadata to get full URLs per AuthZEN spec
		md := metadata.New(map[string]string{
			":authority": "pdp.example.com",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		resp, err := s.GetConfiguration(ctx, &authzenv1.GetConfigurationRequest{StoreId: storeID})
		require.NoError(t, err)

		// Verify all required fields are present per AuthZEN spec
		require.NotNil(t, resp.GetPolicyDecisionPoint(), "PolicyDecisionPoint is required")
		require.NotNil(t, resp.GetAccessEndpoints(), "AccessEndpoints is required")
		require.NotNil(t, resp.GetCapabilities(), "Capabilities is required")

		// Verify PDP fields are non-empty strings
		require.NotEmpty(t, resp.GetPolicyDecisionPoint().GetName(), "PDP name is required")
		require.NotEmpty(t, resp.GetPolicyDecisionPoint().GetVersion(), "PDP version is required")

		// Verify endpoints are absolute URLs per AuthZEN spec (scheme + host + path)
		require.Regexp(t, `^https://pdp\.example\.com/stores/[0-9A-Z]+/access/v1/evaluation$`, resp.GetAccessEndpoints().GetEvaluation())
		require.Regexp(t, `^https://pdp\.example\.com/stores/[0-9A-Z]+/access/v1/evaluations$`, resp.GetAccessEndpoints().GetEvaluations())
		require.Regexp(t, `^https://pdp\.example\.com/stores/[0-9A-Z]+/access/v1/search/subject$`, resp.GetAccessEndpoints().GetSubjectSearch())
		require.Regexp(t, `^https://pdp\.example\.com/stores/[0-9A-Z]+/access/v1/search/resource$`, resp.GetAccessEndpoints().GetResourceSearch())
		require.Regexp(t, `^https://pdp\.example\.com/stores/[0-9A-Z]+/access/v1/search/action$`, resp.GetAccessEndpoints().GetActionSearch())

		// Verify no template placeholders remain
		require.NotContains(t, resp.GetAccessEndpoints().GetEvaluation(), "{store_id}")
		require.NotContains(t, resp.GetAccessEndpoints().GetEvaluations(), "{store_id}")
	})
}
