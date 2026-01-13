package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"

	"github.com/openfga/openfga/cmd/util"
	"github.com/openfga/openfga/internal/build"
)

func TestGetConfiguration(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	_, ds, _ := util.MustBootstrapDatastore(t, "memory")

	t.Run("returns_pdp_metadata", func(t *testing.T) {
		s := MustNewServerWithOpts(WithDatastore(ds))
		t.Cleanup(s.Close)

		resp, err := s.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{})
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

	t.Run("returns_correct_endpoints", func(t *testing.T) {
		s := MustNewServerWithOpts(WithDatastore(ds))
		t.Cleanup(s.Close)

		resp, err := s.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{})
		require.NoError(t, err)

		require.NotNil(t, resp.GetAccessEndpoints())
		require.Equal(t, "/stores/{store_id}/access/v1/evaluation", resp.GetAccessEndpoints().GetEvaluation())
		require.Equal(t, "/stores/{store_id}/access/v1/evaluations", resp.GetAccessEndpoints().GetEvaluations())
		require.Equal(t, "/stores/{store_id}/access/v1/search/subject", resp.GetAccessEndpoints().GetSubjectSearch())
		require.Equal(t, "/stores/{store_id}/access/v1/search/resource", resp.GetAccessEndpoints().GetResourceSearch())
		require.Equal(t, "/stores/{store_id}/access/v1/search/action", resp.GetAccessEndpoints().GetActionSearch())
	})

	t.Run("returns_capabilities", func(t *testing.T) {
		s := MustNewServerWithOpts(WithDatastore(ds))
		t.Cleanup(s.Close)

		resp, err := s.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{})
		require.NoError(t, err)

		require.NotEmpty(t, resp.GetCapabilities())
		require.Len(t, resp.GetCapabilities(), 5)
		require.Contains(t, resp.GetCapabilities(), "evaluation")
		require.Contains(t, resp.GetCapabilities(), "evaluations")
		require.Contains(t, resp.GetCapabilities(), "subject_search")
		require.Contains(t, resp.GetCapabilities(), "resource_search")
		require.Contains(t, resp.GetCapabilities(), "action_search")
	})

	t.Run("response_format_compliance", func(t *testing.T) {
		s := MustNewServerWithOpts(WithDatastore(ds))
		t.Cleanup(s.Close)

		resp, err := s.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{})
		require.NoError(t, err)

		// Verify all required fields are present per AuthZEN spec
		require.NotNil(t, resp.GetPolicyDecisionPoint(), "PolicyDecisionPoint is required")
		require.NotNil(t, resp.GetAccessEndpoints(), "AccessEndpoints is required")
		require.NotNil(t, resp.GetCapabilities(), "Capabilities is required")

		// Verify PDP fields are non-empty strings
		require.NotEmpty(t, resp.GetPolicyDecisionPoint().GetName(), "PDP name is required")
		require.NotEmpty(t, resp.GetPolicyDecisionPoint().GetVersion(), "PDP version is required")

		// Verify endpoint paths have valid format
		require.Regexp(t, `^/stores/\{store_id\}/access/v1/`, resp.GetAccessEndpoints().GetEvaluation())
		require.Regexp(t, `^/stores/\{store_id\}/access/v1/`, resp.GetAccessEndpoints().GetEvaluations())
		require.Regexp(t, `^/stores/\{store_id\}/access/v1/`, resp.GetAccessEndpoints().GetSubjectSearch())
		require.Regexp(t, `^/stores/\{store_id\}/access/v1/`, resp.GetAccessEndpoints().GetResourceSearch())
		require.Regexp(t, `^/stores/\{store_id\}/access/v1/`, resp.GetAccessEndpoints().GetActionSearch())
	})
}
