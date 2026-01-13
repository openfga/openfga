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
		require.NotNil(t, resp.PolicyDecisionPoint)
		require.Equal(t, "OpenFGA", resp.PolicyDecisionPoint.Name)
		require.Equal(t, build.Version, resp.PolicyDecisionPoint.Version)
		require.NotEmpty(t, resp.PolicyDecisionPoint.Description)
		require.Contains(t, resp.PolicyDecisionPoint.Description, "OpenFGA")
		require.Contains(t, resp.PolicyDecisionPoint.Description, "AuthZEN")
	})

	t.Run("returns_correct_endpoints", func(t *testing.T) {
		s := MustNewServerWithOpts(WithDatastore(ds))
		t.Cleanup(s.Close)

		resp, err := s.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{})
		require.NoError(t, err)

		require.NotNil(t, resp.AccessEndpoints)
		require.Equal(t, "/stores/{store_id}/access/v1/evaluation", resp.AccessEndpoints.Evaluation)
		require.Equal(t, "/stores/{store_id}/access/v1/evaluations", resp.AccessEndpoints.Evaluations)
		require.Equal(t, "/stores/{store_id}/access/v1/search/subject", resp.AccessEndpoints.SubjectSearch)
		require.Equal(t, "/stores/{store_id}/access/v1/search/resource", resp.AccessEndpoints.ResourceSearch)
		require.Equal(t, "/stores/{store_id}/access/v1/search/action", resp.AccessEndpoints.ActionSearch)
	})

	t.Run("returns_capabilities", func(t *testing.T) {
		s := MustNewServerWithOpts(WithDatastore(ds))
		t.Cleanup(s.Close)

		resp, err := s.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{})
		require.NoError(t, err)

		require.NotEmpty(t, resp.Capabilities)
		require.Len(t, resp.Capabilities, 5)
		require.Contains(t, resp.Capabilities, "evaluation")
		require.Contains(t, resp.Capabilities, "evaluations")
		require.Contains(t, resp.Capabilities, "subject_search")
		require.Contains(t, resp.Capabilities, "resource_search")
		require.Contains(t, resp.Capabilities, "action_search")
	})

	t.Run("response_format_compliance", func(t *testing.T) {
		s := MustNewServerWithOpts(WithDatastore(ds))
		t.Cleanup(s.Close)

		resp, err := s.GetConfiguration(context.Background(), &authzenv1.GetConfigurationRequest{})
		require.NoError(t, err)

		// Verify all required fields are present per AuthZEN spec
		require.NotNil(t, resp.PolicyDecisionPoint, "PolicyDecisionPoint is required")
		require.NotNil(t, resp.AccessEndpoints, "AccessEndpoints is required")
		require.NotNil(t, resp.Capabilities, "Capabilities is required")

		// Verify PDP fields are non-empty strings
		require.NotEmpty(t, resp.PolicyDecisionPoint.Name, "PDP name is required")
		require.NotEmpty(t, resp.PolicyDecisionPoint.Version, "PDP version is required")

		// Verify endpoint paths have valid format
		require.Regexp(t, `^/stores/\{store_id\}/access/v1/`, resp.AccessEndpoints.Evaluation)
		require.Regexp(t, `^/stores/\{store_id\}/access/v1/`, resp.AccessEndpoints.Evaluations)
		require.Regexp(t, `^/stores/\{store_id\}/access/v1/`, resp.AccessEndpoints.SubjectSearch)
		require.Regexp(t, `^/stores/\{store_id\}/access/v1/`, resp.AccessEndpoints.ResourceSearch)
		require.Regexp(t, `^/stores/\{store_id\}/access/v1/`, resp.AccessEndpoints.ActionSearch)
	})
}
