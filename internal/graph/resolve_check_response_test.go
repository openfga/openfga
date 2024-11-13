package graph

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCloneResolveCheckResponse(t *testing.T) {
	t.Run("clone_and_modify_orig", func(t *testing.T) {
		resp1 := &ResolveCheckResponse{
			Allowed: true,
			// default values for ResolveCheckResponseMetadata
		}
		clonedResp1 := resp1.clone()

		require.Equal(t, resp1, clonedResp1)
		require.NotSame(t, resp1, clonedResp1)

		// mutate the clone and ensure the original reference is
		// unchanged
		clonedResp1.Allowed = false
		clonedResp1.ResolutionMetadata.CycleDetected = true
		clonedResp1.ResolutionMetadata.DatastoreQueryCount = 1
		require.True(t, resp1.GetAllowed())
		require.False(t, resp1.GetResolutionMetadata().CycleDetected)
		require.Zero(t, resp1.GetResolutionMetadata().DatastoreQueryCount)
	})
}

func TestResolveCheckResponseDefaultValue(t *testing.T) {
	var r ResolveCheckResponse
	require.False(t, r.GetCycleDetected())
	require.False(t, r.GetAllowed())
	require.NotNil(t, r.GetResolutionMetadata())
	require.Zero(t, r.GetResolutionMetadata().DatastoreQueryCount)
	require.False(t, r.GetResolutionMetadata().CycleDetected)
}
