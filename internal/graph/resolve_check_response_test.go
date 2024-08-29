package graph

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCloneResolveCheckResponse(t *testing.T) {
	t.Run("clone_and_modify_orig", func(t *testing.T) {
		resp1 := &ResolveCheckResponse{
			Allowed: true,
			ResolutionMetadata: &ResolveCheckResponseMetadata{
				DatastoreQueryCount: 1,
				CycleDetected:       false,
			},
		}
		clonedResp1 := resp1.clone()

		require.Equal(t, resp1, clonedResp1)
		require.NotSame(t, resp1, clonedResp1)

		// mutate the clone and ensure the original reference is
		// unchanged
		clonedResp1.Allowed = false
		clonedResp1.ResolutionMetadata.DatastoreQueryCount = 2
		clonedResp1.ResolutionMetadata.CycleDetected = true
		require.True(t, resp1.GetAllowed())
		require.Equal(t, uint32(1), resp1.GetResolutionMetadata().DatastoreQueryCount)
		require.False(t, resp1.GetResolutionMetadata().CycleDetected)
	})

	t.Run("clone_empty_ResolutionMetadata", func(t *testing.T) {
		resp2 := &ResolveCheckResponse{
			Allowed: true,
		}
		clonedResp2 := resp2.clone()

		require.NotSame(t, resp2, clonedResp2)
		require.Equal(t, resp2.GetAllowed(), clonedResp2.GetAllowed())
		require.NotNil(t, clonedResp2.ResolutionMetadata)
		require.Equal(t, uint32(0), clonedResp2.GetResolutionMetadata().DatastoreQueryCount)
		require.False(t, clonedResp2.GetResolutionMetadata().CycleDetected)
	})

	t.Run("clone_nil_ResolutionMetadata", func(t *testing.T) {
		var resp2 *ResolveCheckResponse
		clonedResp2 := resp2.clone()

		require.NotSame(t, resp2, clonedResp2)
		require.Equal(t, resp2.GetAllowed(), clonedResp2.GetAllowed())
		require.NotNil(t, clonedResp2.ResolutionMetadata)
		require.Equal(t, uint32(0), clonedResp2.GetResolutionMetadata().DatastoreQueryCount)
		require.False(t, clonedResp2.GetResolutionMetadata().CycleDetected)
	})
}

func TestResolveCheckResponseDefaultValue(t *testing.T) {
	var r *ResolveCheckResponse
	require.False(t, r.GetCycleDetected())
	require.False(t, r.GetAllowed())
	require.Nil(t, r.GetResolutionMetadata())
}
