package graph

import (
	"context"

	dispatchpb "github.com/openfga/openfga/pkg/proto/dispatch/v1"
)

var emptyMetadata = &dispatchpb.ResponseMeta{}

// ReduceableCheckFunc is a function that can be bound to a concurrent check execution context and
// yields a check result on the provided channel.
type ReduceableCheckFunc func(ctx context.Context, resultChan chan<- *dispatchpb.DispatchCheckResponse) error

// CheckReducer is a type for the functions 'any', `all`, and `difference` which combine check results.
type CheckReducer func(ctx context.Context, requests []ReduceableCheckFunc) (*dispatchpb.DispatchCheckResponse, error)

func addCallToResponseMetadata(metadata *dispatchpb.ResponseMeta) *dispatchpb.ResponseMeta {
	// + 1 for the current call.
	return &dispatchpb.ResponseMeta{
		DispatchCount:       metadata.DispatchCount + 1,
		DepthRequired:       metadata.DepthRequired + 1,
		CachedDispatchCount: metadata.CachedDispatchCount,
	}
}

func decrementDepth(metadata *dispatchpb.ResolverMeta) *dispatchpb.ResolverMeta {
	return &dispatchpb.ResolverMeta{
		DepthRemaining: metadata.DepthRemaining - 1,
	}
}

func combineResponseMetadata(existing *dispatchpb.ResponseMeta, responseMetadata *dispatchpb.ResponseMeta) *dispatchpb.ResponseMeta {

	// take the maximum depth of the two
	depthRequired := existing.DepthRequired
	if responseMetadata.DepthRequired > existing.DepthRequired {
		depthRequired = responseMetadata.DepthRequired
	}

	return &dispatchpb.ResponseMeta{
		DispatchCount:       existing.DispatchCount + responseMetadata.DispatchCount,
		DepthRequired:       depthRequired,
		CachedDispatchCount: existing.CachedDispatchCount + responseMetadata.CachedDispatchCount,
	}
}
