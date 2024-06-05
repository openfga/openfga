//go:generate mockgen -source dispatch.go -destination ../../internal/mocks/mock_dispatch.go -package mocks Dispatcher

package dispatch

import (
	"context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"

	dispatchv1 "github.com/openfga/openfga/internal/proto/dispatch/v1alpha1"
)

const (
	DefaultCheckResolutionDepth = 25
)

// CheckDispatcher defines an interface for which FGA Check queries can be dispatched.
//
// Different implementations of the CheckDispatcher interface provide the ability to orchestrate
// different compositions of Check dispatching semantics. As just a few examples, dispatches can
// be throttled, cached, or de-duplicated. Dispatchers can be layered on to one another to form a
// chain of CheckDispatcher's which establish a layered Check resolution pattern.
//
// For examples of CheckDispatcher implementations, see the `cached`, `throttled`, and `singleflight`
// packages within this parent package.
//
// Note: This interface definition purposefully matches the DispatchCheck RPC definition defined in
// 'proto/dispatch/v1alpha1' so that the CheckDispatcher can be implemented with local composition
// and/or remote (peer dispatching) composition. This signature should be deviate from the RPC generated
// stubs.
type CheckDispatcher interface {
	DispatchCheck(
		ctx context.Context,
		req *dispatchv1.DispatchCheckRequest,
	) (*dispatchv1.DispatchCheckResponse, error)
}

func NewDispatchCheckRequestMetadata(
	resolutionDepth uint32,
) *dispatchv1.DispatchCheckRequestMetadata {
	return &dispatchv1.DispatchCheckRequestMetadata{
		Depth:                    resolutionDepth,
		DatastoreQueryCount:      0,
		DispatchCount:            0,
		WasThrottled:             false,
		VisitedParentSubproblems: map[string]*emptypb.Empty{},
	}
}
