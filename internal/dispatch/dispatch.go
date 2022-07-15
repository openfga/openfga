package dispatch

import (
	"context"
	"errors"
	"fmt"

	dispatchv1 "github.com/openfga/openfga/pkg/proto/dispatch/v1"
)

// ErrMaxResolutionDepth is returned from CheckDepth if the maximum recursive resolution depth has been exceeded.
var ErrMaxResolutionDepth = errors.New("max resolution depth exceeded")

// Dispatcher is an interface to dispatch query subproblems of the relationship graph to one or more systems.
type Dispatcher interface {
	Check
	Expand

	// Close closes the dispatcher, cleaning up any residual resources.
	Close(ctx context.Context) error

	// IsReady reports whether the dispatcher is able to respond to requests.
	IsReady() bool
}

// Check defines the dispatch interface to dispatch check requests.
type Check interface {
	DispatchCheck(ctx context.Context, req *dispatchv1.DispatchCheckRequest) (*dispatchv1.DispatchCheckResponse, error)
}

// Expand defines the dispatch interface to dispatch expand requests.
type Expand interface {
	DispatchExpand(ctx context.Context, req *dispatchv1.DispatchExpandRequest) (*dispatchv1.DispatchExpandResponse, error)
}

// HasResolverMetadata defines and interface for structures/requests containing resolver metadata.
type HasResolverMetadata interface {
	GetMetadata() *dispatchv1.ResolverMeta
}

// CheckDepth returns ErrMaxResolutionDepth if there is no remaining resolution depth remaining to dispatch.
func CheckDepth(ctx context.Context, req HasResolverMetadata) error {

	metadata := req.GetMetadata()
	if metadata == nil {
		return fmt.Errorf("request is missing resolver metadata")
	}

	if metadata.DepthRemaining > 0 {
		return nil
	}

	return ErrMaxResolutionDepth
}
