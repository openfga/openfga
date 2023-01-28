package dispatcher

import (
	"context"

	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type Dispatcher interface {
	CheckDispatcher
}

// CheckDispatcher defines an interface that can be implemented to resolve dispatched Check
// requests. Implementations of the interface can provide local or remote dispatching mechansims.
type CheckDispatcher interface {
	DispatchCheck(ctx context.Context, req *DispatchCheckRequest) (*DispatchCheckResponse, error)
}

type DispatchCheckRequest struct {
	StoreID              string
	AuthorizationModelID string
	TupleKey             *openfgapb.TupleKey
	ContextualTuples     []*openfgapb.TupleKey
	ResolutionMetadata   *ResolutionMetadata
}

type DispatchCheckResponse struct {
	Allowed bool
}

type ResolutionMetadata struct {
	Depth uint32
}

func (d *DispatchCheckRequest) GetStoreID() string {
	if d != nil {
		return d.StoreID
	}

	return ""
}

func (d *DispatchCheckRequest) GetAuthorizationModelID() string {
	if d != nil {
		return d.AuthorizationModelID
	}

	return ""
}

func (d *DispatchCheckRequest) GetTupleKey() *openfgapb.TupleKey {
	if d != nil {
		return d.TupleKey
	}

	return nil
}

func (d *DispatchCheckRequest) GetContextualTuples() []*openfgapb.TupleKey {
	if d != nil {
		return d.ContextualTuples
	}

	return nil
}

func (d *DispatchCheckRequest) GetResolutionMetadata() *ResolutionMetadata {
	if d != nil {
		return d.ResolutionMetadata
	}

	return nil
}
