package listusers

import (
	"maps"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

type listUsersRequest interface {
	GetStoreId() string
	GetAuthorizationModelId() string
	GetObject() *openfgav1.Object
	GetRelation() string
	GetUserFilters() []*openfgav1.ListUsersFilter
	GetContextualTuples() *openfgav1.ContextualTupleKeys
	GetDepth() uint32
}

type ListUsersRequest struct {
	*openfgav1.ListUsersRequest

	// visitedUsersetsMap keeps track of the "path" we've made so far.
	// It prevents stack overflows by preventing visiting the same userset twice.
	visitedUsersetsMap map[string]struct{}

	// Depth is the current depths of the traversal expressed as a positive, decrementing integer.
	// When expansion of list users recursively traverses one level, we decrement by one. If this
	// counter is 0, we throw ErrResolutionDepthExceeded. This protects against a potentially deep
	// or endless cycle of recursion.
	Depth uint32
}

var _ listUsersRequest = (*ListUsersRequest)(nil)

//nolint:stylecheck // it should be GetStoreID, but we want to satisfy the interface listUsersRequest
func (r *ListUsersRequest) GetStoreId() string {
	if r == nil {
		return ""
	}
	return r.StoreId
}

//nolint:stylecheck // it should be GetAuthorizationModelID, but we want to satisfy the interface listUsersRequest
func (r *ListUsersRequest) GetAuthorizationModelId() string {
	if r == nil {
		return ""
	}
	return r.AuthorizationModelId
}

func (r *ListUsersRequest) GetObject() *openfgav1.Object {
	if r == nil {
		return nil
	}
	return r.Object
}

func (r *ListUsersRequest) GetRelation() string {
	if r == nil {
		return ""
	}
	return r.Relation
}

func (r *ListUsersRequest) GetUserFilters() []*openfgav1.ListUsersFilter {
	if r == nil {
		return nil
	}
	return r.UserFilters
}

func (r *ListUsersRequest) GetContextualTuples() *openfgav1.ContextualTupleKeys {
	if r == nil {
		return nil
	}
	return r.ContextualTuples
}

func (r *ListUsersRequest) GetDepth() uint32 {
	if r == nil {
		return uint32(0)
	}
	return r.Depth
}

func fromListUsersRequest(o listUsersRequest) *ListUsersRequest {
	return &ListUsersRequest{
		ListUsersRequest: &openfgav1.ListUsersRequest{
			StoreId:              o.GetStoreId(),
			AuthorizationModelId: o.GetAuthorizationModelId(),
			Object:               o.GetObject(),
			Relation:             o.GetRelation(),
			UserFilters:          o.GetUserFilters(),
			ContextualTuples:     o.GetContextualTuples(),
		},
		visitedUsersetsMap: make(map[string]struct{}),
		Depth:              o.GetDepth(),
	}
}

// clone creates a copy of the request. Note that some fields are not deep-cloned.
func (r *ListUsersRequest) clone() *ListUsersRequest {
	v := fromListUsersRequest(r)
	v.visitedUsersetsMap = maps.Clone(r.visitedUsersetsMap)
	return v
}
