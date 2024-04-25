package listusers

import (
	"maps"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

type listUsersRequest interface {
	GetStoreId() string
	GetAuthorizationModelId() string
	GetObject() *openfgav1.Object
	GetRelation() string
	GetUserFilters() []*openfgav1.UserTypeFilter
	GetContextualTuples() []*openfgav1.TupleKey
	GetContext() *structpb.Struct
}

type internalListUsersRequest struct {
	*openfgav1.ListUsersRequest

	// visitedUsersetsMap keeps track of the "path" we've made so far.
	// It prevents stack overflows by preventing visiting the same userset twice.
	visitedUsersetsMap map[string]struct{}

	// depth is the current depths of the traversal expressed as a positive, incrementing integer.
	// When expansion of list users recursively traverses one level, we increment by one. If this
	// counter hits the limit, we throw ErrResolutionDepthExceeded. This protects against a potentially deep
	// or endless cycle of recursion.
	depth uint32
}

var _ listUsersRequest = (*internalListUsersRequest)(nil)

// nolint // it should be GetStoreID, but we want to satisfy the interface listUsersRequest
func (r *internalListUsersRequest) GetStoreId() string {
	if r == nil {
		return ""
	}
	return r.StoreId
}

// nolint // it should be GetAuthorizationModelID, but we want to satisfy the interface listUsersRequest
func (r *internalListUsersRequest) GetAuthorizationModelId() string {
	if r == nil {
		return ""
	}
	return r.AuthorizationModelId
}

func (r *internalListUsersRequest) GetObject() *openfgav1.Object {
	if r == nil {
		return nil
	}
	return r.Object
}

func (r *internalListUsersRequest) GetRelation() string {
	if r == nil {
		return ""
	}
	return r.Relation
}

func (r *internalListUsersRequest) GetUserFilters() []*openfgav1.UserTypeFilter {
	if r == nil {
		return nil
	}
	return r.UserFilters
}

func (r *internalListUsersRequest) GetContextualTuples() []*openfgav1.TupleKey {
	if r == nil {
		return nil
	}
	return r.ContextualTuples
}

func (r *internalListUsersRequest) GetContext() *structpb.Struct {
	if r == nil {
		return nil
	}
	return r.Context
}

func fromListUsersRequest(o listUsersRequest) *internalListUsersRequest {
	return &internalListUsersRequest{
		ListUsersRequest: &openfgav1.ListUsersRequest{
			StoreId:              o.GetStoreId(),
			AuthorizationModelId: o.GetAuthorizationModelId(),
			Object:               o.GetObject(),
			Relation:             o.GetRelation(),
			UserFilters:          o.GetUserFilters(),
			ContextualTuples:     o.GetContextualTuples(),
			Context:              o.GetContext(),
		},
		visitedUsersetsMap: make(map[string]struct{}),
		depth:              0,
	}
}

// clone creates a copy of the request. Note that some fields are not deep-cloned.
func (r *internalListUsersRequest) clone() *internalListUsersRequest {
	v := fromListUsersRequest(r)
	v.visitedUsersetsMap = maps.Clone(r.visitedUsersetsMap)
	v.depth = r.depth
	return v
}
