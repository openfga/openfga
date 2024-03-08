package listusers

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

type listUsersRequest interface {
	GetStoreId() string
	GetAuthorizationModelId() string
	GetObject() *openfgav1.Object
	GetRelation() string
	GetUserFilters() []*openfgav1.ListUsersFilter
	GetContextualTuples() *openfgav1.ContextualTupleKeys
}
