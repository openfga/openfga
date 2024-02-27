package listusers

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

type listUsersRequest interface {
	GetStoreId() string
	GetAuthorizationModelId() string
	GetObject() *openfgav1.Object
	GetRelation() string
	GetTargetUserObjectType() string
	GetTargetUserRelation() string
	GetContextualTuples() []*openfgav1.TupleKey
}
