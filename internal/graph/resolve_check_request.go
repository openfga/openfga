package graph

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type ResolveCheckRequest struct {
	StoreID              string
	AuthorizationModelID string
	TupleKey             *openfgav1.TupleKey
	ContextualTuples     []*openfgav1.TupleKey
	Context              *structpb.Struct
	RequestMetadata      *ResolveCheckRequestMetadata
	VisitedPaths         map[string]struct{}
	Consistency          openfgav1.ConsistencyPreference
}

func (r *ResolveCheckRequest) clone() *ResolveCheckRequest {
	var requestMetadata *ResolveCheckRequestMetadata
	origRequestMetadata := r.GetRequestMetadata()
	if origRequestMetadata != nil {
		requestMetadata = &ResolveCheckRequestMetadata{
			DispatchCounter:     origRequestMetadata.DispatchCounter,
			Depth:               origRequestMetadata.Depth,
			DatastoreQueryCount: origRequestMetadata.DatastoreQueryCount,
			WasThrottled:        origRequestMetadata.WasThrottled,
		}
	}

	var tupleKey *openfgav1.TupleKey
	if origTupleKey := r.GetTupleKey(); origTupleKey != nil {
		tupleKey = proto.Clone(origTupleKey).(*openfgav1.TupleKey)
	}

	return &ResolveCheckRequest{
		StoreID:              r.GetStoreID(),
		AuthorizationModelID: r.GetAuthorizationModelID(),
		TupleKey:             tupleKey,
		ContextualTuples:     r.GetContextualTuples(),
		Context:              r.GetContext(),
		RequestMetadata:      requestMetadata,
		VisitedPaths:         maps.Clone(r.GetVistedPaths()),
		Consistency:          r.GetConsistency(),
	}
}

func (r *ResolveCheckRequest) GetStoreID() string {
	if r == nil {
		return ""
	}
	return r.StoreID
}

func (r *ResolveCheckRequest) GetAuthorizationModelID() string {
	if r == nil {
		return ""
	}
	return r.AuthorizationModelID
}

func (r *ResolveCheckRequest) GetTupleKey() *openfgav1.TupleKey {
	if r == nil {
		return nil
	}
	return r.TupleKey
}

func (r *ResolveCheckRequest) GetContextualTuples() []*openfgav1.TupleKey {
	if r == nil {
		return nil
	}
	return r.ContextualTuples
}

func (r *ResolveCheckRequest) GetRequestMetadata() *ResolveCheckRequestMetadata {
	if r == nil {
		return nil
	}
	return r.RequestMetadata
}

func (r *ResolveCheckRequest) GetContext() *structpb.Struct {
	if r == nil {
		return nil
	}
	return r.Context
}

func (r *ResolveCheckRequest) GetConsistency() openfgav1.ConsistencyPreference {
	if r == nil {
		return openfgav1.ConsistencyPreference_UNSPECIFIED
	}
	return r.Consistency
}

func (r *ResolveCheckRequest) GetVistedPaths() map[string]struct{} {
	if r == nil {
		return map[string]struct{}{}
	}
	return r.VisitedPaths
}
