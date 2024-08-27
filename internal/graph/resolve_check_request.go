package graph

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"golang.org/x/exp/maps"
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

func clone(r *ResolveCheckRequest) *ResolveCheckRequest {
	return &ResolveCheckRequest{
		StoreID:              r.StoreID,
		AuthorizationModelID: r.AuthorizationModelID,
		TupleKey:             r.TupleKey,
		ContextualTuples:     r.ContextualTuples,
		Context:              r.Context,
		RequestMetadata: &ResolveCheckRequestMetadata{
			DispatchCounter:     r.GetRequestMetadata().DispatchCounter,
			Depth:               r.GetRequestMetadata().Depth,
			DatastoreQueryCount: r.GetRequestMetadata().DatastoreQueryCount,
			WasThrottled:        r.GetRequestMetadata().WasThrottled,
		},
		VisitedPaths: maps.Clone(r.VisitedPaths),
		Consistency:  r.Consistency,
	}
}

func (r *ResolveCheckRequest) GetStoreID() string {
	if r != nil {
		return r.StoreID
	}

	return ""
}

func (r *ResolveCheckRequest) GetAuthorizationModelID() string {
	if r != nil {
		return r.AuthorizationModelID
	}

	return ""
}

func (r *ResolveCheckRequest) GetTupleKey() *openfgav1.TupleKey {
	if r != nil {
		return r.TupleKey
	}

	return nil
}

func (r *ResolveCheckRequest) GetContextualTuples() []*openfgav1.TupleKey {
	if r != nil {
		return r.ContextualTuples
	}

	return nil
}

func (r *ResolveCheckRequest) GetRequestMetadata() *ResolveCheckRequestMetadata {
	if r != nil {
		return r.RequestMetadata
	}

	return nil
}

func (r *ResolveCheckRequest) GetContext() *structpb.Struct {
	if r != nil {
		return r.Context
	}
	return nil
}

func (r *ResolveCheckRequest) GetConsistency() openfgav1.ConsistencyPreference {
	if r != nil {
		return r.Consistency
	}
	return openfgav1.ConsistencyPreference_UNSPECIFIED
}
