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

func (r *ResolveCheckRequest) clone() *ResolveCheckRequest {
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
	return r.StoreID
}

func (r *ResolveCheckRequest) GetAuthorizationModelID() string {
	return r.AuthorizationModelID
}

func (r *ResolveCheckRequest) GetTupleKey() *openfgav1.TupleKey {
	return r.TupleKey
}

func (r *ResolveCheckRequest) GetContextualTuples() []*openfgav1.TupleKey {
	return r.ContextualTuples
}

func (r *ResolveCheckRequest) GetRequestMetadata() *ResolveCheckRequestMetadata {
	return r.RequestMetadata
}

func (r *ResolveCheckRequest) GetContext() *structpb.Struct {
	return r.Context
}

func (r *ResolveCheckRequest) GetConsistency() openfgav1.ConsistencyPreference {
	return r.Consistency
}
