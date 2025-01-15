package graph

import (
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
)

type ResolveCheckRequest struct {
	storeID                   string
	authorizationModelID      string // TODO replace with typesystem
	TupleKey                  *openfgav1.TupleKey
	contextualTuples          []*openfgav1.TupleKey
	context                   *structpb.Struct
	RequestMetadata           *ResolveCheckRequestMetadata
	VisitedPaths              map[string]struct{}
	consistency               openfgav1.ConsistencyPreference
	lastCacheInvalidationTime time.Time
	InvariantCacheKey         string
}

type ResolveCheckRequestMetadata struct {
	// Thinking of a Check as a tree of evaluations,
	// Depth is the current level in the tree in the current path that we are exploring.
	// When we jump one level, we increment it by 1. If it hits maxResolutionDepth (resolveNodeLimit), we throw ErrResolutionDepthExceeded.
	Depth uint32

	// DispatchCounter is the address to a shared counter that keeps track of how many calls to ResolveCheck we had to do
	// to solve the root/parent problem.
	// The contents of this counter will be written by concurrent goroutines.
	// After the root problem has been solved, this value can be read.
	DispatchCounter *atomic.Uint32

	// WasThrottled indicates whether the request was throttled
	WasThrottled *atomic.Bool
}

type ResolveCheckRequestParams struct {
	StoreID               string
	TupleKey              *openfgav1.TupleKey
	ContextualTuples      *openfgav1.ContextualTupleKeys
	Context               *structpb.Struct
	Consistency           openfgav1.ConsistencyPreference
	CacheInvalidationTime time.Time
	AuthorizationModelID  string
}

func NewCheckRequestMetadata() *ResolveCheckRequestMetadata {
	return &ResolveCheckRequestMetadata{
		DispatchCounter: new(atomic.Uint32),
		WasThrottled:    new(atomic.Bool),
	}
}

func NewResolveCheckRequest(
	params ResolveCheckRequestParams,
) (*ResolveCheckRequest, error) {
	r := &ResolveCheckRequest{
		storeID:              params.StoreID,
		authorizationModelID: params.AuthorizationModelID,
		TupleKey:             params.TupleKey,
		contextualTuples:     params.ContextualTuples.GetTupleKeys(),
		context:              params.Context,
		VisitedPaths:         make(map[string]struct{}),
		RequestMetadata:      NewCheckRequestMetadata(),
		consistency:          params.Consistency,
		// avoid having to read from cache consistently by propagating it
		lastCacheInvalidationTime: params.CacheInvalidationTime,
	}

	err := r.SetRequestInvariantCacheKey()
	if err != nil {
		return nil, err
	}

	if r.GetAuthorizationModelID() == "" {
		return nil, errors.New("missing authorization_model_id")
	}

	if r.GetStoreID() == "" {
		return nil, errors.New("missing store_id")
	}

	return r, nil
}

func (r *ResolveCheckRequest) clone() *ResolveCheckRequest {
	var requestMetadata *ResolveCheckRequestMetadata
	origRequestMetadata := r.GetRequestMetadata()
	if origRequestMetadata != nil {
		requestMetadata = &ResolveCheckRequestMetadata{
			DispatchCounter: origRequestMetadata.DispatchCounter,
			Depth:           origRequestMetadata.Depth,
			WasThrottled:    origRequestMetadata.WasThrottled,
		}
	}

	var tupleKey *openfgav1.TupleKey
	if origTupleKey := r.GetTupleKey(); origTupleKey != nil {
		tupleKey = proto.Clone(origTupleKey).(*openfgav1.TupleKey)
	}

	return &ResolveCheckRequest{
		storeID:                   r.GetStoreID(),
		authorizationModelID:      r.GetAuthorizationModelID(),
		TupleKey:                  tupleKey,
		contextualTuples:          r.GetContextualTuples(),
		context:                   r.GetContext(),
		RequestMetadata:           requestMetadata,
		VisitedPaths:              maps.Clone(r.GetVisitedPaths()),
		consistency:               r.GetConsistency(),
		lastCacheInvalidationTime: r.GetLastCacheInvalidationTime(),
		InvariantCacheKey:         r.GetInvariantCacheKey(),
	}
}

func (r *ResolveCheckRequest) GetStoreID() string {
	if r == nil {
		return ""
	}
	return r.storeID
}

func (r *ResolveCheckRequest) GetAuthorizationModelID() string {
	if r == nil {
		return ""
	}
	return r.authorizationModelID
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
	return r.contextualTuples
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
	return r.context
}

func (r *ResolveCheckRequest) GetConsistency() openfgav1.ConsistencyPreference {
	if r == nil {
		return openfgav1.ConsistencyPreference_UNSPECIFIED
	}
	return r.consistency
}

func (r *ResolveCheckRequest) GetVisitedPaths() map[string]struct{} {
	if r == nil {
		return map[string]struct{}{}
	}
	return r.VisitedPaths
}

func (r *ResolveCheckRequest) GetLastCacheInvalidationTime() time.Time {
	if r == nil {
		return time.Time{}
	}
	return r.lastCacheInvalidationTime
}
func (r *ResolveCheckRequest) GetInvariantCacheKey() string {
	if r == nil {
		return ""
	}
	return r.InvariantCacheKey
}

// SetRequestInvariantCacheKey calculates a cache key for the "Invariant" parts of a check request and attaches it to the request.
// Invariant parts: StoreID, AuthorizationModelID, Context and ContextualTuples, as they do not change within a single request.
// The InvariantCacheKey is calculated once per request and passed along to
// sub-problems via ResolveCheckRequest.clone() if necessary.
func (r *ResolveCheckRequest) SetRequestInvariantCacheKey() error {
	params := &storage.CheckCacheKeyParams{
		StoreID:              r.GetStoreID(),
		AuthorizationModelID: r.GetAuthorizationModelID(),
		ContextualTuples:     r.GetContextualTuples(),
		Context:              r.GetContext(),
	}

	writer := &strings.Builder{}
	err := storage.WriteInvariantCheckCacheKey(writer, params)
	if err != nil {
		return err
	}

	r.InvariantCacheKey = writer.String()
	return nil
}
