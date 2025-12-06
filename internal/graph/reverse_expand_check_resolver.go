package graph

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

var _ CheckResolver = (*ReverseExpandCheckResolver)(nil)

// ReverseExpandQueryExecutor is an interface for executing reverse expansion queries.
// This interface exists to break the import cycle between graph and reverseexpand packages.
type ReverseExpandQueryExecutor interface {
	Execute(ctx context.Context, req ReverseExpandRequest, resultChan chan<- ReverseExpandResult, metadata ReverseExpandResolutionMetadata) error
}

// ReverseExpandRequest represents a request for reverse expansion.
// This is a simplified version to avoid import cycles.
type ReverseExpandRequest struct {
	StoreID          string
	ObjectType       string
	Relation         string
	User             ReverseExpandUserRef
	ContextualTuples []*openfgav1.TupleKey
	Context          interface{} // *structpb.Struct
	Consistency      openfgav1.ConsistencyPreference
}

// ReverseExpandUserRef represents a user reference for reverse expansion.
type ReverseExpandUserRef interface {
	GetObjectType() string
	String() string
}

const (
	RequiresFurtherEvalStatus = 0
	NoFurtherEvalStatus       = 1
)

// ReverseExpandResult represents a result from reverse expansion.
type ReverseExpandResult struct {
	Object       string
	ResultStatus int // Use RequiresFurtherEvalStatus or NoFurtherEvalStatus
}

// ReverseExpandResolutionMetadata contains metadata about reverse expansion execution.
type ReverseExpandResolutionMetadata interface {
	GetDispatchCounter() uint32
	GetDatastoreQueryCount() uint32
	GetDatastoreItemCount() uint64
	SetDispatchCounter(uint32)
	SetDatastoreQueryCount(uint32)
	SetDatastoreItemCount(uint64)
}

// ReverseExpandQueryExecutorFactory creates a ReverseExpandQueryExecutor from datastore and typesystem.
type ReverseExpandQueryExecutorFactory func(ds storage.RelationshipTupleReader, ts *typesystem.TypeSystem) ReverseExpandQueryExecutor

// ReverseExpandCheckResolver attempts to optimize check queries by using reverse expansion
// (user→objects) when it would be more efficient than forward expansion (object→users).
// This is particularly beneficial when many objects connect to a single object (many-to-one relationships).
type ReverseExpandCheckResolver struct {
	delegate                CheckResolver
	logger                  logger.Logger
	enabled                 bool
	executorFactory         ReverseExpandQueryExecutorFactory
	resolveNodeLimit        uint32
	resolveNodeBreadthLimit uint32
	timeout                 time.Duration
}

// ReverseExpandCheckResolverOpt defines an option that can be used to change the behavior
// of ReverseExpandCheckResolver instance.
type ReverseExpandCheckResolverOpt func(*ReverseExpandCheckResolver)

// WithReverseExpandCheckResolverLogger sets the logger for the reverse expand check resolver.
func WithReverseExpandCheckResolverLogger(logger logger.Logger) ReverseExpandCheckResolverOpt {
	return func(r *ReverseExpandCheckResolver) {
		r.logger = logger
	}
}

// WithReverseExpandCheckResolverExecutorFactory sets the factory for creating reverse expand query executors.
func WithReverseExpandCheckResolverExecutorFactory(factory ReverseExpandQueryExecutorFactory) ReverseExpandCheckResolverOpt {
	return func(r *ReverseExpandCheckResolver) {
		r.executorFactory = factory
	}
}

// WithReverseExpandCheckResolverResolveNodeBreadthLimit sets the resolve node breadth limit.
func WithReverseExpandCheckResolverResolveNodeBreadthLimit(limit uint32) ReverseExpandCheckResolverOpt {
	return func(r *ReverseExpandCheckResolver) {
		r.resolveNodeBreadthLimit = limit
	}
}

// WithReverseExpandCheckResolverResolveNodeLimit sets the resolve node limit for the reverse expand check resolver.
func WithReverseExpandCheckResolverResolveNodeLimit(limit uint32) ReverseExpandCheckResolverOpt {
	return func(r *ReverseExpandCheckResolver) {
		r.resolveNodeLimit = limit
	}
}

// WithReverseExpandCheckResolverTimeout sets the timeout for reverse expansion attempts.
func WithReverseExpandCheckResolverTimeout(timeout time.Duration) ReverseExpandCheckResolverOpt {
	return func(r *ReverseExpandCheckResolver) {
		r.timeout = timeout
	}
}

// NewReverseExpandCheckResolver constructs a CheckResolver that attempts to optimize check queries
// by using reverse expansion when beneficial, falling back to the delegate resolver otherwise.
func NewReverseExpandCheckResolver(
	enabled bool,
	opts ...ReverseExpandCheckResolverOpt,
) (*ReverseExpandCheckResolver, error) {
	resolver := &ReverseExpandCheckResolver{
		enabled:                 enabled,
		logger:                  logger.NewNoopLogger(),
		resolveNodeLimit:        serverconfig.DefaultResolveNodeLimit,
		resolveNodeBreadthLimit: serverconfig.DefaultResolveNodeBreadthLimit,
		timeout:                 5 * time.Second, // Default timeout for reverse expansion attempt
	}
	resolver.delegate = resolver

	for _, opt := range opts {
		opt(resolver)
	}

	return resolver, nil
}

// SetDelegate sets this ReverseExpandCheckResolver's dispatch delegate.
func (r *ReverseExpandCheckResolver) SetDelegate(delegate CheckResolver) {
	r.delegate = delegate
}

// GetDelegate returns this ReverseExpandCheckResolver's dispatch delegate.
func (r *ReverseExpandCheckResolver) GetDelegate() CheckResolver {
	return r.delegate
}

// Close releases resources allocated by the ReverseExpandCheckResolver.
func (r *ReverseExpandCheckResolver) Close() {
	// No resources to close currently
}

// ResolveCheck attempts to use reverse expansion to optimize the check query.
// If reverse expansion is not beneficial or doesn't find the object, it delegates to the normal check.
func (r *ReverseExpandCheckResolver) ResolveCheck(
	ctx context.Context,
	req *ResolveCheckRequest,
) (*ResolveCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "reverseExpandCheckResolver.ResolveCheck")
	defer span.End()

	// If disabled or factory not set, delegate immediately
	if !r.enabled || r.executorFactory == nil {
		return r.delegate.ResolveCheck(ctx, req)
	}

	// Get typesystem and datastore from context
	typesys, ok := typesystem.TypesystemFromContext(ctx)
	if !ok {
		// No typesystem in context, delegate
		return r.delegate.ResolveCheck(ctx, req)
	}

	ds, ok := storage.RelationshipTupleReaderFromContext(ctx)
	if !ok {
		// No datastore in context, delegate
		return r.delegate.ResolveCheck(ctx, req)
	}

	// Create executor from factory
	reverseExpandExecutor := r.executorFactory(ds, typesys)
	if reverseExpandExecutor == nil {
		return r.delegate.ResolveCheck(ctx, req)
	}

	tupleKey := req.GetTupleKey()
	objectType, objectID := tuple.SplitObject(tupleKey.GetObject())

	// Check if reverse expansion would be beneficial
	if !r.shouldUseReverseExpansion(ctx, req, typesys) {
		span.SetAttributes(attribute.Bool("reverse_expand_skipped", true))
		r.logger.Debug("Reverse expansion SKIPPED - heuristic determined not beneficial",
			zap.String("object", tupleKey.GetObject()),
			zap.String("relation", tupleKey.GetRelation()),
			zap.String("user", tupleKey.GetUser()))
		return r.delegate.ResolveCheck(ctx, req)
	}

	span.SetAttributes(attribute.Bool("reverse_expand_attempted", true))
	r.logger.Debug("Reverse expansion attempted",
		zap.String("object", tupleKey.GetObject()),
		zap.String("relation", tupleKey.GetRelation()),
		zap.String("user", tupleKey.GetUser()))

	// Try reverse expansion with timeout
	reverseExpandCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// Convert user to IsUserRef format
	userRef := r.convertUserToUserRef(tupleKey.GetUser())
	if userRef == nil {
		// If we can't convert the user, fall back to normal check
		return r.delegate.ResolveCheck(ctx, req)
	}

	// Create reverse expand request
	reverseExpandReq := ReverseExpandRequest{
		StoreID:          req.GetStoreID(),
		ObjectType:       objectType,
		Relation:         tupleKey.GetRelation(),
		User:             userRef,
		ContextualTuples: req.GetContextualTuples(),
		Context:          req.GetContext(),
		Consistency:      req.GetConsistency(),
	}

	// Execute reverse expansion
	resultChan := make(chan ReverseExpandResult, 10)
	metadata := &reverseExpandMetadata{}

	// Run reverse expansion in a goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- reverseExpandExecutor.Execute(reverseExpandCtx, reverseExpandReq, resultChan, metadata)
	}()

	// Check results as they come in
	for {
		select {
		case <-reverseExpandCtx.Done():
			// Timeout or cancellation - fall back to normal check
			span.SetAttributes(attribute.Bool("reverse_expand_timeout", true))
			r.logger.Warn("Reverse expansion timed out, falling back to normal check",
				zap.String("object", tupleKey.GetObject()),
				zap.String("relation", tupleKey.GetRelation()),
				zap.String("user", tupleKey.GetUser()),
				zap.Duration("timeout", r.timeout))
			return r.delegate.ResolveCheck(ctx, req)
		case err := <-errChan:
			if err != nil {
				// Error during reverse expansion - fall back to normal check
				r.logger.Debug("Reverse expansion failed, falling back to normal check",
					zap.Error(err),
					zap.String("object", tupleKey.GetObject()),
					zap.String("relation", tupleKey.GetRelation()),
					zap.String("user", tupleKey.GetUser()))
				span.SetAttributes(attribute.Bool("reverse_expand_error", true))
				return r.delegate.ResolveCheck(ctx, req)
			}
			// Reverse expansion completed successfully, channel will be closed
			// Continue to check remaining results in the channel
		case result, ok := <-resultChan:
			if !ok {
				// Channel closed, no more results
				// If we got here without finding the object, fall back to normal check
				span.SetAttributes(attribute.Bool("reverse_expand_not_found", true))
				r.logger.Debug("Reverse expansion completed but object not found, falling back to normal check",
					zap.String("object", tupleKey.GetObject()),
					zap.String("relation", tupleKey.GetRelation()),
					zap.String("user", tupleKey.GetUser()))
				return r.delegate.ResolveCheck(ctx, req)
			}

			// Check if this result matches our target object
			resultObjectType, resultObjectID := tuple.SplitObject(result.Object)
			if resultObjectType == objectType && resultObjectID == objectID {
				// Found the object! Return early
				span.SetAttributes(
					attribute.Bool("reverse_expand_found", true),
					attribute.Bool("allowed", true))
				r.logger.Info("Reverse expansion SUCCESS - object found via reverse expansion",
					zap.String("object", tupleKey.GetObject()),
					zap.String("relation", tupleKey.GetRelation()),
					zap.String("user", tupleKey.GetUser()),
					zap.Uint32("datastore_queries", metadata.GetDatastoreQueryCount()),
					zap.Uint32("dispatches", metadata.GetDispatchCounter()))
				// Note: We don't track exact query counts here since this is an optimization path.
				// The metadata will be minimal as we're short-circuiting the normal check flow.
				return &ResolveCheckResponse{
					Allowed: true,
					ResolutionMetadata: ResolveCheckResponseMetadata{
						DatastoreQueryCount: metadata.GetDatastoreQueryCount(),
					},
				}, nil
			}
		}
	}
}

// shouldUseReverseExpansion determines if reverse expansion would be beneficial for this check query.
// It analyzes the graph structure to detect many-to-one relationships that would benefit from reverse expansion.
func (r *ReverseExpandCheckResolver) shouldUseReverseExpansion(
	ctx context.Context,
	req *ResolveCheckRequest,
	typesys *typesystem.TypeSystem,
) bool {
	if typesys == nil {
		return false
	}

	tupleKey := req.GetTupleKey()
	objectType, _ := tuple.SplitObject(tupleKey.GetObject())
	relation := tupleKey.GetRelation()

	// First, check if the target relation itself uses tuple-to-userset
	// This is a strong indicator that reverse expansion would be beneficial
	rel, err := typesys.GetRelation(objectType, relation)
	if err != nil {
		r.logger.Debug("Failed to get relation for reverse expansion heuristic",
			zap.Error(err),
			zap.String("object_type", objectType),
			zap.String("relation", relation))
		return false
	}

	rewrite := rel.GetRewrite()
	if rewrite != nil {
		if _, ok := rewrite.GetUserset().(*openfgav1.Userset_TupleToUserset); ok {
			// The target relation uses tuple-to-userset, which suggests many-to-one relationships
			// (e.g., many folders → one instance via folder relation)
			r.logger.Debug("Target relation uses tuple-to-userset, reverse expansion likely beneficial",
				zap.String("object_type", objectType),
				zap.String("relation", relation))
			return true
		}
	}

	// Also check for tuple-to-userset edges in the path from user to target
	// This catches cases where the path involves tuple-to-userset even if the target doesn't
	userType := tuple.GetType(tupleKey.GetUser())
	graph := New(typesys)

	targetRef := typesystem.DirectRelationReference(objectType, relation)
	sourceRef := typesystem.DirectRelationReference(userType, "")

	edges, err := graph.GetRelationshipEdges(targetRef, sourceRef)
	if err != nil {
		r.logger.Debug("Failed to get relationship edges for reverse expansion heuristic",
			zap.Error(err))
		return false
	}

	// Check for tuple-to-userset edges that suggest high fan-out
	for _, edge := range edges {
		if edge.Type == TupleToUsersetEdge {
			// Tuple-to-userset edges often indicate many-to-one relationships
			r.logger.Debug("Found tuple-to-userset edge in path, reverse expansion likely beneficial",
				zap.String("edge", edge.String()))
			return true
		}
	}

	// If no clear indicator, be conservative and use normal check
	return false
}

// reverseExpandUserRef implementations
type reverseExpandUserRefObject struct {
	objectType string
	objectID   string
}

func (r *reverseExpandUserRefObject) GetObjectType() string { return r.objectType }
func (r *reverseExpandUserRefObject) String() string {
	return tuple.BuildObject(r.objectType, r.objectID)
}

type reverseExpandUserRefTypedWildcard struct {
	objectType string
}

func (r *reverseExpandUserRefTypedWildcard) GetObjectType() string { return r.objectType }
func (r *reverseExpandUserRefTypedWildcard) String() string {
	return tuple.TypedPublicWildcard(r.objectType)
}

type reverseExpandUserRefObjectRelation struct {
	object   string
	relation string
}

func (r *reverseExpandUserRefObjectRelation) GetObjectType() string { return tuple.GetType(r.object) }
func (r *reverseExpandUserRefObjectRelation) String() string {
	return tuple.ToObjectRelationString(r.object, r.relation)
}

// convertUserToUserRef converts a user string to a ReverseExpandUserRef.
func (r *ReverseExpandCheckResolver) convertUserToUserRef(user string) ReverseExpandUserRef {
	if tuple.IsTypedWildcard(user) {
		return &reverseExpandUserRefTypedWildcard{
			objectType: tuple.GetType(user),
		}
	}

	if tuple.IsObjectRelation(user) {
		// User is a userset (e.g., "group:eng#member")
		obj, rel := tuple.SplitObjectRelation(user)
		return &reverseExpandUserRefObjectRelation{
			object:   obj,
			relation: rel,
		}
	}

	// User is a direct object (e.g., "user:alice")
	objType, objID := tuple.SplitObject(user)
	return &reverseExpandUserRefObject{
		objectType: objType,
		objectID:   objID,
	}
}

// reverseExpandMetadata implements ReverseExpandResolutionMetadata
type reverseExpandMetadata struct {
	dispatchCounter     atomic.Uint32
	datastoreQueryCount atomic.Uint32
	datastoreItemCount  atomic.Uint64
}

func (m *reverseExpandMetadata) GetDispatchCounter() uint32      { return m.dispatchCounter.Load() }
func (m *reverseExpandMetadata) GetDatastoreQueryCount() uint32  { return m.datastoreQueryCount.Load() }
func (m *reverseExpandMetadata) GetDatastoreItemCount() uint64   { return m.datastoreItemCount.Load() }
func (m *reverseExpandMetadata) SetDispatchCounter(v uint32)     { m.dispatchCounter.Store(v) }
func (m *reverseExpandMetadata) SetDatastoreQueryCount(v uint32) { m.datastoreQueryCount.Store(v) }
func (m *reverseExpandMetadata) SetDatastoreItemCount(v uint64)  { m.datastoreItemCount.Store(v) }
