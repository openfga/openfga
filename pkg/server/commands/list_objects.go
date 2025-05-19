package commands

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/internal/cachecontroller"
	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/condition"
	openfgaErrors "github.com/openfga/openfga/internal/errors"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/shared"
	"github.com/openfga/openfga/internal/throttler"
	"github.com/openfga/openfga/internal/throttler/threshold"
	"github.com/openfga/openfga/internal/utils/apimethod"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands/reverseexpand"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

const streamedBufferSize = 100

var (
	furtherEvalRequiredCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "list_objects_further_eval_required_count",
		Help:      "Number of objects in a ListObjects call that needed to issue a Check call to determine a final result",
	})

	noFurtherEvalRequiredCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "list_objects_no_further_eval_required_count",
		Help:      "Number of objects in a ListObjects call that needed to issue a Check call to determine a final result",
	})
)

type ListObjectsQuery struct {
	datastore               storage.RelationshipTupleReader
	logger                  logger.Logger
	listObjectsDeadline     time.Duration
	listObjectsMaxResults   uint32
	resolveNodeLimit        uint32
	resolveNodeBreadthLimit uint32
	maxConcurrentReads      uint32

	dispatchThrottlerConfig threshold.Config

	datastoreThrottleThreshold int
	datastoreThrottleDuration  time.Duration

	checkResolver            graph.CheckResolver
	cacheSettings            serverconfig.CacheSettings
	sharedDatastoreResources *shared.SharedDatastoreResources
}

type ListObjectsResolutionMetadata struct {
	// The total number of database reads from reverse_expand and Check (if any) to complete the ListObjects request
	DatastoreQueryCount *atomic.Uint32

	// The total number of dispatches aggregated from reverse_expand and check resolutions (if any) to complete the ListObjects request
	DispatchCounter *atomic.Uint32

	// WasThrottled indicates whether the request was throttled
	WasThrottled *atomic.Bool
}

func NewListObjectsResolutionMetadata() *ListObjectsResolutionMetadata {
	return &ListObjectsResolutionMetadata{
		DatastoreQueryCount: new(atomic.Uint32),
		DispatchCounter:     new(atomic.Uint32),
		WasThrottled:        new(atomic.Bool),
	}
}

type ListObjectsResponse struct {
	Objects            []string
	ResolutionMetadata ListObjectsResolutionMetadata
}

type ListObjectsQueryOption func(d *ListObjectsQuery)

func WithListObjectsDeadline(deadline time.Duration) ListObjectsQueryOption {
	return func(d *ListObjectsQuery) {
		d.listObjectsDeadline = deadline
	}
}

func WithDispatchThrottlerConfig(config threshold.Config) ListObjectsQueryOption {
	return func(d *ListObjectsQuery) {
		d.dispatchThrottlerConfig = config
	}
}

func WithListObjectsMaxResults(maxResults uint32) ListObjectsQueryOption {
	return func(d *ListObjectsQuery) {
		d.listObjectsMaxResults = maxResults
	}
}

// WithResolveNodeLimit see server.WithResolveNodeLimit.
func WithResolveNodeLimit(limit uint32) ListObjectsQueryOption {
	return func(d *ListObjectsQuery) {
		d.resolveNodeLimit = limit
	}
}

// WithResolveNodeBreadthLimit see server.WithResolveNodeBreadthLimit.
func WithResolveNodeBreadthLimit(limit uint32) ListObjectsQueryOption {
	return func(d *ListObjectsQuery) {
		d.resolveNodeBreadthLimit = limit
	}
}

func WithLogger(l logger.Logger) ListObjectsQueryOption {
	return func(d *ListObjectsQuery) {
		d.logger = l
	}
}

// WithMaxConcurrentReads see server.WithMaxConcurrentReadsForListObjects.
func WithMaxConcurrentReads(limit uint32) ListObjectsQueryOption {
	return func(d *ListObjectsQuery) {
		d.maxConcurrentReads = limit
	}
}

func WithListObjectsCache(sharedDatastoreResources *shared.SharedDatastoreResources, cacheSettings serverconfig.CacheSettings) ListObjectsQueryOption {
	return func(d *ListObjectsQuery) {
		d.cacheSettings = cacheSettings
		d.sharedDatastoreResources = sharedDatastoreResources
	}
}

func WithListObjectsDatastoreThrottler(threshold int, duration time.Duration) ListObjectsQueryOption {
	return func(d *ListObjectsQuery) {
		d.datastoreThrottleThreshold = threshold
		d.datastoreThrottleDuration = duration
	}
}

func NewListObjectsQuery(
	ds storage.RelationshipTupleReader,
	checkResolver graph.CheckResolver,
	opts ...ListObjectsQueryOption,
) (*ListObjectsQuery, error) {
	if ds == nil {
		return nil, fmt.Errorf("the provided datastore parameter 'ds' must be non-nil")
	}

	if checkResolver == nil {
		return nil, fmt.Errorf("the provided CheckResolver parameter 'checkResolver' must be non-nil")
	}

	query := &ListObjectsQuery{
		datastore:               ds,
		logger:                  logger.NewNoopLogger(),
		listObjectsDeadline:     serverconfig.DefaultListObjectsDeadline,
		listObjectsMaxResults:   serverconfig.DefaultListObjectsMaxResults,
		resolveNodeLimit:        serverconfig.DefaultResolveNodeLimit,
		resolveNodeBreadthLimit: serverconfig.DefaultResolveNodeBreadthLimit,
		maxConcurrentReads:      serverconfig.DefaultMaxConcurrentReadsForListObjects,
		dispatchThrottlerConfig: threshold.Config{
			Throttler:    throttler.NewNoopThrottler(),
			Enabled:      serverconfig.DefaultListObjectsDispatchThrottlingEnabled,
			Threshold:    serverconfig.DefaultListObjectsDispatchThrottlingDefaultThreshold,
			MaxThreshold: serverconfig.DefaultListObjectsDispatchThrottlingMaxThreshold,
		},
		checkResolver: checkResolver,
		cacheSettings: serverconfig.NewDefaultCacheSettings(),
		sharedDatastoreResources: &shared.SharedDatastoreResources{
			CacheController: cachecontroller.NewNoopCacheController(),
		},
	}

	for _, opt := range opts {
		opt(query)
	}

	return query, nil
}

type ListObjectsResult struct {
	ObjectID string
	Err      error
}

// listObjectsRequest captures the RPC request definition interface for the ListObjects API.
// The unary and streaming RPC definitions implement this interface, and so it can be used
// interchangeably for a canonical representation between the two.
type listObjectsRequest interface {
	GetStoreId() string
	GetAuthorizationModelId() string
	GetType() string
	GetRelation() string
	GetUser() string
	GetContextualTuples() *openfgav1.ContextualTupleKeys
	GetContext() *structpb.Struct
	GetConsistency() openfgav1.ConsistencyPreference
}

// evaluate fires of evaluation of the ListObjects query by delegating to
// [[reverseexpand.ReverseExpand#Execute]] and resolving the results yielded
// from it. If any results yielded by reverse expansion require further eval,
// then these results get dispatched to Check to resolve the residual outcome.
//
// The resultsChan is **always** closed by evaluate when it is done with its work,
// which is either when all results have been yielded, the deadline has been met,
// or some other terminal error case has occurred.
func (q *ListObjectsQuery) evaluate(
	ctx context.Context,
	req listObjectsRequest,
	resultsChan chan<- ListObjectsResult,
	maxResults uint32,
	resolutionMetadata *ListObjectsResolutionMetadata,
) error {
	targetObjectType := req.GetType()
	targetRelation := req.GetRelation()

	typesys, ok := typesystem.TypesystemFromContext(ctx)
	if !ok {
		return fmt.Errorf("%w: typesystem missing in context", openfgaErrors.ErrUnknown)
	}

	if !typesystem.IsSchemaVersionSupported(typesys.GetSchemaVersion()) {
		return serverErrors.ValidationError(typesystem.ErrInvalidSchemaVersion)
	}

	for _, ctxTuple := range req.GetContextualTuples().GetTupleKeys() {
		if err := validation.ValidateTupleForWrite(typesys, ctxTuple); err != nil {
			return serverErrors.HandleTupleValidateError(err)
		}
	}

	_, err := typesys.GetRelation(targetObjectType, targetRelation)
	if err != nil {
		if errors.Is(err, typesystem.ErrObjectTypeUndefined) {
			return serverErrors.TypeNotFound(targetObjectType)
		}

		if errors.Is(err, typesystem.ErrRelationUndefined) {
			return serverErrors.RelationNotFound(targetRelation, targetObjectType, nil)
		}

		return serverErrors.HandleError("", err)
	}

	if err := validation.ValidateUser(typesys, req.GetUser()); err != nil {
		return serverErrors.ValidationError(fmt.Errorf("invalid 'user' value: %s", err))
	}

	handler := func() {
		userObj, userRel := tuple.SplitObjectRelation(req.GetUser())
		userObjType, userObjID := tuple.SplitObject(userObj)

		var sourceUserRef reverseexpand.IsUserRef
		sourceUserRef = &reverseexpand.UserRefObject{
			Object: &openfgav1.Object{
				Type: userObjType,
				Id:   userObjID,
			},
		}

		if tuple.IsTypedWildcard(userObj) {
			sourceUserRef = &reverseexpand.UserRefTypedWildcard{Type: tuple.GetType(userObj)}
		}

		if userRel != "" {
			sourceUserRef = &reverseexpand.UserRefObjectRelation{
				ObjectRelation: &openfgav1.ObjectRelation{
					Object:   userObj,
					Relation: userRel,
				},
			}
		}

		reverseExpandResultsChan := make(chan *reverseexpand.ReverseExpandResult, 1)
		objectsFound := atomic.Uint32{}

		ds := storagewrappers.NewRequestStorageWrapperWithCache(
			q.datastore,
			req.GetContextualTuples().GetTupleKeys(),
			&storagewrappers.Operation{
				Method:            apimethod.ListObjects,
				Concurrency:       q.maxConcurrentReads,
				ThrottleThreshold: q.datastoreThrottleThreshold,
				ThrottleDuration:  q.datastoreThrottleDuration,
			},
			q.sharedDatastoreResources,
			q.cacheSettings,
		)

		reverseExpandQuery := reverseexpand.NewReverseExpandQuery(
			ds,
			typesys,
			reverseexpand.WithResolveNodeLimit(q.resolveNodeLimit),
			reverseexpand.WithDispatchThrottlerConfig(q.dispatchThrottlerConfig),
			reverseexpand.WithResolveNodeBreadthLimit(q.resolveNodeBreadthLimit),
			reverseexpand.WithLogger(q.logger),
		)

		reverseExpandDoneWithError := make(chan struct{}, 1)
		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		pool := concurrency.NewPool(cancelCtx, int(1+q.resolveNodeBreadthLimit))

		pool.Go(func(ctx context.Context) error {
			reverseExpandResolutionMetadata := reverseexpand.NewResolutionMetadata()
			err := reverseExpandQuery.Execute(ctx, &reverseexpand.ReverseExpandRequest{
				StoreID:          req.GetStoreId(),
				ObjectType:       targetObjectType,
				Relation:         targetRelation,
				User:             sourceUserRef,
				ContextualTuples: req.GetContextualTuples().GetTupleKeys(),
				Context:          req.GetContext(),
				Consistency:      req.GetConsistency(),
			}, reverseExpandResultsChan, reverseExpandResolutionMetadata)
			if err != nil {
				reverseExpandDoneWithError <- struct{}{}
				return err
			}
			resolutionMetadata.DispatchCounter.Add(reverseExpandResolutionMetadata.DispatchCounter.Load())
			if !resolutionMetadata.WasThrottled.Load() && reverseExpandResolutionMetadata.WasThrottled.Load() {
				resolutionMetadata.WasThrottled.Store(true)
			}
			return nil
		})

	ConsumerReadLoop:
		for {
			select {
			case <-reverseExpandDoneWithError:
				cancel() // cancel any inflight work if e.g. model too complex
				break ConsumerReadLoop
			case <-ctx.Done():
				cancel() // cancel any inflight work if e.g. deadline exceeded
				break ConsumerReadLoop
			case res, channelOpen := <-reverseExpandResultsChan:
				if !channelOpen {
					// don't cancel here. Reverse Expand has finished finding candidate object IDs
					// but since we haven't collected "maxResults",
					// we need to wait until all the inflight Checks finish in the hopes that
					// we collect a few more object IDs.
					// if we send a cancellation now, we might miss those.
					break ConsumerReadLoop
				}

				if (maxResults != 0) && objectsFound.Load() >= maxResults {
					cancel() // cancel any inflight work if we already found enough results
					break ConsumerReadLoop
				}

				if res.ResultStatus == reverseexpand.NoFurtherEvalStatus {
					noFurtherEvalRequiredCounter.Inc()
					trySendObject(ctx, res.Object, &objectsFound, maxResults, resultsChan)
					continue
				}

				furtherEvalRequiredCounter.Inc()

				pool.Go(func(ctx context.Context) error {
					resp, checkRequestMetadata, err := NewCheckCommand(q.datastore, q.checkResolver, typesys,
						WithCheckCommandLogger(q.logger),
						WithCheckCommandMaxConcurrentReads(q.maxConcurrentReads),
						WithCheckDatastoreThrottler(q.datastoreThrottleThreshold, q.datastoreThrottleDuration),
					).
						Execute(ctx, &CheckCommandParams{
							StoreID:          req.GetStoreId(),
							TupleKey:         tuple.NewCheckRequestTupleKey(res.Object, req.GetRelation(), req.GetUser()),
							ContextualTuples: req.GetContextualTuples(),
							Context:          req.GetContext(),
							Consistency:      req.GetConsistency(),
						})
					if err != nil {
						return err
					}
					resolutionMetadata.DatastoreQueryCount.Add(resp.GetResolutionMetadata().DatastoreQueryCount)
					resolutionMetadata.DispatchCounter.Add(checkRequestMetadata.DispatchCounter.Load())
					if !resolutionMetadata.WasThrottled.Load() && checkRequestMetadata.WasThrottled.Load() {
						resolutionMetadata.WasThrottled.Store(true)
					}
					if resp.Allowed {
						trySendObject(ctx, res.Object, &objectsFound, maxResults, resultsChan)
					}
					return nil
				})
			}
		}

		err := pool.Wait()
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				resultsChan <- ListObjectsResult{Err: err}
			}
			// TODO set header to indicate "deadline exceeded"
		}
		close(resultsChan)
		dsMeta := ds.GetMetadata()
		resolutionMetadata.DatastoreQueryCount.Add(dsMeta.DatastoreQueryCount)
		resolutionMetadata.WasThrottled.CompareAndSwap(false, dsMeta.WasThrottled)
	}

	go handler()

	return nil
}

func trySendObject(ctx context.Context, object string, objectsFound *atomic.Uint32, maxResults uint32, resultsChan chan<- ListObjectsResult) {
	if maxResults != 0 {
		if objectsFound.Add(1) > maxResults {
			return
		}
	}
	concurrency.TrySendThroughChannel(ctx, ListObjectsResult{ObjectID: object}, resultsChan)
}

// Execute the ListObjectsQuery, returning a list of object IDs up to a maximum of q.listObjectsMaxResults
// or until q.listObjectsDeadline is hit, whichever happens first.
func (q *ListObjectsQuery) Execute(
	ctx context.Context,
	req *openfgav1.ListObjectsRequest,
) (*ListObjectsResponse, error) {
	resultsChan := make(chan ListObjectsResult, 1)
	maxResults := q.listObjectsMaxResults
	if maxResults > 0 {
		resultsChan = make(chan ListObjectsResult, maxResults)
	}

	timeoutCtx := ctx
	if q.listObjectsDeadline != 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, q.listObjectsDeadline)
		defer cancel()
	}

	resolutionMetadata := NewListObjectsResolutionMetadata()

	if req.GetConsistency() != openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY && q.cacheSettings.ShouldCacheListObjectsIterators() {
		span := trace.SpanFromContext(ctx)

		// Kick off background job to check if cache records are stale, inavlidating where needed
		q.sharedDatastoreResources.CacheController.InvalidateIfNeeded(req.GetStoreId(), span)
	}
	err := q.evaluate(timeoutCtx, req, resultsChan, maxResults, resolutionMetadata)
	if err != nil {
		return nil, err
	}

	objects := make([]string, 0)

	var errs error

	for result := range resultsChan {
		if result.Err != nil {
			if errors.Is(result.Err, graph.ErrResolutionDepthExceeded) {
				return nil, serverErrors.ErrAuthorizationModelResolutionTooComplex
			}

			if errors.Is(result.Err, condition.ErrEvaluationFailed) {
				errs = errors.Join(errs, result.Err)
				continue
			}

			return nil, serverErrors.HandleError("", result.Err)
		}

		objects = append(objects, result.ObjectID)
	}

	if len(objects) < int(maxResults) && errs != nil {
		return nil, errs
	}

	return &ListObjectsResponse{
		Objects:            objects,
		ResolutionMetadata: *resolutionMetadata,
	}, nil
}

// ExecuteStreamed executes the ListObjectsQuery, returning a stream of object IDs.
// It ignores the value of q.listObjectsMaxResults and returns all available results
// until q.listObjectsDeadline is hit.
func (q *ListObjectsQuery) ExecuteStreamed(ctx context.Context, req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) (*ListObjectsResolutionMetadata, error) {
	maxResults := uint32(math.MaxUint32)
	// make a buffered channel so that writer goroutines aren't blocked when attempting to send a result
	resultsChan := make(chan ListObjectsResult, streamedBufferSize)

	timeoutCtx := ctx
	if q.listObjectsDeadline != 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, q.listObjectsDeadline)
		defer cancel()
	}

	resolutionMetadata := NewListObjectsResolutionMetadata()

	err := q.evaluate(timeoutCtx, req, resultsChan, maxResults, resolutionMetadata)
	if err != nil {
		return nil, err
	}

	for result := range resultsChan {
		if result.Err != nil {
			if errors.Is(result.Err, graph.ErrResolutionDepthExceeded) {
				return nil, serverErrors.ErrAuthorizationModelResolutionTooComplex
			}

			if errors.Is(result.Err, condition.ErrEvaluationFailed) {
				return nil, serverErrors.ValidationError(result.Err)
			}

			return nil, serverErrors.HandleError("", result.Err)
		}

		if err := srv.Send(&openfgav1.StreamedListObjectsResponse{
			Object: result.ObjectID,
		}); err != nil {
			return nil, serverErrors.HandleError("", err)
		}
	}

	return resolutionMetadata, nil
}
