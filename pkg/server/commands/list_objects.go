package commands

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/featureflags"
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
	ff                      featureflags.Client
	logger                  logger.Logger
	encoder                 encoder.Encoder
	listObjectsDeadline     time.Duration
	listObjectsMaxResults   uint32
	resolveNodeLimit        uint32
	resolveNodeBreadthLimit uint32
	maxConcurrentReads      uint32

	dispatchThrottlerConfig threshold.Config

	datastoreThrottlingEnabled bool
	datastoreThrottleThreshold int
	datastoreThrottleDuration  time.Duration

	checkResolver            graph.CheckResolver
	cacheSettings            serverconfig.CacheSettings
	sharedDatastoreResources *shared.SharedDatastoreResources

	optimizationsEnabled bool // Indicates if experimental optimizations are enabled for ListObjectsResolver
	useShadowCache       bool // Indicates that the shadow cache should be used instead of the main cache

	pipelineEnabled bool // Indicates whether to run with the pipeline optimized code
}

type ListObjectsResolver interface {
	// Execute the ListObjectsQuery, returning a list of object IDs up to a maximum of q.listObjectsMaxResults
	// or until q.listObjectsDeadline is hit, whichever happens first.
	Execute(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error)

	// ExecuteStreamed executes the ListObjectsQuery, returning a stream of object IDs.
	// It ignores the value of q.listObjectsMaxResults and returns all available results
	// until q.listObjectsDeadline is hit.
	ExecuteStreamed(ctx context.Context, req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) (*ListObjectsResolutionMetadata, error)
}

type ListObjectsResolutionMetadata struct {
	// The total number of database reads from reverse_expand and Check (if any) to complete the ListObjects request
	DatastoreQueryCount atomic.Uint32

	// The total number of items read from the database during a ListObjects request.
	DatastoreItemCount atomic.Uint64

	// The total number of dispatches aggregated from reverse_expand and check resolutions (if any) to complete the ListObjects request
	DispatchCounter atomic.Uint32

	// DispatchThrottled indicates whether this request was throttled by dispatch count.
	DispatchThrottled atomic.Bool

	// DatastoreThrottled indicates whether the request was throttled by the Datastore.
	DatastoreThrottled atomic.Bool

	// WasWeightedGraphUsed indicates whether the weighted graph was used as the algorithm for the ListObjects request.
	WasWeightedGraphUsed atomic.Bool

	// CheckCounter is the total number of check requests made during the ListObjects execution for the optimized path
	CheckCounter atomic.Uint32
}

// listObjectsToken represents the continuation token structure for ListObjects pagination.
// It stores the object IDs that have already been returned to avoid duplicates on resume.
type listObjectsToken struct {
	Objects []string `json:"o"` // Short key to minimize token size
}

// compressToken compresses the token JSON using gzip to reduce token size.
// Object IDs are highly compressible (e.g., "folder:1", "folder:2"... compress well).
func compressToken(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(data); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decompressToken decompresses a gzip-compressed token.
func decompressToken(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

type ListObjectsResponse struct {
	Objects            []string
	ContinuationToken  string // Non-empty if more results may be available
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

func WithListObjectsDatastoreThrottler(enabled bool, threshold int, duration time.Duration) ListObjectsQueryOption {
	return func(d *ListObjectsQuery) {
		d.datastoreThrottlingEnabled = enabled
		d.datastoreThrottleThreshold = threshold
		d.datastoreThrottleDuration = duration
	}
}

func WithFeatureFlagClient(client featureflags.Client) ListObjectsQueryOption {
	return func(d *ListObjectsQuery) {
		if client != nil {
			d.ff = client
			return
		}

		d.ff = featureflags.NewNoopFeatureFlagClient()
	}
}

func WithListObjectsUseShadowCache(useShadowCache bool) ListObjectsQueryOption {
	return func(d *ListObjectsQuery) {
		d.useShadowCache = useShadowCache
	}
}

func WithListObjectsPipelineEnabled(value bool) ListObjectsQueryOption {
	return func(d *ListObjectsQuery) {
		d.pipelineEnabled = value
	}
}

// WithListObjectsEncoder sets the encoder used for continuation token encoding/decoding.
func WithListObjectsEncoder(e encoder.Encoder) ListObjectsQueryOption {
	return func(d *ListObjectsQuery) {
		d.encoder = e
	}
}

func NewListObjectsQuery(
	ds storage.RelationshipTupleReader,
	checkResolver graph.CheckResolver,
	storeID string,
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
		encoder:                 encoder.NewBase64Encoder(),
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
		optimizationsEnabled: false,
		useShadowCache:       false,
		ff:                   featureflags.NewNoopFeatureFlagClient(),
	}

	for _, opt := range opts {
		opt(query)
	}

	if query.ff.Boolean(serverconfig.ExperimentalListObjectsOptimizations, storeID) {
		query.optimizationsEnabled = true
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

		var bufferSize uint32
		cappedMaxResults := uint32(math.Min(float64(maxResults), 1000)) // cap max results at 1000
		bufferSize = uint32(math.Max(float64(cappedMaxResults/10), 10)) // 10% of max results, but make it at least 10

		reverseExpandResultsChan := make(chan *reverseexpand.ReverseExpandResult, bufferSize)
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
			storagewrappers.DataResourceConfiguration{
				Resources:      q.sharedDatastoreResources,
				CacheSettings:  q.cacheSettings,
				UseShadowCache: q.useShadowCache,
			},
		)

		reverseExpandQuery := reverseexpand.NewReverseExpandQuery(
			ds,
			typesys,
			reverseexpand.WithResolveNodeLimit(q.resolveNodeLimit),
			reverseexpand.WithDispatchThrottlerConfig(q.dispatchThrottlerConfig),
			reverseexpand.WithResolveNodeBreadthLimit(q.resolveNodeBreadthLimit),
			reverseexpand.WithLogger(q.logger),
			reverseexpand.WithCheckResolver(q.checkResolver),
			reverseexpand.WithListObjectOptimizationsEnabled(q.optimizationsEnabled),
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
			if !resolutionMetadata.DispatchThrottled.Load() && reverseExpandResolutionMetadata.DispatchThrottled.Load() {
				resolutionMetadata.DispatchThrottled.Store(true)
			}
			resolutionMetadata.CheckCounter.Add(reverseExpandResolutionMetadata.CheckCounter.Load())
			resolutionMetadata.WasWeightedGraphUsed.Store(reverseExpandResolutionMetadata.WasWeightedGraphUsed.Load())
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
						WithCheckDatastoreThrottler(
							q.datastoreThrottlingEnabled,
							q.datastoreThrottleThreshold,
							q.datastoreThrottleDuration,
						),
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
					resolutionMetadata.DatastoreItemCount.Add(resp.GetResolutionMetadata().DatastoreItemCount)
					resolutionMetadata.DispatchCounter.Add(checkRequestMetadata.DispatchCounter.Load())
					if !resolutionMetadata.DispatchThrottled.Load() && checkRequestMetadata.DispatchThrottled.Load() {
						resolutionMetadata.DispatchThrottled.Store(true)
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
		resolutionMetadata.DatastoreItemCount.Add(dsMeta.DatastoreItemCount)
		resolutionMetadata.DatastoreThrottled.Store(dsMeta.WasThrottled)
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
	maxResults := q.listObjectsMaxResults

	timeoutCtx := ctx
	if q.listObjectsDeadline != 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, q.listObjectsDeadline)
		defer cancel()
	}

	targetObjectType := req.GetType()
	targetRelation := req.GetRelation()

	typesys, ok := typesystem.TypesystemFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("%w: typesystem missing in context", openfgaErrors.ErrUnknown)
	}

	if !typesystem.IsSchemaVersionSupported(typesys.GetSchemaVersion()) {
		return nil, serverErrors.ValidationError(typesystem.ErrInvalidSchemaVersion)
	}

	for _, ctxTuple := range req.GetContextualTuples().GetTupleKeys() {
		if err := validation.ValidateTupleForWrite(typesys, ctxTuple); err != nil {
			return nil, serverErrors.HandleTupleValidateError(err)
		}
	}

	_, err := typesys.GetRelation(targetObjectType, targetRelation)
	if err != nil {
		if errors.Is(err, typesystem.ErrObjectTypeUndefined) {
			return nil, serverErrors.TypeNotFound(targetObjectType)
		}

		if errors.Is(err, typesystem.ErrRelationUndefined) {
			return nil, serverErrors.RelationNotFound(targetRelation, targetObjectType, nil)
		}

		return nil, serverErrors.HandleError("", err)
	}

	if err := validation.ValidateUser(typesys, req.GetUser()); err != nil {
		return nil, serverErrors.ValidationError(fmt.Errorf("invalid 'user' value: %s", err))
	}

	if req.GetConsistency() != openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY {
		if q.cacheSettings.ShouldCacheListObjectsIterators() {
			// Kick off background job to check if cache records are stale, invalidating where needed
			q.sharedDatastoreResources.CacheController.InvalidateIfNeeded(ctx, req.GetStoreId())
		}
		if q.cacheSettings.ShouldShadowCacheListObjectsIterators() {
			q.sharedDatastoreResources.ShadowCacheController.InvalidateIfNeeded(ctx, req.GetStoreId())
		}
	}

	// Decode continuation token if provided (for pagination support)
	// TODO: Get continuation token from request once proto is updated
	// For now, this prepares the server-side logic
	var previouslyReturned map[string]struct{}
	continuationToken := "" // Will be populated from req once proto is updated
	if continuationToken != "" {
		decodedToken, err := q.encoder.Decode(continuationToken)
		if err != nil {
			return nil, serverErrors.ErrInvalidContinuationToken
		}
		// Decompress the token (tokens are gzip compressed to reduce size)
		decompressed, err := decompressToken(decodedToken)
		if err != nil {
			return nil, serverErrors.ErrInvalidContinuationToken
		}
		var token listObjectsToken
		if err := json.Unmarshal(decompressed, &token); err != nil {
			return nil, serverErrors.ErrInvalidContinuationToken
		}
		previouslyReturned = make(map[string]struct{}, len(token.Objects))
		for _, obj := range token.Objects {
			previouslyReturned[obj] = struct{}{}
		}
	}

	wgraph := typesys.GetWeightedGraph()

	if wgraph != nil && q.pipelineEnabled {
		ds := storagewrappers.NewRequestStorageWrapperWithCache(
			q.datastore,
			req.GetContextualTuples().GetTupleKeys(),
			&storagewrappers.Operation{
				Method:            apimethod.ListObjects,
				Concurrency:       q.maxConcurrentReads,
				ThrottlingEnabled: q.datastoreThrottlingEnabled,
				ThrottleThreshold: q.datastoreThrottleThreshold,
				ThrottleDuration:  q.datastoreThrottleDuration,
			},
			storagewrappers.DataResourceConfiguration{
				Resources:      q.sharedDatastoreResources,
				CacheSettings:  q.cacheSettings,
				UseShadowCache: q.useShadowCache,
			},
		)

		backend := &reverseexpand.Backend{
			Datastore:  ds,
			StoreID:    req.GetStoreId(),
			TypeSystem: typesys,
			Context:    req.GetContext(),
			Graph:      wgraph,
			Preference: req.GetConsistency(),
		}

		pipeline := reverseexpand.NewPipeline(backend)

		var source reverseexpand.Source
		var target reverseexpand.Target

		if source, ok = pipeline.Source(targetObjectType, targetRelation); !ok {
			return nil, serverErrors.ValidationError(fmt.Errorf("object: %s relation: %s not in graph", targetObjectType, targetRelation))
		}

		userParts := strings.Split(req.GetUser(), "#")

		objectParts := strings.Split(userParts[0], ":")
		objectType := objectParts[0]
		objectID := objectParts[1]

		if len(userParts) > 1 {
			objectType += "#" + userParts[1]
		}

		if target, ok = pipeline.Target(objectType, objectID); !ok {
			return nil, serverErrors.ValidationError(fmt.Errorf("user: %s relation: %s not in graph", objectType, objectID))
		}

		seq := pipeline.Build(ctx, source, target)

		var res ListObjectsResponse

		for obj := range seq {
			if timeoutCtx.Err() != nil {
				break
			}

			if obj.Err != nil {
				return nil, serverErrors.HandleError("", obj.Err)
			}

			// Skip objects already returned in previous requests (pagination)
			if previouslyReturned != nil {
				if _, seen := previouslyReturned[obj.Value]; seen {
					continue
				}
			}

			res.Objects = append(res.Objects, obj.Value)

			// Check if we've reached the max results limit
			if maxResults > 0 && uint32(len(res.Objects)) >= maxResults {
				break
			}
		}

		dsMeta := ds.GetMetadata()
		res.ResolutionMetadata.DatastoreQueryCount.Add(dsMeta.DatastoreQueryCount)
		res.ResolutionMetadata.DatastoreItemCount.Add(dsMeta.DatastoreItemCount)

		// Generate continuation token if results may be incomplete
		// This happens when: deadline exceeded OR max results limit reached
		deadlineExceeded := timeoutCtx.Err() == context.DeadlineExceeded
		hitMaxResults := maxResults > 0 && uint32(len(res.Objects)) >= maxResults
		if (deadlineExceeded || hitMaxResults) && len(res.Objects) > 0 {
			// Combine previously returned objects with newly returned objects
			// This ensures no duplicates across paginated requests
			allObjects := res.Objects
			for obj := range previouslyReturned {
				allObjects = append(allObjects, obj)
			}
			token := listObjectsToken{Objects: allObjects}
			tokenBytes, err := json.Marshal(token)
			if err == nil {
				// Compress token to reduce size (object IDs are highly compressible)
				compressed, err := compressToken(tokenBytes)
				if err == nil {
					encoded, err := q.encoder.Encode(compressed)
					if err == nil {
						res.ContinuationToken = encoded
					}
				}
			}
		}

		return &res, nil
	}

	// --------- OLD STUFF -----------
	resultsChan := make(chan ListObjectsResult, 1)
	if maxResults > 0 {
		resultsChan = make(chan ListObjectsResult, maxResults)
	}

	var listObjectsResponse ListObjectsResponse

	err = q.evaluate(timeoutCtx, req, resultsChan, maxResults, &listObjectsResponse.ResolutionMetadata)
	if err != nil {
		return nil, err
	}

	listObjectsResponse.Objects = make([]string, 0, maxResults)

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

		// Skip objects already returned in previous requests (pagination)
		if previouslyReturned != nil {
			if _, seen := previouslyReturned[result.ObjectID]; seen {
				continue
			}
		}

		listObjectsResponse.Objects = append(listObjectsResponse.Objects, result.ObjectID)
	}

	if len(listObjectsResponse.Objects) < int(maxResults) && errs != nil {
		return nil, errs
	}

	// Generate continuation token if results may be incomplete
	// This happens when: deadline exceeded OR max results limit reached
	deadlineExceeded := timeoutCtx.Err() == context.DeadlineExceeded
	hitMaxResults := maxResults > 0 && uint32(len(listObjectsResponse.Objects)) >= maxResults
	if (deadlineExceeded || hitMaxResults) && len(listObjectsResponse.Objects) > 0 {
		// Combine previously returned objects with newly returned objects
		// This ensures no duplicates across paginated requests
		allObjects := listObjectsResponse.Objects
		for obj := range previouslyReturned {
			allObjects = append(allObjects, obj)
		}
		token := listObjectsToken{Objects: allObjects}
		tokenBytes, err := json.Marshal(token)
		if err == nil {
			// Compress token to reduce size (object IDs are highly compressible)
			compressed, err := compressToken(tokenBytes)
			if err == nil {
				encoded, err := q.encoder.Encode(compressed)
				if err == nil {
					listObjectsResponse.ContinuationToken = encoded
				}
			}
		}
	}

	return &listObjectsResponse, nil
}

// ExecuteStreamed executes the ListObjectsQuery, returning a stream of object IDs.
// It ignores the value of q.listObjectsMaxResults and returns all available results
// until q.listObjectsDeadline is hit.
func (q *ListObjectsQuery) ExecuteStreamed(ctx context.Context, req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) (*ListObjectsResolutionMetadata, error) {
	maxResults := uint32(math.MaxUint32)

	timeoutCtx := ctx
	if q.listObjectsDeadline != 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, q.listObjectsDeadline)
		defer cancel()
	}

	var resolutionMetadata ListObjectsResolutionMetadata

	targetObjectType := req.GetType()
	targetRelation := req.GetRelation()

	typesys, ok := typesystem.TypesystemFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("%w: typesystem missing in context", openfgaErrors.ErrUnknown)
	}

	if !typesystem.IsSchemaVersionSupported(typesys.GetSchemaVersion()) {
		return nil, serverErrors.ValidationError(typesystem.ErrInvalidSchemaVersion)
	}

	for _, ctxTuple := range req.GetContextualTuples().GetTupleKeys() {
		if err := validation.ValidateTupleForWrite(typesys, ctxTuple); err != nil {
			return nil, serverErrors.HandleTupleValidateError(err)
		}
	}

	_, err := typesys.GetRelation(targetObjectType, targetRelation)
	if err != nil {
		if errors.Is(err, typesystem.ErrObjectTypeUndefined) {
			return nil, serverErrors.TypeNotFound(targetObjectType)
		}

		if errors.Is(err, typesystem.ErrRelationUndefined) {
			return nil, serverErrors.RelationNotFound(targetRelation, targetObjectType, nil)
		}

		return nil, serverErrors.HandleError("", err)
	}

	if err := validation.ValidateUser(typesys, req.GetUser()); err != nil {
		return nil, serverErrors.ValidationError(fmt.Errorf("invalid 'user' value: %s", err))
	}

	wgraph := typesys.GetWeightedGraph()

	if wgraph != nil && q.pipelineEnabled {
		ds := storagewrappers.NewRequestStorageWrapperWithCache(
			q.datastore,
			req.GetContextualTuples().GetTupleKeys(),
			&storagewrappers.Operation{
				Method:            apimethod.ListObjects,
				Concurrency:       q.maxConcurrentReads,
				ThrottlingEnabled: q.datastoreThrottlingEnabled,
				ThrottleThreshold: q.datastoreThrottleThreshold,
				ThrottleDuration:  q.datastoreThrottleDuration,
			},
			storagewrappers.DataResourceConfiguration{
				Resources:      q.sharedDatastoreResources,
				CacheSettings:  q.cacheSettings,
				UseShadowCache: q.useShadowCache,
			},
		)

		backend := &reverseexpand.Backend{
			Datastore:  ds,
			StoreID:    req.GetStoreId(),
			TypeSystem: typesys,
			Context:    req.GetContext(),
			Graph:      wgraph,
			Preference: req.GetConsistency(),
		}

		pipeline := reverseexpand.NewPipeline(backend)

		var source reverseexpand.Source
		var target reverseexpand.Target

		if source, ok = pipeline.Source(targetObjectType, targetRelation); !ok {
			return nil, serverErrors.ValidationError(fmt.Errorf("object: %s relation: %s not in graph", targetObjectType, targetRelation))
		}

		userParts := strings.Split(req.GetUser(), "#")

		objectParts := strings.Split(userParts[0], ":")
		objectType := objectParts[0]
		objectID := objectParts[1]

		if len(userParts) > 1 {
			objectType += "#" + userParts[1]
		}

		if target, ok = pipeline.Target(objectType, objectID); !ok {
			return nil, serverErrors.ValidationError(fmt.Errorf("user: %s relation: %s not in graph", objectType, objectID))
		}

		seq := pipeline.Build(ctx, source, target)

		var listObjectsCount uint32 = 0

		for obj := range seq {
			if timeoutCtx.Err() != nil {
				break
			}

			if obj.Err != nil {
				if errors.Is(obj.Err, condition.ErrEvaluationFailed) {
					return nil, serverErrors.ValidationError(obj.Err)
				}
				return nil, serverErrors.HandleError("", obj.Err)
			}

			if err := srv.Send(&openfgav1.StreamedListObjectsResponse{
				Object: obj.Value,
			}); err != nil {
				return nil, serverErrors.HandleError("", err)
			}

			listObjectsCount++

			// Check if we've reached the max results limit
			if maxResults > 0 && listObjectsCount >= maxResults {
				break
			}
		}

		dsMeta := ds.GetMetadata()
		resolutionMetadata.DatastoreQueryCount.Add(dsMeta.DatastoreQueryCount)
		resolutionMetadata.DatastoreItemCount.Add(dsMeta.DatastoreItemCount)
		return &resolutionMetadata, nil
	}

	// make a buffered channel so that writer goroutines aren't blocked when attempting to send a result
	resultsChan := make(chan ListObjectsResult, streamedBufferSize)

	err = q.evaluate(timeoutCtx, req, resultsChan, maxResults, &resolutionMetadata)
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

	return &resolutionMetadata, nil
}
