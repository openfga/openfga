package commands

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands/connectedobjects"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	streamedBufferSize = 100

	// same values as run.DefaultConfig() (TODO break the import cycle, remove these hardcoded values and import those constants here)
	defaultResolveNodeLimit        = 25
	defaultResolveNodeBreadthLimit = 100
	defaultListObjectsDeadline     = 3 * time.Second
	defaultListObjectsMaxResults   = 1000
)

var (
	furtherEvalRequiredCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "list_objects_further_eval_required_count",
		Help: "Number of objects in a ListObjects call that needed to issue a Check call to determine a final result",
	})

	noFurtherEvalRequiredCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "list_objects_no_further_eval_required_count",
		Help: "Number of objects in a ListObjects call that needed to issue a Check call to determine a final result",
	})
)

type ListObjectsQuery struct {
	datastore               storage.RelationshipTupleReader
	logger                  logger.Logger
	listObjectsDeadline     time.Duration
	listObjectsMaxResults   uint32
	resolveNodeLimit        uint32
	resolveNodeBreadthLimit uint32

	checkOptions []graph.LocalCheckerOption
}

type ListObjectsQueryOption func(d *ListObjectsQuery)

func WithListObjectsDeadline(deadline time.Duration) ListObjectsQueryOption {
	return func(d *ListObjectsQuery) {
		d.listObjectsDeadline = deadline
	}
}

func WithListObjectsMaxResults(max uint32) ListObjectsQueryOption {
	return func(d *ListObjectsQuery) {
		d.listObjectsMaxResults = max
	}
}

// WithResolveNodeLimit see server.WithResolveNodeLimit
func WithResolveNodeLimit(limit uint32) ListObjectsQueryOption {
	return func(d *ListObjectsQuery) {
		d.resolveNodeLimit = limit
	}
}

// WithResolveNodeBreadthLimit see server.WithResolveNodeBreadthLimit
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

func WithCheckOptions(checkOptions []graph.LocalCheckerOption) ListObjectsQueryOption {
	return func(d *ListObjectsQuery) {
		d.checkOptions = checkOptions
	}
}

func NewListObjectsQuery(ds storage.RelationshipTupleReader, opts ...ListObjectsQueryOption) *ListObjectsQuery {
	query := &ListObjectsQuery{
		datastore:               ds,
		logger:                  logger.NewNoopLogger(),
		listObjectsDeadline:     defaultListObjectsDeadline,
		listObjectsMaxResults:   defaultListObjectsMaxResults,
		resolveNodeLimit:        defaultResolveNodeLimit,
		resolveNodeBreadthLimit: defaultResolveNodeBreadthLimit,
		checkOptions:            []graph.LocalCheckerOption{},
	}

	for _, opt := range opts {
		opt(query)
	}

	return query
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
}

func (q *ListObjectsQuery) evaluate(
	ctx context.Context,
	req listObjectsRequest,
	resultsChan chan<- ListObjectsResult,
	maxResults uint32,
) error {

	targetObjectType := req.GetType()
	targetRelation := req.GetRelation()

	typesys, ok := typesystem.TypesystemFromContext(ctx)
	if !ok {
		panic("typesystem missing in context")
	}

	if !typesystem.IsSchemaVersionSupported(typesys.GetSchemaVersion()) {
		return serverErrors.ValidationError(typesystem.ErrInvalidSchemaVersion)
	}

	for _, ctxTuple := range req.GetContextualTuples().GetTupleKeys() {
		if err := validation.ValidateTuple(typesys, ctxTuple); err != nil {
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

		return serverErrors.NewInternalError("", err)
	}

	if err := validation.ValidateUser(typesys, req.GetUser()); err != nil {
		return serverErrors.ValidationError(fmt.Errorf("invalid 'user' value: %s", err))
	}

	handler := func() {
		userObj, userRel := tuple.SplitObjectRelation(req.GetUser())
		userObjType, userObjID := tuple.SplitObject(userObj)

		var sourceUserRef connectedobjects.IsUserRef
		sourceUserRef = &connectedobjects.UserRefObject{
			Object: &openfgav1.Object{
				Type: userObjType,
				Id:   userObjID,
			},
		}

		if tuple.IsTypedWildcard(userObj) {
			sourceUserRef = &connectedobjects.UserRefTypedWildcard{Type: tuple.GetType(userObj)}

		}

		if userRel != "" {
			sourceUserRef = &connectedobjects.UserRefObjectRelation{
				ObjectRelation: &openfgav1.ObjectRelation{
					Object:   userObj,
					Relation: userRel,
				},
			}
		}

		connectedObjectsResChan := make(chan *connectedobjects.ConnectedObjectsResult, 1)
		var objectsFound = new(uint32)

		connectedObjectsQuery := connectedobjects.NewConnectedObjectsQuery(q.datastore, typesys,
			connectedobjects.WithResolveNodeLimit(q.resolveNodeLimit),
			connectedobjects.WithResolveNodeBreadthLimit(q.resolveNodeBreadthLimit),
		)

		go func() {
			err = connectedObjectsQuery.Execute(ctx, &connectedobjects.ConnectedObjectsRequest{
				StoreID:          req.GetStoreId(),
				ObjectType:       targetObjectType,
				Relation:         targetRelation,
				User:             sourceUserRef,
				ContextualTuples: req.GetContextualTuples().GetTupleKeys(),
			}, connectedObjectsResChan)
			if err != nil {
				resultsChan <- ListObjectsResult{Err: err}
			}

			// this is necessary to terminate the range loop below
			close(connectedObjectsResChan)
		}()

		checkResolver := graph.NewLocalChecker(
			storagewrappers.NewCombinedTupleReader(q.datastore, req.GetContextualTuples().GetTupleKeys()),
			q.checkOptions...,
		)
		defer checkResolver.Close()

		concurrencyLimiterCh := make(chan struct{}, q.resolveNodeBreadthLimit)

		wg := sync.WaitGroup{}

		for res := range connectedObjectsResChan {
			if atomic.LoadUint32(objectsFound) >= maxResults {
				break
			}
			if res.ResultStatus == connectedobjects.NoFurtherEvalStatus {
				noFurtherEvalRequiredCounter.Inc()
				trySendObject(res.Object, objectsFound, maxResults, resultsChan)
				continue
			}

			furtherEvalRequiredCounter.Inc()

			wg.Add(1)
			go func(res *connectedobjects.ConnectedObjectsResult) {
				defer func() {
					<-concurrencyLimiterCh
					wg.Done()
				}()

				concurrencyLimiterCh <- struct{}{}

				resp, err := checkResolver.ResolveCheck(ctx, &graph.ResolveCheckRequest{
					StoreID:              req.GetStoreId(),
					AuthorizationModelID: req.GetAuthorizationModelId(),
					TupleKey:             tuple.NewTupleKey(res.Object, req.GetRelation(), req.GetUser()),
					ContextualTuples:     req.GetContextualTuples().GetTupleKeys(),
					ResolutionMetadata: &graph.ResolutionMetadata{
						Depth: q.resolveNodeLimit,
					},
				})
				if err != nil {
					resultsChan <- ListObjectsResult{Err: err}
					return
				}

				if resp.Allowed {
					trySendObject(res.Object, objectsFound, maxResults, resultsChan)
				}
			}(res)
		}

		wg.Wait()

		close(resultsChan)
	}

	go handler()

	return nil
}

func trySendObject(object string, objectsFound *uint32, maxResults uint32, resultsChan chan<- ListObjectsResult) {
	if objectsFound != nil && atomic.AddUint32(objectsFound, 1) > maxResults {
		return
	}
	resultsChan <- ListObjectsResult{ObjectID: object}
}

// Execute the ListObjectsQuery, returning a list of object IDs up to a maximum of q.listObjectsMaxResults
// or until q.listObjectsDeadline is hit, whichever happens first.
func (q *ListObjectsQuery) Execute(
	ctx context.Context,
	req *openfgav1.ListObjectsRequest,
) (*openfgav1.ListObjectsResponse, error) {

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

	err := q.evaluate(timeoutCtx, req, resultsChan, maxResults)
	if err != nil {
		return nil, err
	}

	objects := make([]string, 0)

	for {
		select {

		case <-timeoutCtx.Done():
			q.logger.WarnWithContext(
				ctx, fmt.Sprintf("list objects timeout after %s", q.listObjectsDeadline.String()),
			)
			return &openfgav1.ListObjectsResponse{
				Objects: objects,
			}, nil

		case result, channelOpen := <-resultsChan:
			if result.Err != nil {
				if errors.Is(result.Err, serverErrors.AuthorizationModelResolutionTooComplex) {
					return nil, result.Err
				}
				return nil, serverErrors.HandleError("", result.Err)
			}

			if !channelOpen {
				return &openfgav1.ListObjectsResponse{
					Objects: objects,
				}, nil
			}
			objects = append(objects, result.ObjectID)
		}
	}
}

// ExecuteStreamed executes the ListObjectsQuery, returning a stream of object IDs.
// It ignores the value of q.listObjectsMaxResults and returns all available results
// until q.listObjectsDeadline is hit
func (q *ListObjectsQuery) ExecuteStreamed(
	ctx context.Context,
	req *openfgav1.StreamedListObjectsRequest,
	srv openfgav1.OpenFGAService_StreamedListObjectsServer,
) error {

	maxResults := uint32(math.MaxUint32)
	// make a buffered channel so that writer goroutines aren't blocked when attempting to send a result
	resultsChan := make(chan ListObjectsResult, streamedBufferSize)

	timeoutCtx := ctx
	if q.listObjectsDeadline != 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, q.listObjectsDeadline)
		defer cancel()
	}

	err := q.evaluate(timeoutCtx, req, resultsChan, maxResults)
	if err != nil {
		return err
	}

	for {
		select {

		case <-timeoutCtx.Done():
			q.logger.WarnWithContext(
				ctx, fmt.Sprintf("list objects timeout after %s", q.listObjectsDeadline.String()),
			)
			return nil

		case result, channelOpen := <-resultsChan:
			if !channelOpen {
				// Channel closed! No more results.
				return nil
			}

			if result.Err != nil {
				if errors.Is(result.Err, serverErrors.AuthorizationModelResolutionTooComplex) {
					return result.Err
				}

				return serverErrors.HandleError("", result.Err)
			}

			if err := srv.Send(&openfgav1.StreamedListObjectsResponse{
				Object: result.ObjectID,
			}); err != nil {
				return serverErrors.NewInternalError("", err)
			}
		}
	}
}
