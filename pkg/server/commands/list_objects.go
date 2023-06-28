package commands

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	streamedBufferSize      = 100
	maximumConcurrentChecks = 100 // todo(jon-whit): make this configurable, but for now limit to 100 concurrent checks
	listObjectsOptimizedKey = "list_objects_optimized"
)

var (
	furtherEvalRequiredCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "list_objects_further_eval_required_count",
	})

	noFurtherEvalRequiredCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "list_objects_no_further_eval_required_count",
	})
)

type ListObjectsQuery struct {
	Datastore                     storage.RelationshipTupleReader
	Logger                        logger.Logger
	ListObjectsDeadline           time.Duration
	ListObjectsMaxResults         uint32
	ResolveNodeLimit              uint32
	CheckConcurrencyLimit         uint32
	OptimizeIntersectionExclusion bool
}

type ListObjectsResult struct {
	ObjectID string
	Err      error
}

// listObjectsRequest captures the RPC request definition interface for the ListObjects API.
// The unary and streaming RPC definitions implement this interface, and so it can be used
// interchangably for a canonical representation between the two.
type listObjectsRequest interface {
	GetStoreId() string
	GetAuthorizationModelId() string
	GetType() string
	GetRelation() string
	GetUser() string
	GetContextualTuples() *openfgapb.ContextualTupleKeys
}

func (q *ListObjectsQuery) evaluate(
	ctx context.Context,
	req listObjectsRequest,
	resultsChan chan<- ListObjectsResult,
	maxResults uint32,
) error {

	span := trace.SpanFromContext(ctx)

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

		var sourceUserRef isUserRef
		sourceUserRef = &UserRefObject{
			Object: &openfgapb.Object{
				Type: userObjType,
				Id:   userObjID,
			},
		}

		if tuple.IsTypedWildcard(userObj) {
			sourceUserRef = &UserRefTypedWildcard{Type: tuple.GetType(userObj)}

		}

		if userRel != "" {
			sourceUserRef = &UserRefObjectRelation{
				ObjectRelation: &openfgapb.ObjectRelation{
					Object:   userObj,
					Relation: userRel,
				},
			}
		}

		connectedObjectsResChan := make(chan *ConnectedObjectsResult, 1)
		var objectsFound = new(uint32)

		connectedObjectsCmd := &ConnectedObjectsCommand{
			Datastore:        q.Datastore,
			Typesystem:       typesys,
			ResolveNodeLimit: q.ResolveNodeLimit,
			Limit:            maxResults,
		}

		go func() {
			err = connectedObjectsCmd.StreamedConnectedObjects(ctx, &ConnectedObjectsRequest{
				StoreID:          req.GetStoreId(),
				Typesystem:       typesys,
				ObjectType:       targetObjectType,
				Relation:         targetRelation,
				User:             sourceUserRef,
				ContextualTuples: req.GetContextualTuples().GetTupleKeys(),
			}, connectedObjectsResChan)
			if err != nil {
				resultsChan <- ListObjectsResult{Err: err}
			}

			close(connectedObjectsResChan)
		}()

		checkResolver := graph.NewLocalChecker(
			storage.NewCombinedTupleReader(q.Datastore, req.GetContextualTuples().GetTupleKeys()),
			q.CheckConcurrencyLimit,
		)

		concurrencyLimiterCh := make(chan struct{}, maximumConcurrentChecks)

		wg := sync.WaitGroup{}

		for res := range connectedObjectsResChan {

			if res.ResultStatus == NoFurtherEvalStatus {
				noFurtherEvalRequiredCounter.Inc()

				if atomic.AddUint32(objectsFound, 1) <= maxResults {
					resultsChan <- ListObjectsResult{ObjectID: res.Object}
				}

				continue
			}

			furtherEvalRequiredCounter.Inc()

			wg.Add(1)
			go func(res *ConnectedObjectsResult) {
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
						Depth: q.ResolveNodeLimit,
					},
				})
				if err != nil {
					resultsChan <- ListObjectsResult{Err: err}
					return
				}

				if resp.Allowed && atomic.AddUint32(objectsFound, 1) <= maxResults {
					resultsChan <- ListObjectsResult{ObjectID: res.Object}
				}
			}(res)
		}

		wg.Wait()

		close(resultsChan)
	}

	containsIntersection, _ := typesys.RelationInvolvesIntersection(targetObjectType, targetRelation)
	containsExclusion, _ := typesys.RelationInvolvesExclusion(targetObjectType, targetRelation)

	if (containsIntersection || containsExclusion) && !q.OptimizeIntersectionExclusion {
		handler = func() {
			defer close(resultsChan)

			ctx = typesystem.ContextWithTypesystem(ctx, typesys)
			span.SetAttributes(attribute.Bool(listObjectsOptimizedKey, false))

			err = q.performChecks(ctx, req, resultsChan, maxResults)
			if err != nil {
				resultsChan <- ListObjectsResult{Err: err}
			}
		}
	}

	go handler()

	return nil
}

// Execute the ListObjectsQuery, returning a list of object IDs up to a maximum of q.ListObjectsMaxResults
// or until q.ListObjectsDeadline is hit, whichever happens first.
func (q *ListObjectsQuery) Execute(
	ctx context.Context,
	req *openfgapb.ListObjectsRequest,
) (*openfgapb.ListObjectsResponse, error) {

	resultsChan := make(chan ListObjectsResult, 1)
	maxResults := q.ListObjectsMaxResults
	if maxResults > 0 {
		resultsChan = make(chan ListObjectsResult, maxResults)
	}

	timeoutCtx := ctx
	if q.ListObjectsDeadline != 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, q.ListObjectsDeadline)
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
			q.Logger.WarnWithContext(
				ctx, "list objects timeout with list object configuration timeout",
				zap.String("timeout duration", q.ListObjectsDeadline.String()),
			)
			return &openfgapb.ListObjectsResponse{
				Objects: objects,
			}, nil

		case result, channelOpen := <-resultsChan:
			if result.Err != nil {
				return nil, serverErrors.NewInternalError("", result.Err)
			}

			if !channelOpen {
				return &openfgapb.ListObjectsResponse{
					Objects: objects,
				}, nil
			}
			objects = append(objects, result.ObjectID)
		}
	}
}

// ExecuteStreamed executes the ListObjectsQuery, returning a stream of object IDs.
// It ignores the value of q.ListObjectsMaxResults and returns all available results
// until q.ListObjectsDeadline is hit
func (q *ListObjectsQuery) ExecuteStreamed(
	ctx context.Context,
	req *openfgapb.StreamedListObjectsRequest,
	srv openfgapb.OpenFGAService_StreamedListObjectsServer,
) error {

	maxResults := uint32(math.MaxUint32)
	// make a buffered channel so that writer goroutines aren't blocked when attempting to send a result
	resultsChan := make(chan ListObjectsResult, streamedBufferSize)

	timeoutCtx := ctx
	if q.ListObjectsDeadline != 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, q.ListObjectsDeadline)
		defer cancel()
	}

	err := q.evaluate(timeoutCtx, req, resultsChan, maxResults)
	if err != nil {
		return err
	}

	for {
		select {

		case <-timeoutCtx.Done():
			q.Logger.WarnWithContext(
				ctx, "list objects timeout with list object configuration timeout",
				zap.String("timeout duration", q.ListObjectsDeadline.String()),
			)
			return nil

		case result, channelOpen := <-resultsChan:
			if !channelOpen {
				// Channel closed! No more results.
				return nil
			}

			if result.Err != nil {
				return serverErrors.NewInternalError("", result.Err)
			}

			if err := srv.Send(&openfgapb.StreamedListObjectsResponse{
				Object: result.ObjectID,
			}); err != nil {
				return serverErrors.NewInternalError("", err)
			}
		}
	}
}

func (q *ListObjectsQuery) performChecks(ctx context.Context, req listObjectsRequest, resultsChan chan<- ListObjectsResult, maxResults uint32) error {
	ctx, span := tracer.Start(ctx, "performChecks")
	defer span.End()
	var objectsFound = new(uint32)

	combinedDatastore := storage.NewCombinedTupleReader(q.Datastore, req.GetContextualTuples().GetTupleKeys())

	iter, err := combinedDatastore.ListObjectsByType(ctx, req.GetStoreId(), req.GetType())
	if err != nil {
		return err
	}
	defer iter.Stop()

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(maximumConcurrentChecks)

	checkResolver := graph.NewLocalChecker(combinedDatastore, maximumConcurrentChecks)
	// iterate over all object IDs in the store and check if the user has relation with each
	for {
		object, err := iter.Next()
		if err != nil {
			if !errors.Is(err, storage.ErrIteratorDone) {
				return err
			}
			break
		}
		if atomic.LoadUint32(objectsFound) >= maxResults {
			break
		}

		checkFunction := func() error {
			return q.internalCheck(ctx, checkResolver, object, req, objectsFound, resultsChan, maxResults)
		}

		g.Go(checkFunction)
	}

	return g.Wait()
}

func (q *ListObjectsQuery) internalCheck(
	ctx context.Context,
	checkResolver *graph.LocalChecker,
	obj *openfgapb.Object,
	req listObjectsRequest,
	objectsFound *uint32,
	resultsChan chan<- ListObjectsResult,
	maxResults uint32,
) error {
	resp, err := checkResolver.ResolveCheck(ctx, &graph.ResolveCheckRequest{
		StoreID:              req.GetStoreId(),
		AuthorizationModelID: req.GetAuthorizationModelId(),
		TupleKey:             tuple.NewTupleKey(tuple.ObjectKey(obj), req.GetRelation(), req.GetUser()),
		ContextualTuples:     req.GetContextualTuples().GetTupleKeys(),
		ResolutionMetadata: &graph.ResolutionMetadata{
			Depth: q.ResolveNodeLimit,
		},
	})
	if err != nil {
		// ignore the error. we don't want to abort everything if one of the checks failed.
		q.Logger.ErrorWithContext(ctx, "check_error", zap.Error(err))
		return err
	}

	if resp.Allowed && atomic.AddUint32(objectsFound, 1) <= maxResults {
		resultsChan <- ListObjectsResult{ObjectID: tuple.ObjectKey(obj)}
	}

	return nil
}
