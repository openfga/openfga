package commands

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
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

type ListObjectsQuery struct {
	Datastore             storage.OpenFGADatastore
	Logger                logger.Logger
	ListObjectsDeadline   time.Duration
	ListObjectsMaxResults uint32
	ResolveNodeLimit      uint32
}

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
	resultsChan chan<- string,
	errChan chan<- error,
	maxResults uint32,
) error {

	span := trace.SpanFromContext(ctx)

	targetObjectType := req.GetType()
	targetRelation := req.GetRelation()

	typesys, ok := typesystem.TypesystemFromContext(ctx)
	if !ok {
		panic("typesystem missing in context")
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
		ctx = typesystem.ContextWithTypesystem(ctx, typesys)
		span.SetAttributes(attribute.Bool(listObjectsOptimizedKey, false))

		err = q.performChecks(ctx, req, resultsChan, maxResults)
		if err != nil {
			errChan <- err
			close(errChan)
			return
		}
		close(resultsChan)
	}

	hasTypeInfo, err := typesys.HasTypeInfo(targetObjectType, targetRelation)
	if err != nil {
		q.Logger.WarnWithContext(
			ctx, fmt.Sprintf("failed to lookup type info for relation '%s'", targetRelation),
			zap.String("store_id", req.GetStoreId()),
			zap.String("object_type", targetObjectType),
		)
	}

	containsIntersection, _ := typesys.RelationInvolvesIntersection(targetObjectType, targetRelation)
	containsExclusion, _ := typesys.RelationInvolvesExclusion(targetObjectType, targetRelation)

	// ConnectedObjects currently only supports models that do not include intersection and exclusion,
	// and the model must include type info for ConnectedObjects to work.
	if !containsIntersection && !containsExclusion && hasTypeInfo {
		userObj, userRel := tuple.SplitObjectRelation(req.GetUser())

		userObjType, userObjID := tuple.SplitObject(userObj)

		var targetUserRef isUserRef
		targetUserRef = &UserRefObject{
			Object: &openfgapb.Object{
				Type: userObjType,
				Id:   userObjID,
			},
		}

		if tuple.IsTypedWildcard(userObj) {
			targetUserRef = &UserRefTypedWildcard{Type: tuple.GetType(userObj)}
		}

		if userRel != "" {
			targetUserRef = &UserRefObjectRelation{
				ObjectRelation: &openfgapb.ObjectRelation{
					Object:   userObj,
					Relation: userRel,
				},
			}
		}

		handler = func() {
			span.SetAttributes(attribute.Bool(listObjectsOptimizedKey, true))

			connectObjCmd := &ConnectedObjectsCommand{
				Datastore:        q.Datastore,
				ResolveNodeLimit: q.ResolveNodeLimit,
				Limit:            maxResults,
			}

			err = connectObjCmd.StreamedConnectedObjects(ctx, &ConnectedObjectsRequest{
				StoreID:          req.GetStoreId(),
				Typesystem:       typesys,
				ObjectType:       targetObjectType,
				Relation:         targetRelation,
				User:             targetUserRef,
				ContextualTuples: req.GetContextualTuples().GetTupleKeys(),
			}, resultsChan)
			if err != nil {
				errChan <- err
				close(errChan)
				return
			}
			close(resultsChan)
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

	resultsChan := make(chan string, 1)
	maxResults := q.ListObjectsMaxResults
	if maxResults > 0 {
		resultsChan = make(chan string, maxResults)
	}

	// make a buffered channel so that writer goroutines aren't blocked when attempting to send an error
	errChan := make(chan error, 1)

	timeoutCtx := ctx
	if q.ListObjectsDeadline != 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, q.ListObjectsDeadline)
		defer cancel()
	}

	err := q.evaluate(timeoutCtx, req, resultsChan, errChan, maxResults)
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

		case objectID, channelOpen := <-resultsChan:
			if !channelOpen {
				return &openfgapb.ListObjectsResponse{
					Objects: objects,
				}, nil
			}
			objects = append(objects, objectID)
		case genericError, valueAvailable := <-errChan:
			if valueAvailable {
				return nil, serverErrors.NewInternalError("", genericError)
			}
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
	resultsChan := make(chan string, streamedBufferSize)

	// make a buffered channel so that writer goroutines aren't blocked when attempting to send an error
	errChan := make(chan error, 1)

	timeoutCtx := ctx
	if q.ListObjectsDeadline != 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, q.ListObjectsDeadline)
		defer cancel()
	}

	err := q.evaluate(timeoutCtx, req, resultsChan, errChan, maxResults)
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

		case object, ok := <-resultsChan:
			if !ok {
				// Channel closed! No more results.
				return nil
			}

			if err := srv.Send(&openfgapb.StreamedListObjectsResponse{
				Object: object,
			}); err != nil {
				return serverErrors.NewInternalError("", err)
			}
		case genericError, ok := <-errChan:
			if ok {
				return serverErrors.NewInternalError("", genericError)
			}
		}
	}
}

func (q *ListObjectsQuery) performChecks(ctx context.Context, req listObjectsRequest, resultsChan chan<- string, maxResults uint32) error {
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
	resultsChan chan<- string,
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
		return nil
	}

	if resp.Allowed && atomic.AddUint32(objectsFound, 1) <= maxResults {
		resultsChan <- tuple.ObjectKey(obj)
	}

	return nil
}
