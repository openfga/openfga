package commands

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/go-errors/errors"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/tuple"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

const (
	MaximumConcurrentChecks = 100 // todo(jon-whit): make this configurable, but for now limit to 100 concurrent checks
)

type ListObjectsQuery struct {
	datastore                    storage.OpenFGADatastore
	logger                       logger.Logger
	tracer                       trace.Tracer
	meter                        metric.Meter
	listObjectsDeadlineInSeconds int
	listObjectsMaxResults        uint32
	resolveNodeLimit             uint32
}

// NewListObjectsQuery creates a ListObjectsQuery
func NewListObjectsQuery(datastore storage.OpenFGADatastore, tracer trace.Tracer, logger logger.Logger, meter metric.Meter, listObjectsDeadlineInSeconds int, listObjectsMaxResults uint32, ResolveNodeLimit uint32) *ListObjectsQuery {
	return &ListObjectsQuery{
		datastore:                    datastore,
		logger:                       logger,
		tracer:                       tracer,
		meter:                        meter,
		listObjectsDeadlineInSeconds: listObjectsDeadlineInSeconds,
		listObjectsMaxResults:        listObjectsMaxResults,
		resolveNodeLimit:             ResolveNodeLimit,
	}
}

// Execute the ListObjectsQuery, returning a list of object IDs
func (q *ListObjectsQuery) Execute(ctx context.Context, req *openfgapb.ListObjectsRequest) (*openfgapb.ListObjectsResponse, error) {
	err := q.validateInput(ctx, req.StoreId, req.Type, req.AuthorizationModelId, req.Relation)
	if err != nil {
		return nil, err
	}

	resultsChan := make(chan string, q.listObjectsMaxResults)
	errChan := make(chan error)
	resolvedChan := make(chan struct{})

	timeoutCtx := ctx
	if q.listObjectsDeadlineInSeconds != 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, time.Second*time.Duration(q.listObjectsDeadlineInSeconds))
		defer cancel()
	}

	go func() {
		q.performChecks(timeoutCtx, &PerformChecksInput{
			storeID:     req.StoreId,
			authModelID: req.AuthorizationModelId,
			objectType:  req.Type,
			relation:    req.Relation,
			user:        req.User,
			ctxTuples:   req.ContextualTuples,
		}, resultsChan, errChan, resolvedChan)
	}()

	select {
	case <-timeoutCtx.Done():
		return nil, serverErrors.NewInternalError("Timeout exceeded", timeoutCtx.Err())
	case <-resolvedChan:
		objectIDs := make([]string, 0)
		for objectID := range resultsChan {
			objectIDs = append(objectIDs, objectID)
		}

		return &openfgapb.ListObjectsResponse{
			ObjectIds: objectIDs,
		}, nil
	case genericError := <-errChan:
		return nil, genericError
	}
}

func (q *ListObjectsQuery) ExecuteStreamed(ctx context.Context, req *openfgapb.StreamedListObjectsRequest, srv openfgapb.OpenFGAService_StreamedListObjectsServer) error {
	err := q.validateInput(ctx, req.StoreId, req.Type, req.AuthorizationModelId, req.Relation)
	if err != nil {
		return err
	}

	resultsChan := make(chan string, q.listObjectsMaxResults)
	errChan := make(chan error)
	resolvedChan := make(chan struct{})

	timeoutCtx := ctx
	if q.listObjectsDeadlineInSeconds != 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, time.Second*time.Duration(q.listObjectsDeadlineInSeconds))
		defer cancel()
	}

	go func() {
		q.performChecks(timeoutCtx, &PerformChecksInput{
			storeID:     req.StoreId,
			authModelID: req.AuthorizationModelId,
			objectType:  req.Type,
			relation:    req.Relation,
			user:        req.User,
			ctxTuples:   req.ContextualTuples,
		}, resultsChan, errChan, resolvedChan)
	}()

	for {
		select {
		case <-timeoutCtx.Done():
			return nil
		case objectID, ok := <-resultsChan:
			if !ok {
				return nil //channel was closed
			}
			if err := srv.Send(&openfgapb.StreamedListObjectsResponse{
				ObjectId: objectID,
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

func (q *ListObjectsQuery) getUniqueObjects(ctx context.Context, storeID, targetObjectType string, ctxTuples *openfgapb.ContextualTupleKeys) ([]string, error) {
	uniqueObjects, err := q.datastore.ListObjectsByType(ctx, storage.ListObjectsFilter{
		StoreID:    storeID,
		ObjectType: targetObjectType,
	})
	if err != nil {
		return nil, serverErrors.NewInternalError("", err)
	}

	if ctxTuples != nil {
		// Take the relevant objects (i.e. same type) from the contextual tuples and add them to the result
		uniqueSet := make(map[string]bool, len(uniqueObjects))
		for _, o := range uniqueObjects {
			_, found := uniqueSet[o]
			if !found {
				uniqueSet[o] = true
			}
		}

		for _, ctxTuple := range ctxTuples.TupleKeys {
			objectType, _ := tuple.SplitObject(ctxTuple.Object)
			if objectType != targetObjectType {
				continue
			}
			_, found := uniqueSet[ctxTuple.Object]
			if !found {
				uniqueSet[ctxTuple.Object] = true
				uniqueObjects = append(uniqueObjects, ctxTuple.Object)
			}
		}
	}
	return uniqueObjects, nil
}

func (q *ListObjectsQuery) validateInput(ctx context.Context, storeID string, targetObjectType string, authModelID string, relation string) error {
	definition, err := q.datastore.ReadTypeDefinition(ctx, storeID, authModelID, targetObjectType)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return serverErrors.TypeNotFound(targetObjectType)
		}
		return err
	}
	_, ok := definition.Relations[relation]
	if !ok {
		return serverErrors.UnknownRelationWhenListingObjects(relation, targetObjectType)
	}
	return nil
}

type PerformChecksInput struct {
	storeID     string
	authModelID string
	objectType  string
	relation    string
	user        string
	ctxTuples   *openfgapb.ContextualTupleKeys
}

func (q *ListObjectsQuery) performChecks(timeoutCtx context.Context, input *PerformChecksInput, resultsChan chan<- string, errChan chan<- error, resolvedChan chan<- struct{}) {
	g := new(errgroup.Group)
	g.SetLimit(MaximumConcurrentChecks)
	var objectsFound uint32

	uniqueObjects, err := q.getUniqueObjects(timeoutCtx, input.storeID, input.objectType, input.ctxTuples)
	if err != nil {
		errChan <- err
		close(errChan)
		return
	}

	// iterate over all object IDs in the store and check if the user has relation with each
	for _, object := range uniqueObjects {
		object := object
		if atomic.LoadUint32(&objectsFound) >= q.listObjectsMaxResults {
			break
		}

		checkFunction := func() error {
			_, objectID := tuple.SplitObject(object)
			query := NewCheckQuery(q.datastore, q.tracer, q.meter, q.logger, q.resolveNodeLimit)

			resp, err := query.Execute(timeoutCtx, &openfgapb.CheckRequest{
				StoreId:              input.storeID,
				AuthorizationModelId: input.authModelID,
				TupleKey: &openfgapb.TupleKey{
					Object:   object,
					Relation: input.relation,
					User:     input.user,
				},
				ContextualTuples: input.ctxTuples,
			})
			if err != nil {
				// ignore the error. we don't want to abort everything if one of the checks failed.
				q.logger.Error("Check errored: ", logger.Error(err))
				return nil
			}

			if resp.Allowed && atomic.LoadUint32(&objectsFound) < q.listObjectsMaxResults {
				resultsChan <- objectID
				atomic.AddUint32(&objectsFound, 1)
			}

			return nil
		}

		g.Go(checkFunction)
	}

	err = g.Wait()
	if err != nil {
		errChan <- err
	}

	close(resultsChan)
	close(resolvedChan)
	close(errChan)
}
