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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/unit"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

const (
	maximumConcurrentChecks = 100 // todo(jon-whit): make this configurable, but for now limit to 100 concurrent checks
)

type ListObjectsQuery struct {
	Datastore             storage.OpenFGADatastore
	Logger                logger.Logger
	Tracer                trace.Tracer
	Meter                 metric.Meter
	ListObjectsDeadline   time.Duration
	ListObjectsMaxResults uint32
	ResolveNodeLimit      uint32
}

// Execute the ListObjectsQuery, returning a list of object IDs
func (q *ListObjectsQuery) Execute(ctx context.Context, req *openfgapb.ListObjectsRequest) (*openfgapb.ListObjectsResponse, error) {
	err := q.validateInput(ctx, req.StoreId, req.Type, req.AuthorizationModelId, req.Relation)
	if err != nil {
		return nil, err
	}

	listObjectsGauge, err := q.Meter.AsyncInt64().Gauge(
		"openfga.listObjects.results",
		instrument.WithDescription("Number of results returned by ListObjects"),
		instrument.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		return nil, serverErrors.NewInternalError("", err)
	}

	resultsChan := make(chan string, q.ListObjectsMaxResults)
	errChan := make(chan error)
	resolvedChan := make(chan struct{})

	timeoutCtx := ctx
	if q.ListObjectsDeadline != 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, q.ListObjectsDeadline)
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

	attributes := make([]attribute.KeyValue, 1)

	select {
	case <-timeoutCtx.Done():
		attributes = append(attributes, attribute.Bool("complete_results", false))
	case <-resolvedChan:
		attributes = append(attributes, attribute.Bool("complete_results", true))
	case genericError := <-errChan:
		return nil, genericError
	}

	objectIDs := make([]string, 0)
	for objectID := range resultsChan {
		objectIDs = append(objectIDs, objectID)
	}

	listObjectsGauge.Observe(ctx, int64(len(objectIDs)), attributes...)

	return &openfgapb.ListObjectsResponse{
		ObjectIds: objectIDs,
	}, nil
}

func (q *ListObjectsQuery) ExecuteStreamed(ctx context.Context, req *openfgapb.StreamedListObjectsRequest, srv openfgapb.OpenFGAService_StreamedListObjectsServer) error {
	err := q.validateInput(ctx, req.StoreId, req.Type, req.AuthorizationModelId, req.Relation)
	if err != nil {
		return err
	}

	resultsChan := make(chan string, q.ListObjectsMaxResults)
	errChan := make(chan error)
	resolvedChan := make(chan struct{})

	timeoutCtx := ctx
	if q.ListObjectsDeadline != 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, q.ListObjectsDeadline)
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

func (q *ListObjectsQuery) validateInput(ctx context.Context, storeID string, targetObjectType string, authModelID string, relation string) error {
	definition, err := q.Datastore.ReadTypeDefinition(ctx, storeID, authModelID, targetObjectType)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return serverErrors.TypeNotFound(targetObjectType)
		}
		return err
	}
	_, ok := definition.Relations[relation]
	if !ok {
		return serverErrors.RelationNotFound(relation, targetObjectType, nil)
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
	g.SetLimit(maximumConcurrentChecks)
	var objectsFound = new(uint32)

	iter, err := q.Datastore.ListObjectsByType(timeoutCtx, input.storeID, input.objectType)
	if err != nil {
		errChan <- err
		return
	}
	defer iter.Stop()

	// iterate over the contextual tuples
	if input.ctxTuples != nil {
		for _, t := range input.ctxTuples.TupleKeys {
			if atomic.LoadUint32(objectsFound) >= q.ListObjectsMaxResults {
				break
			}
			t := t
			checkFunction := func() error {
				return q.internalCheck(timeoutCtx, t.Object, input, objectsFound, resultsChan)
			}

			g.Go(checkFunction)
		}
	}

	// iterate over all object IDs in the store and check if the user has relation with each
	for {
		object, err := iter.Next()
		if err != nil {
			if errors.Is(err, storage.ErrObjectIteratorDone) {
				break
			} else {
				errChan <- err
				return
			}
		}
		if atomic.LoadUint32(objectsFound) >= q.ListObjectsMaxResults {
			break
		}

		checkFunction := func() error {
			return q.internalCheck(timeoutCtx, tuple.BuildObject(object.Type, object.Id), input, objectsFound, resultsChan)
		}

		g.Go(checkFunction)
	}

	err = g.Wait()
	if err != nil {
		errChan <- err
	}

	close(resultsChan)
	close(resolvedChan)
}

func (q *ListObjectsQuery) internalCheck(ctx context.Context, object string, input *PerformChecksInput, objectsFound *uint32, resultsChan chan<- string) error {
	_, objectID := tuple.SplitObject(object)
	query := NewCheckQuery(q.Datastore, q.Tracer, q.Meter, q.Logger, q.ResolveNodeLimit)

	resp, err := query.Execute(ctx, &openfgapb.CheckRequest{
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
		q.Logger.ErrorWithContext(ctx, "check_error", logger.Error(err))
		return nil
	}

	if resp.Allowed && atomic.LoadUint32(objectsFound) < q.ListObjectsMaxResults {
		resultsChan <- objectID
		atomic.AddUint32(objectsFound, 1)
	}

	return nil
}
