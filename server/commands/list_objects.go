package commands

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/unit"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
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
	ConnectedObjects      func(ctx context.Context, req *ConnectedObjectsRequest, results chan<- string) error
}

type listObjectsRequest interface {
	GetStoreId() string
	GetAuthorizationModelId() string
	GetType() string
	GetRelation() string
	GetUser() string
	GetContextualTuples() *openfgapb.ContextualTupleKeys
}

func (q *ListObjectsQuery) handler(
	ctx context.Context,
	req listObjectsRequest,
	resolvedChan chan<- struct{},
	resultsChan chan<- string,
	errChan chan<- error,
) error {

	targetObjectType := req.GetType()
	targetRelation := req.GetRelation()

	model, err := q.Datastore.ReadAuthorizationModel(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return serverErrors.AuthorizationModelNotFound(req.GetAuthorizationModelId())
		}
		return err
	}

	typesys := typesystem.New(model)

	hasTypeInfo, err := typesys.HasTypeInfo(targetObjectType, targetRelation)
	if err != nil {
		q.Logger.WarnWithContext(
			ctx, fmt.Sprintf("failed to lookup type info for relation '%s'", targetRelation),
			zap.String("store_id", req.GetStoreId()),
			zap.String("object_type", targetObjectType),
		)
	}

	handler := func() {
		q.performChecks(ctx, req, resultsChan, errChan, resolvedChan)
	}

	_, err = typesys.GetRelation(targetObjectType, targetRelation)
	if err != nil {
		if errors.Is(err, typesystem.ErrObjectTypeUndefined) {
			return serverErrors.TypeNotFound(targetObjectType)
		}

		if errors.Is(err, typesystem.ErrRelationUndefined) {
			return serverErrors.RelationNotFound(targetRelation, targetObjectType, nil)
		}

		return serverErrors.HandleError("", err)
	}

	containsIntersection, _ := typesys.RelationInvolvesIntersection(targetObjectType, targetRelation)
	containsExclusion, _ := typesys.RelationInvolvesExclusion(targetObjectType, targetRelation)

	// ConnectedObjects currently only supports models that do not include intersection and exclusion,
	// and the model must include type info for ConnectedObjects to work.
	if !containsIntersection && !containsExclusion && hasTypeInfo {

		userObj, userRel := tuple.SplitObjectRelation(req.GetUser())

		handler = func() {
			err = q.ConnectedObjects(ctx, &ConnectedObjectsRequest{
				StoreID:          req.GetStoreId(),
				ObjectType:       targetObjectType,
				Relation:         targetRelation,
				User:             &openfgapb.ObjectRelation{Object: userObj, Relation: userRel},
				ContextualTuples: req.GetContextualTuples().GetTupleKeys(),
			}, resultsChan)
			if err != nil {
				errChan <- err
				return
			}

			close(resolvedChan)
			close(resultsChan)
		}
	}

	go handler()

	return nil
}

// Execute the ListObjectsQuery, returning a list of object IDs
func (q *ListObjectsQuery) Execute(
	ctx context.Context,
	req *openfgapb.ListObjectsRequest,
) (*openfgapb.ListObjectsResponse, error) {

	listObjectsGauge, err := q.Meter.AsyncInt64().Gauge(
		"openfga.listObjects.results",
		instrument.WithDescription("Number of results returned by ListObjects"),
		instrument.WithUnit(unit.Dimensionless),
	)
	if err != nil {
		return nil, serverErrors.NewInternalError("", err)
	}

	resultsChan := make(chan string, 1)
	if q.ListObjectsMaxResults > 0 {
		resultsChan = make(chan string, q.ListObjectsMaxResults)
	}

	errChan := make(chan error)
	resolvedChan := make(chan struct{})

	timeoutCtx := ctx
	if q.ListObjectsDeadline != 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, q.ListObjectsDeadline)
		defer cancel()
	}

	err = q.handler(timeoutCtx, req, resolvedChan, resultsChan, errChan)
	if err != nil {
		return nil, err
	}

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

func (q *ListObjectsQuery) ExecuteStreamed(
	ctx context.Context,
	req *openfgapb.StreamedListObjectsRequest,
	srv openfgapb.OpenFGAService_StreamedListObjectsServer,
) error {

	resultsChan := make(chan string, 1)
	if q.ListObjectsMaxResults > 0 {
		resultsChan = make(chan string, q.ListObjectsMaxResults)
	}

	errChan := make(chan error)
	resolvedChan := make(chan struct{})

	timeoutCtx := ctx
	if q.ListObjectsDeadline != 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, q.ListObjectsDeadline)
		defer cancel()
	}

	err := q.handler(timeoutCtx, req, resolvedChan, resultsChan, errChan)
	if err != nil {
		return err
	}

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

func (q *ListObjectsQuery) performChecks(
	ctx context.Context,
	req listObjectsRequest,
	resultsChan chan<- string,
	errChan chan<- error,
	resolvedChan chan<- struct{},
) {
	iter1 := storage.NewObjectIteratorFromTupleKeyIterator(storage.NewFilteredTupleKeyIterator(
		storage.NewStaticTupleKeyIterator(req.GetContextualTuples().GetTupleKeys()),
		func(tk *openfgapb.TupleKey) bool {
			return tuple.GetType(tk.GetObject()) == req.GetType()
		}))

	iter2, err := q.Datastore.ListObjectsByType(ctx, req.GetStoreId(), req.GetType())
	if err != nil {
		iter1.Stop()
		errChan <- err
		return
	}

	// pass contextual tuples iterator (iter1) first to exploit uniqueness optimization
	iter := storage.NewUniqueObjectIterator(iter1, iter2)
	defer iter.Stop()

	g := new(errgroup.Group)
	g.SetLimit(maximumConcurrentChecks)
	var objectsFound = new(uint32)

	// iterate over all object IDs in the store and check if the user has relation with each
	for {
		object, err := iter.Next()
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
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
			return q.internalCheck(ctx, object, req, objectsFound, resultsChan)
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

func (q *ListObjectsQuery) internalCheck(
	ctx context.Context,
	obj *openfgapb.Object,
	req listObjectsRequest,
	objectsFound *uint32,
	resultsChan chan<- string,
) error {
	query := NewCheckQuery(q.Datastore, q.Tracer, q.Meter, q.Logger, q.ResolveNodeLimit)

	resp, err := query.Execute(ctx, &openfgapb.CheckRequest{
		StoreId:              req.GetStoreId(),
		AuthorizationModelId: req.GetAuthorizationModelId(),
		TupleKey:             tuple.NewTupleKey(tuple.ObjectKey(obj), req.GetRelation(), req.GetUser()),
		ContextualTuples:     req.GetContextualTuples(),
	})
	if err != nil {
		// ignore the error. we don't want to abort everything if one of the checks failed.
		q.Logger.ErrorWithContext(ctx, "check_error", logger.Error(err))
		return nil
	}
	if resp.Allowed && atomic.AddUint32(objectsFound, 1) <= q.ListObjectsMaxResults {
		resultsChan <- tuple.ObjectKey(obj)
	}

	return nil
}
