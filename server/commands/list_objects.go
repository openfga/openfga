package commands

import (
	"context"
	"sort"
	"sync/atomic"
	"time"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/utils"
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
	datastore             storage.OpenFGADatastore
	logger                logger.Logger
	tracer                trace.Tracer
	meter                 metric.Meter
	ListObjectsDeadline   time.Duration
	ListObjectsMaxResults uint32
	ResolveNodeLimit      uint32
}

// NewListObjectsQuery creates a ListObjectsQuery
func NewListObjectsQuery(datastore storage.OpenFGADatastore, tracer trace.Tracer, logger logger.Logger, meter metric.Meter, listObjectsDeadline time.Duration, listObjectsMaxResults uint32, ResolveNodeLimit uint32) *ListObjectsQuery {
	return &ListObjectsQuery{
		datastore:             datastore,
		logger:                logger,
		tracer:                tracer,
		meter:                 meter,
		ListObjectsDeadline:   listObjectsDeadline,
		ListObjectsMaxResults: listObjectsMaxResults,
		ResolveNodeLimit:      ResolveNodeLimit,
	}
}

// Execute the ListObjectsQuery, returning a list of object IDs
func (q *ListObjectsQuery) Execute(ctx context.Context, req *openfgapb.ListObjectsRequest) (*openfgapb.ListObjectsResponse, error) {
	iter, err := q.getTupleIterator(ctx, req.StoreId, req.Type, req.AuthorizationModelId, req.Relation)
	if err != nil {
		return nil, err
	}
	defer iter.Stop()

	g := new(errgroup.Group)
	g.SetLimit(MaximumConcurrentChecks)

	uniqueObjectIdsSet := utils.NewConcurrentSet()
	errChan := make(chan error)
	resolvedChan := make(chan struct{})

	timeoutCtx := ctx
	var cancel context.CancelFunc
	if q.ListObjectsDeadline != 0 {
		timeoutCtx, cancel = context.WithTimeout(ctx, q.ListObjectsDeadline)
		defer cancel()
	}

	objectIDs := make([]string, 0)

	go func() {
		var objectsFound uint32
		for {
			if atomic.LoadUint32(&objectsFound) >= q.ListObjectsMaxResults {
				break
			}

			t, err := iter.Next()
			if err != nil {
				if err == storage.TupleIteratorDone {
					break
				} else {
					errChan <- err
				}
			}
			checkFunction := func() error {
				_, objectID := tuple.SplitObject(t.Key.Object)
				if uniqueObjectIdsSet.Has(objectID) {
					return nil
				}
				query := NewCheckQuery(q.datastore, q.tracer, q.meter, q.logger, q.ResolveNodeLimit)

				resp, err := query.Execute(timeoutCtx, &openfgapb.CheckRequest{
					StoreId:              req.StoreId,
					AuthorizationModelId: req.AuthorizationModelId,
					TupleKey: &openfgapb.TupleKey{
						Object:   t.Key.Object,
						Relation: req.GetRelation(),
						User:     req.GetUser(),
					},
					ContextualTuples: req.GetContextualTuples(),
				})
				if err != nil {
					return err
				}

				if resp.Allowed && !uniqueObjectIdsSet.Has(objectID) && atomic.LoadUint32(&objectsFound) < q.ListObjectsMaxResults {
					uniqueObjectIdsSet.Add(objectID)
					atomic.AddUint32(&objectsFound, 1)
				}

				return nil
			}

			g.Go(checkFunction)
		}

		err = g.Wait()
		if err != nil {
			// we don't want to abort everything if one of the checks failed.
			q.logger.Warn("ListObjects errored: ", logger.Error(err))
		}

		for objectID := range uniqueObjectIdsSet.M {
			objectIDs = append(objectIDs, objectID)
		}

		sort.Slice(objectIDs, func(i, j int) bool {
			return objectIDs[i] < objectIDs[j]
		})

		close(resolvedChan)
		close(errChan)
	}()

	select {
	case <-timeoutCtx.Done():
		return nil, serverErrors.NewInternalError("Timeout exceeded", timeoutCtx.Err())
	case <-resolvedChan:
		return &openfgapb.ListObjectsResponse{
			ObjectIds: objectIDs,
		}, nil
	case genericError := <-errChan:
		return nil, genericError
	}
}

func (q *ListObjectsQuery) ExecuteStreamed(ctx context.Context, req *openfgapb.StreamedListObjectsRequest, srv openfgapb.OpenFGAService_StreamedListObjectsServer) error {
	iter, err := q.getTupleIterator(ctx, req.StoreId, req.Type, req.AuthorizationModelId, req.Relation)
	if err != nil {
		return err
	}
	defer iter.Stop()

	g := new(errgroup.Group)
	g.SetLimit(MaximumConcurrentChecks)

	uniqueObjectIdsSet := utils.NewConcurrentSet()
	errChan := make(chan error)
	resolvedChan := make(chan struct{}, MaximumConcurrentChecks)

	var timeoutCtx context.Context
	var cancel context.CancelFunc
	if q.ListObjectsDeadline != 0 {
		timeoutCtx, cancel = context.WithTimeout(ctx, q.ListObjectsDeadline)
		defer cancel()
	}

	go func() {
		var objectsFound uint32
		for {
			if atomic.LoadUint32(&objectsFound) >= q.ListObjectsMaxResults {
				break
			}

			t, err := iter.Next()
			if err != nil {
				if err == storage.TupleIteratorDone {
					break
				} else {
					errChan <- err

				}
			}

			fn := func() error {
				_, objectID := tuple.SplitObject(t.Key.Object)
				if uniqueObjectIdsSet.Has(objectID) {
					return nil
				}

				query := NewCheckQuery(q.datastore, q.tracer, q.meter, q.logger, q.ResolveNodeLimit)

				resp, err := query.Execute(timeoutCtx, &openfgapb.CheckRequest{
					StoreId:              req.StoreId,
					AuthorizationModelId: req.AuthorizationModelId,
					TupleKey: &openfgapb.TupleKey{
						Object:   t.Key.Object,
						Relation: req.GetRelation(),
						User:     req.GetUser(),
					},
					ContextualTuples: req.GetContextualTuples(),
				})
				if err != nil {
					return err
				}

				if resp.Allowed && !uniqueObjectIdsSet.Has(objectID) && atomic.LoadUint32(&objectsFound) < q.ListObjectsMaxResults {
					err = srv.Send(&openfgapb.StreamedListObjectsResponse{
						ObjectId: objectID,
					})
					if err != nil {
						return err
					}

					atomic.AddUint32(&objectsFound, 1)
				}

				return nil
			}

			g.Go(fn)
		}

		err := g.Wait()
		if err != nil {
			// we don't want to abort everything if one of the checks failed.
			q.logger.Warn("ListObjects errored: ", logger.Error(err))
		}

		resolvedChan <- struct{}{}
		close(errChan)
	}()

	select {
	case <-timeoutCtx.Done():
	case <-resolvedChan:
	case genericError := <-errChan:
		return genericError
	}

	return nil
}

func (q *ListObjectsQuery) getTupleIterator(ctx context.Context, storeID, targetObjectType, authModelID, relation string) (storage.TupleIterator, error) {
	readAuthModelQuery := NewReadAuthorizationModelQuery(q.datastore, q.logger)
	res, err := readAuthModelQuery.Execute(ctx, &openfgapb.ReadAuthorizationModelRequest{Id: authModelID, StoreId: storeID})
	if err != nil {
		return nil, err
	}
	foundType := false
	for _, typeDef := range res.AuthorizationModel.TypeDefinitions {
		if typeDef.Type == targetObjectType {
			foundType = true
			_, ok := typeDef.Relations[relation]
			if !ok {
				return nil, serverErrors.UnknownRelationWhenListingObjects(relation, targetObjectType)
			}
		}
	}

	if !foundType {
		return nil, serverErrors.TypeNotFound(targetObjectType)
	}

	iter, err := q.datastore.ReadRelationshipTuples(ctx, storage.ReadRelationshipTuplesFilter{
		StoreID:            storeID,
		OptionalObjectType: targetObjectType,
	})
	if err != nil {
		return nil, serverErrors.NewInternalError("", err)
	}
	return iter, nil
}
