package commands

import (
	"context"
	"sort"
	"sync/atomic"
	"time"

	"github.com/go-errors/errors"
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
	iter, err := q.getTupleIterator(ctx, req.StoreId, req.Type)
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
	if q.listObjectsDeadlineInSeconds != 0 {
		timeoutCtx, cancel = context.WithTimeout(ctx, time.Second*time.Duration(q.listObjectsDeadlineInSeconds))
		defer cancel()
	}

	objectIDs := make([]string, 0)

	go func() {
		var objectsFound uint32

		// iterate over all object IDs in the store and check if the user has relation with each
		for {
			if atomic.LoadUint32(&objectsFound) >= q.listObjectsMaxResults {
				break
			}

			t, err := iter.Next()
			if err != nil {
				if err == storage.TupleIteratorDone {
					break
				} else {
					errChan <- err
					continue
				}
			}
			checkFunction := func() error {
				_, objectID := tuple.SplitObject(t.Key.Object)
				if uniqueObjectIdsSet.Has(objectID) {
					return nil
				}
				query := NewCheckQuery(q.datastore, q.tracer, q.meter, q.logger, q.resolveNodeLimit)

				resp, err := query.Execute(timeoutCtx, &openfgapb.CheckRequest{
					StoreId:              req.StoreId,
					AuthorizationModelId: req.AuthorizationModelId,
					TupleKey: &openfgapb.TupleKey{
						Object:   t.Key.Object,
						Relation: req.GetRelation(),
						User:     req.GetUser(),
					},
				})
				if err != nil {
					return err
				}

				if resp.Allowed && !uniqueObjectIdsSet.Has(objectID) && atomic.LoadUint32(&objectsFound) < q.listObjectsMaxResults {
					uniqueObjectIdsSet.Add(objectID)
					atomic.AddUint32(&objectsFound, 1)
				}

				return nil
			}

			g.Go(checkFunction)
		}

		// iterate over all tuples in the contextual tuples input
		if req.ContextualTuples != nil {
			for _, t := range req.ContextualTuples.TupleKeys {
				t := t
				checkFunction := func() error {
					_, objectID := tuple.SplitObject(t.Object)
					if uniqueObjectIdsSet.Has(objectID) {
						return nil
					}
					query := NewCheckQuery(q.datastore, q.tracer, q.meter, q.logger, q.resolveNodeLimit)

					resp, err := query.Execute(timeoutCtx, &openfgapb.CheckRequest{
						StoreId:              req.StoreId,
						AuthorizationModelId: req.AuthorizationModelId,
						TupleKey: &openfgapb.TupleKey{
							Object:   t.Object,
							Relation: req.GetRelation(),
							User:     req.GetUser(),
						},
						ContextualTuples: req.ContextualTuples,
					})
					if err != nil {
						return err
					}

					if resp.Allowed && !uniqueObjectIdsSet.Has(objectID) && atomic.LoadUint32(&objectsFound) < q.listObjectsMaxResults {
						uniqueObjectIdsSet.Add(objectID)
						atomic.AddUint32(&objectsFound, 1)
					}

					return nil
				}

				g.Go(checkFunction)
			}
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
	err := q.validateInput(ctx, req.StoreId, req.Type, req.AuthorizationModelId, req.Relation)
	if err != nil {
		return err
	}
	iter, err := q.getTupleIterator(ctx, req.StoreId, req.Type)
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
	if q.listObjectsDeadlineInSeconds != 0 {
		timeoutCtx, cancel = context.WithTimeout(ctx, time.Second*time.Duration(q.listObjectsDeadlineInSeconds))
		defer cancel()
	}

	go func() {
		var objectsFound uint32

		// iterate over all object IDs in the store and check if the user has relation with each
		for {
			if atomic.LoadUint32(&objectsFound) >= q.listObjectsMaxResults {
				break
			}

			t, err := iter.Next()
			if err != nil {
				if err == storage.TupleIteratorDone {
					break
				} else {
					errChan <- err
					continue
				}
			}

			fn := func() error {
				_, objectID := tuple.SplitObject(t.Key.Object)
				if uniqueObjectIdsSet.Has(objectID) {
					return nil
				}

				query := NewCheckQuery(q.datastore, q.tracer, q.meter, q.logger, q.resolveNodeLimit)

				resp, err := query.Execute(timeoutCtx, &openfgapb.CheckRequest{
					StoreId:              req.StoreId,
					AuthorizationModelId: req.AuthorizationModelId,
					TupleKey: &openfgapb.TupleKey{
						Object:   t.Key.Object,
						Relation: req.GetRelation(),
						User:     req.GetUser(),
					},
				})
				if err != nil {
					return err
				}

				if resp.Allowed && !uniqueObjectIdsSet.Has(objectID) && atomic.LoadUint32(&objectsFound) < q.listObjectsMaxResults {
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

		// iterate over all tuples in the contextual tuples input
		if req.ContextualTuples != nil {
			for _, t := range req.ContextualTuples.TupleKeys {
				if atomic.LoadUint32(&objectsFound) >= q.listObjectsMaxResults {
					break
				}

				t := t
				checkFunction := func() error {
					_, objectID := tuple.SplitObject(t.Object)
					if uniqueObjectIdsSet.Has(objectID) {
						return nil
					}
					query := NewCheckQuery(q.datastore, q.tracer, q.meter, q.logger, q.resolveNodeLimit)

					resp, err := query.Execute(timeoutCtx, &openfgapb.CheckRequest{
						StoreId:              req.StoreId,
						AuthorizationModelId: req.AuthorizationModelId,
						TupleKey: &openfgapb.TupleKey{
							Object:   t.Object,
							Relation: req.GetRelation(),
							User:     req.GetUser(),
						},
						ContextualTuples: req.ContextualTuples,
					})
					if err != nil {
						return err
					}

					if resp.Allowed && !uniqueObjectIdsSet.Has(objectID) && atomic.LoadUint32(&objectsFound) < q.listObjectsMaxResults {
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

				g.Go(checkFunction)
			}
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

func (q *ListObjectsQuery) getTupleIterator(ctx context.Context, storeID, targetObjectType string) (storage.TupleIterator, error) {
	iter, err := q.datastore.ReadRelationshipTuples(ctx, storage.ReadRelationshipTuplesFilter{
		StoreID:            storeID,
		OptionalObjectType: targetObjectType,
	})
	if err != nil {
		return nil, serverErrors.NewInternalError("", err)
	}
	return iter, nil
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
