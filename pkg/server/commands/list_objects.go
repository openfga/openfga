package commands

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/openfga/openfga/internal/contextualtuples"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	maximumConcurrentChecks = 100 // todo(jon-whit): make this configurable, but for now limit to 100 concurrent checks
)

type ListObjectsQuery struct {
	Datastore             storage.OpenFGADatastore
	Logger                logger.Logger
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

	if _, err = contextualtuples.New(typesys, req.GetContextualTuples().GetTupleKeys()); err != nil {
		return err
	}

	_, err = typesys.GetRelation(targetObjectType, targetRelation)
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
		err = q.performChecks(ctx, req, resultsChan)
		if err != nil {
			errChan <- err
		}

		close(resultsChan)
	}

	if q.ConnectedObjects != nil {
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
				err = q.ConnectedObjects(ctx, &ConnectedObjectsRequest{
					StoreID:          req.GetStoreId(),
					ObjectType:       targetObjectType,
					Relation:         targetRelation,
					User:             targetUserRef,
					ContextualTuples: req.GetContextualTuples().GetTupleKeys(),
				}, resultsChan)
				if err != nil {
					errChan <- err
				}

				close(resultsChan)
			}
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

	resultsChan := make(chan string, 1)
	if q.ListObjectsMaxResults > 0 {
		resultsChan = make(chan string, q.ListObjectsMaxResults)
	}

	errChan := make(chan error)

	timeoutCtx := ctx
	if q.ListObjectsDeadline != 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, q.ListObjectsDeadline)
		defer cancel()
	}

	err := q.handler(timeoutCtx, req, resultsChan, errChan)
	if err != nil {
		return nil, err
	}

	objects := make([]string, 0)

	for {
		select {
		case objectID, ok := <-resultsChan:
			if !ok {
				return &openfgapb.ListObjectsResponse{
					Objects: objects,
				}, nil
			}
			objects = append(objects, objectID)
		case genericError, ok := <-errChan:
			if ok {
				return nil, serverErrors.NewInternalError("", genericError)
			}
		}
	}
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

	timeoutCtx := ctx
	if q.ListObjectsDeadline != 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, q.ListObjectsDeadline)
		defer cancel()
	}

	err := q.handler(timeoutCtx, req, resultsChan, errChan)
	if err != nil {
		return err
	}

	for {
		select {
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

func (q *ListObjectsQuery) performChecks(ctx context.Context, req listObjectsRequest, resultsChan chan<- string) error {
	var objectsFound = new(uint32)

	iter1 := storage.NewObjectIteratorFromTupleKeyIterator(storage.NewFilteredTupleKeyIterator(
		storage.NewStaticTupleKeyIterator(req.GetContextualTuples().GetTupleKeys()),
		func(tk *openfgapb.TupleKey) bool {
			return tuple.GetType(tk.GetObject()) == req.GetType()
		}))

	iter2, err := q.Datastore.ListObjectsByType(ctx, req.GetStoreId(), req.GetType())
	if err != nil {
		iter1.Stop()
		return err
	}

	// pass contextual tuples iterator (iter1) first to exploit uniqueness optimization
	iter := storage.NewUniqueObjectIterator(iter1, iter2)
	defer iter.Stop()

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(maximumConcurrentChecks)

	// iterate over all object IDs in the store and check if the user has relation with each
	for {
		object, err := iter.Next()
		if err != nil {
			if !errors.Is(err, storage.ErrIteratorDone) {
				return err
			}
			break
		}
		if atomic.LoadUint32(objectsFound) >= q.ListObjectsMaxResults {
			break
		}

		checkFunction := func() error {
			return q.internalCheck(ctx, object, req, objectsFound, resultsChan)
		}

		g.Go(checkFunction)
	}

	return g.Wait()
}

func (q *ListObjectsQuery) internalCheck(
	ctx context.Context,
	obj *openfgapb.Object,
	req listObjectsRequest,
	objectsFound *uint32,
	resultsChan chan<- string,
) error {
	query := NewCheckQuery(q.Datastore, q.Logger, q.ResolveNodeLimit)

	resp, err := query.Execute(ctx, &openfgapb.CheckRequest{
		StoreId:              req.GetStoreId(),
		AuthorizationModelId: req.GetAuthorizationModelId(),
		TupleKey:             tuple.NewTupleKey(tuple.ObjectKey(obj), req.GetRelation(), req.GetUser()),
		ContextualTuples:     req.GetContextualTuples(),
	})
	if err != nil {
		// ignore the error. we don't want to abort everything if one of the checks failed.
		q.Logger.ErrorWithContext(ctx, "check_error", zap.Error(err))
		return nil
	}
	if resp.Allowed && atomic.AddUint32(objectsFound, 1) <= q.ListObjectsMaxResults {
		resultsChan <- tuple.ObjectKey(obj)
	}

	return nil
}
