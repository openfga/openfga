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
	"go.uber.org/zap"
)

const (
	streamedBufferSize      = 100
	maximumConcurrentChecks = 100 // todo(jon-whit): make this configurable, but for now limit to 100 concurrent checks
)

type ListObjectsQuery struct {
	Datastore             storage.OpenFGADatastore
	Logger                logger.Logger
	ListObjectsDeadline   time.Duration
	ListObjectsMaxResults uint32
	ResolveNodeLimit      uint32
	ConnectedObjects      func(ctx context.Context, req *ConnectedObjectsRequest, results chan<- *ConnectedObjectsResult) error
	CheckResolver         graph.CheckResolver
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
	resultsChan chan<- string,
	errChan chan<- error,
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
	defer close(connectedObjectsResChan)

	var objectsFound = new(uint32)

	go func() {
		defer close(errChan)
		defer close(resultsChan)

		for res := range connectedObjectsResChan {

			if res.ResultStatus != RequiresFurtherEvalStatus {
				resultsChan <- res.Object
				continue
			}

			// todo(optimization): fire this Check resolution off concurrently and continue on to the next object in the
			// result channel
			resp, err := q.CheckResolver.ResolveCheck(ctx, &graph.ResolveCheckRequest{
				StoreID:              req.GetStoreId(),
				AuthorizationModelID: req.GetAuthorizationModelId(),
				TupleKey:             tuple.NewTupleKey(res.Object, req.GetRelation(), req.GetUser()),
				ContextualTuples:     req.GetContextualTuples().GetTupleKeys(),
				ResolutionMetadata: &graph.ResolutionMetadata{
					Depth: q.ResolveNodeLimit,
				},
			})
			if err != nil {
				errChan <- err
				return // is this the right thing here or should we try to find all of them we can up to some limit?
			}

			if resp.Allowed && atomic.AddUint32(objectsFound, 1) <= q.ListObjectsMaxResults {
				resultsChan <- res.Object
			}
		}
	}()

	return q.ConnectedObjects(ctx, &ConnectedObjectsRequest{
		StoreID:          req.GetStoreId(),
		Typesystem:       typesys,
		ObjectType:       targetObjectType,
		Relation:         targetRelation,
		User:             sourceUserRef,
		ContextualTuples: req.GetContextualTuples().GetTupleKeys(),
	}, connectedObjectsResChan)
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

			err := srv.Send(&openfgapb.StreamedListObjectsResponse{
				Object: object,
			})
			if err != nil {
				return serverErrors.NewInternalError("", err)
			}
		case genericError, ok := <-errChan:
			if ok {
				return serverErrors.NewInternalError("", genericError)
			}
		}
	}
}
