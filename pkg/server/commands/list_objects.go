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
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.uber.org/zap"
)

const (
	streamedBufferSize      = 100
	maximumConcurrentChecks = 100 // todo(jon-whit): make this configurable, but for now limit to 100 concurrent checks
	defaultResolveNodeLimit = 25
	defaultMaxResults       = 1000
	defaultDeadline         = 3 * time.Second
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
	datastore             storage.RelationshipTupleReader
	logger                logger.Logger
	listObjectsDeadline   time.Duration
	listObjectsMaxResults uint32
	resolveNodeLimit      uint32
	checkConcurrencyLimit uint32
}

type ListOptionsQueryOption func(d *ListObjectsQuery)

func WithCheckConcurrencyLimit(limit uint32) ListOptionsQueryOption {
	return func(d *ListObjectsQuery) {
		d.checkConcurrencyLimit = limit
	}
}

func WithListObjectsDeadline(deadline time.Duration) ListOptionsQueryOption {
	return func(d *ListObjectsQuery) {
		d.listObjectsDeadline = deadline
	}
}

func WithListObjectsMaxResults(max uint32) ListOptionsQueryOption {
	return func(d *ListObjectsQuery) {
		d.listObjectsMaxResults = max
	}
}

func WithResolveNodeLimit(limit uint32) ListOptionsQueryOption {
	return func(d *ListObjectsQuery) {
		d.resolveNodeLimit = limit
	}
}

func WithLogger(l logger.Logger) ListOptionsQueryOption {
	return func(d *ListObjectsQuery) {
		d.logger = l
	}
}

func NewListObjectsQuery(ds storage.RelationshipTupleReader, opts ...ListOptionsQueryOption) *ListObjectsQuery {
	query := &ListObjectsQuery{
		datastore:             ds,
		logger:                logger.NewNoopLogger(),
		listObjectsDeadline:   defaultDeadline,
		listObjectsMaxResults: defaultMaxResults,
		resolveNodeLimit:      defaultResolveNodeLimit,
		checkConcurrencyLimit: maximumConcurrentChecks,
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

type ListObjectsRequest struct {
	Datastore            storage.RelationshipTupleReader
	StoreID              string
	AuthorizationModelID string
	Type                 string
	Relation             string
	User                 string
	ContextualTuples     []*openfgapb.TupleKey
}

func (q *ListObjectsQuery) execute(
	ctx context.Context,
	req *ListObjectsRequest,
	resultsChan chan<- ListObjectsResult,
	maxResults uint32,
) error {

	targetObjectType := req.Type
	targetRelation := req.Relation

	typesys, ok := typesystem.TypesystemFromContext(ctx)
	if !ok {
		panic("typesystem missing in context")
	}

	if !typesystem.IsSchemaVersionSupported(typesys.GetSchemaVersion()) {
		return serverErrors.ValidationError(typesystem.ErrInvalidSchemaVersion)
	}

	for _, ctxTuple := range req.ContextualTuples {
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

	if err := validation.ValidateUser(typesys, req.User); err != nil {
		return serverErrors.ValidationError(fmt.Errorf("invalid 'user' value: %s", err))
	}

	handler := func() {
		userObj, userRel := tuple.SplitObjectRelation(req.User)
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
			Datastore:        q.datastore,
			Typesystem:       typesys,
			ResolveNodeLimit: q.resolveNodeLimit,
			Limit:            maxResults,
		}

		go func() {
			err = connectedObjectsCmd.Execute(ctx, &ConnectedObjectsRequest{
				StoreID:          req.StoreID,
				ObjectType:       targetObjectType,
				Relation:         targetRelation,
				User:             sourceUserRef,
				ContextualTuples: req.ContextualTuples,
			}, connectedObjectsResChan)
			if err != nil {
				resultsChan <- ListObjectsResult{Err: err}
			}

			close(connectedObjectsResChan)
		}()

		limitedTupleReader := storagewrappers.NewBoundedConcurrencyTupleReader(req.Datastore, q.checkConcurrencyLimit)

		checkResolver := graph.NewLocalChecker(limitedTupleReader,
			graph.WithConcurrencyLimit(q.checkConcurrencyLimit),
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

				resp, err := checkResolver.Execute(ctx, &graph.ResolveCheckRequest{
					StoreID:              req.StoreID,
					AuthorizationModelID: req.AuthorizationModelID,
					TupleKey:             tuple.NewTupleKey(res.Object, req.Relation, req.User),
					ContextualTuples:     req.ContextualTuples,
					ResolutionMetadata: &graph.ResolutionMetadata{
						Depth: q.resolveNodeLimit,
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

	go handler()

	return nil
}

// Execute the ListObjectsQuery, returning a list of object IDs up to a maximum of q.listObjectsMaxResults
// or until q.listObjectsDeadline is hit, whichever happens first.
func (q *ListObjectsQuery) Execute(
	ctx context.Context,
	req *openfgapb.ListObjectsRequest,
) (*openfgapb.ListObjectsResponse, error) {

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

	err := q.execute(timeoutCtx, &ListObjectsRequest{
		StoreID:              req.StoreId,
		AuthorizationModelID: req.AuthorizationModelId,
		Type:                 req.Type,
		Relation:             req.Relation,
		User:                 req.User,
		ContextualTuples:     req.ContextualTuples.GetTupleKeys(),
	}, resultsChan, maxResults)
	if err != nil {
		return nil, err
	}

	objects := make([]string, 0)

	for {
		select {

		case <-timeoutCtx.Done():
			q.logger.WarnWithContext(
				ctx, "list objects timeout with list object configuration timeout",
				zap.String("timeout duration", q.listObjectsDeadline.String()),
			)
			return &openfgapb.ListObjectsResponse{
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
				return &openfgapb.ListObjectsResponse{
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
	req *openfgapb.StreamedListObjectsRequest,
	srv openfgapb.OpenFGAService_StreamedListObjectsServer,
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

	err := q.execute(timeoutCtx, &ListObjectsRequest{
		StoreID:              req.StoreId,
		AuthorizationModelID: req.AuthorizationModelId,
		Type:                 req.Type,
		Relation:             req.Relation,
		User:                 req.User,
		ContextualTuples:     req.ContextualTuples.GetTupleKeys(),
	}, resultsChan, maxResults)
	if err != nil {
		return err
	}

	for {
		select {

		case <-timeoutCtx.Done():
			q.logger.WarnWithContext(
				ctx, "list objects timeout with list object configuration timeout",
				zap.String("timeout duration", q.listObjectsDeadline.String()),
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

			if err := srv.Send(&openfgapb.StreamedListObjectsResponse{
				Object: result.ObjectID,
			}); err != nil {
				return serverErrors.NewInternalError("", err)
			}
		}
	}
}
