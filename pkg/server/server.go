// Package server contains the endpoint handlers.
package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/gateway"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	httpmiddleware "github.com/openfga/openfga/pkg/middleware/http"
	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type ExperimentalFeatureFlag string

const (
	AuthorizationModelIDHeader = "openfga-authorization-model-id"
	authorizationModelIDKey    = "authorization_model_id"

	// same values as run.DefaultConfig() (TODO break the import cycle, remove these hardcoded values and import those constants here)
	defaultChangelogHorizonOffset           = 0
	defaultResolveNodeLimit                 = 25
	defaultResolveNodeBreadthLimit          = 100
	defaultListObjectsDeadline              = 3 * time.Second
	defaultListObjectsMaxResults            = 1000
	defaultMaxConcurrentReadsForCheck       = math.MaxUint32
	defaultMaxConcurrentReadsForListObjects = math.MaxUint32
)

var tracer = otel.Tracer("openfga/pkg/server")

var (
	datastoreQueryCountHistogramName = "datastore_query_count"

	datastoreQueryCountHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            datastoreQueryCountHistogramName,
		Help:                            "The number of database queries required to resolve a query (e.g. Check or ListObjects).",
		Buckets:                         []float64{1, 5, 20, 50, 100, 150, 225, 400, 500, 750, 1000},
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"grpc_service", "grpc_method"})
)

// A Server implements the OpenFGA service backend as both
// a GRPC and HTTP server.
type Server struct {
	openfgav1.UnimplementedOpenFGAServiceServer

	logger                           logger.Logger
	datastore                        storage.OpenFGADatastore
	encoder                          encoder.Encoder
	transport                        gateway.Transport
	resolveNodeLimit                 uint32
	resolveNodeBreadthLimit          uint32
	changelogHorizonOffset           int
	listObjectsDeadline              time.Duration
	listObjectsMaxResults            uint32
	maxConcurrentReadsForListObjects uint32
	maxConcurrentReadsForCheck       uint32
	experimentals                    []ExperimentalFeatureFlag

	typesystemResolver typesystem.TypesystemResolverFunc
}

type OpenFGAServiceV1Option func(s *Server)

func WithDatastore(ds storage.OpenFGADatastore) OpenFGAServiceV1Option {
	return func(s *Server) {
		s.datastore = ds
	}
}

func WithLogger(l logger.Logger) OpenFGAServiceV1Option {
	return func(s *Server) {
		s.logger = l
	}
}

func WithTokenEncoder(encoder encoder.Encoder) OpenFGAServiceV1Option {
	return func(s *Server) {
		s.encoder = encoder
	}
}

func WithTransport(t gateway.Transport) OpenFGAServiceV1Option {
	return func(s *Server) {
		s.transport = t
	}
}

// WithResolveNodeLimit sets a limit on the number of recursive calls that one Check or ListObjects call will allow.
// Thinking of a request as a tree of evaluations, this option controls
// how many levels we will evaluate before throwing an error that the authorization model is too complex.
func WithResolveNodeLimit(limit uint32) OpenFGAServiceV1Option {
	return func(s *Server) {
		s.resolveNodeLimit = limit
	}
}

// WithResolveNodeBreadthLimit sets a limit on the number of goroutines that can be created
// when evaluating a subtree of a Check or ListObjects call.
// Thinking of a Check request as a tree of evaluations, this option controls,
// on a given level of the tree, the maximum number of nodes that can be evaluated concurrently (the breadth).
// If your authorization models are very complex (e.g. one relation is a union of many relations, or one relation
// is deeply nested), or if you have lots of users for (object, relation) pairs,
// you should set this option to be a low number (e.g. 1000)
func WithResolveNodeBreadthLimit(limit uint32) OpenFGAServiceV1Option {
	return func(s *Server) {
		s.resolveNodeBreadthLimit = limit
	}
}

func WithChangelogHorizonOffset(offset int) OpenFGAServiceV1Option {
	return func(s *Server) {
		s.changelogHorizonOffset = offset
	}
}

func WithListObjectsDeadline(deadline time.Duration) OpenFGAServiceV1Option {
	return func(s *Server) {
		s.listObjectsDeadline = deadline
	}
}

func WithListObjectsMaxResults(limit uint32) OpenFGAServiceV1Option {
	return func(s *Server) {
		s.listObjectsMaxResults = limit
	}
}

// WithMaxConcurrentReadsForListObjects sets a limit on the number of datastore reads that can be in flight for a given ListObjects call.
// This number should be set depending on the RPS expected for Check and ListObjects APIs, the number of OpenFGA replicas running,
// and the number of connections the datastore allows.
// E.g. if Datastore.MaxOpenConns = 100 and assuming that each ListObjects call takes 1 second and no traffic to Check API:
// - One OpenFGA replica and expected traffic of 100 RPS => set it to 1.
// - One OpenFGA replica and expected traffic of 1 RPS => set it to 100.
// - Two OpenFGA replicas and expected traffic of 1 RPS => set it to 50.
func WithMaxConcurrentReadsForListObjects(max uint32) OpenFGAServiceV1Option {
	return func(s *Server) {
		s.maxConcurrentReadsForListObjects = max
	}
}

// WithMaxConcurrentReadsForCheck sets a limit on the number of datastore reads that can be in flight for a given Check call.
// This number should be set depending on the RPS expected for Check and ListObjects APIs, the number of OpenFGA replicas running,
// and the number of connections the datastore allows.
// E.g. if Datastore.MaxOpenConns = 100 and assuming that each Check call takes 1 second and no traffic to ListObjects API:
// - One OpenFGA replica and expected traffic of 100 RPS => set it to 1.
// - One OpenFGA replica and expected traffic of 1 RPS => set it to 100.
// - Two OpenFGA replicas and expected traffic of 1 RPS => set it to 50.
func WithMaxConcurrentReadsForCheck(max uint32) OpenFGAServiceV1Option {
	return func(s *Server) {
		s.maxConcurrentReadsForCheck = max
	}
}

func WithExperimentals(experimentals ...ExperimentalFeatureFlag) OpenFGAServiceV1Option {
	return func(s *Server) {
		s.experimentals = experimentals
	}
}

func MustNewServerWithOpts(opts ...OpenFGAServiceV1Option) *Server {
	s, err := NewServerWithOpts(opts...)
	if err != nil {
		panic(fmt.Errorf("failed to construct the OpenFGA server: %w", err))
	}

	return s
}

func NewServerWithOpts(opts ...OpenFGAServiceV1Option) (*Server, error) {

	s := &Server{
		logger:                           logger.NewNoopLogger(),
		encoder:                          encoder.NewBase64Encoder(),
		transport:                        gateway.NewNoopTransport(),
		changelogHorizonOffset:           defaultChangelogHorizonOffset,
		resolveNodeLimit:                 defaultResolveNodeLimit,
		resolveNodeBreadthLimit:          defaultResolveNodeBreadthLimit,
		listObjectsDeadline:              defaultListObjectsDeadline,
		listObjectsMaxResults:            defaultListObjectsMaxResults,
		maxConcurrentReadsForCheck:       defaultMaxConcurrentReadsForCheck,
		maxConcurrentReadsForListObjects: defaultMaxConcurrentReadsForListObjects,
		experimentals:                    make([]ExperimentalFeatureFlag, 0, 10),
	}

	for _, opt := range opts {
		opt(s)
	}

	if s.datastore == nil {
		return nil, fmt.Errorf("a datastore option must be provided")
	}

	s.typesystemResolver = typesystem.MemoizedTypesystemResolverFunc(s.datastore)

	return s, nil
}

func (s *Server) ListObjects(ctx context.Context, req *openfgav1.ListObjectsRequest) (*openfgav1.ListObjectsResponse, error) {

	targetObjectType := req.GetType()

	ctx, span := tracer.Start(ctx, "ListObjects", trace.WithAttributes(
		attribute.String("object_type", targetObjectType),
		attribute.String("relation", req.GetRelation()),
		attribute.String("user", req.GetUser()),
	))
	defer span.End()

	storeID := req.GetStoreId()

	typesys, err := s.resolveTypesystem(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}

	q := commands.NewListObjectsQuery(s.datastore,
		commands.WithLogger(s.logger),
		commands.WithListObjectsDeadline(s.listObjectsDeadline),
		commands.WithListObjectsMaxResults(s.listObjectsMaxResults),
		commands.WithResolveNodeLimit(s.resolveNodeLimit),
		commands.WithResolveNodeBreadthLimit(s.resolveNodeBreadthLimit),
		commands.WithMaxConcurrentReads(s.maxConcurrentReadsForListObjects),
	)

	return q.Execute(
		typesystem.ContextWithTypesystem(ctx, typesys),
		&openfgav1.ListObjectsRequest{
			StoreId:              storeID,
			ContextualTuples:     req.GetContextualTuples(),
			AuthorizationModelId: typesys.GetAuthorizationModelID(), // the resolved model id
			Type:                 targetObjectType,
			Relation:             req.Relation,
			User:                 req.User,
		},
	)
}

func (s *Server) StreamedListObjects(req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) error {
	ctx := srv.Context()
	ctx, span := tracer.Start(ctx, "StreamedListObjects", trace.WithAttributes(
		attribute.String("object_type", req.GetType()),
		attribute.String("relation", req.GetRelation()),
		attribute.String("user", req.GetUser()),
	))
	defer span.End()

	storeID := req.GetStoreId()

	typesys, err := s.resolveTypesystem(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return err
	}

	q := commands.NewListObjectsQuery(s.datastore,
		commands.WithLogger(s.logger),
		commands.WithListObjectsDeadline(s.listObjectsDeadline),
		commands.WithListObjectsMaxResults(s.listObjectsMaxResults),
		commands.WithResolveNodeLimit(s.resolveNodeLimit),
		commands.WithResolveNodeBreadthLimit(s.resolveNodeBreadthLimit),
		commands.WithMaxConcurrentReads(s.maxConcurrentReadsForListObjects),
	)

	req.AuthorizationModelId = typesys.GetAuthorizationModelID() // the resolved model id
	return q.ExecuteStreamed(
		typesystem.ContextWithTypesystem(ctx, typesys),
		req,
		srv,
	)
}

func (s *Server) Read(ctx context.Context, req *openfgav1.ReadRequest) (*openfgav1.ReadResponse, error) {
	tk := req.GetTupleKey()
	ctx, span := tracer.Start(ctx, "Read", trace.WithAttributes(
		attribute.KeyValue{Key: "object", Value: attribute.StringValue(tk.GetObject())},
		attribute.KeyValue{Key: "relation", Value: attribute.StringValue(tk.GetRelation())},
		attribute.KeyValue{Key: "user", Value: attribute.StringValue(tk.GetUser())},
	))
	defer span.End()

	q := commands.NewReadQuery(s.datastore, s.logger, s.encoder)
	return q.Execute(ctx, &openfgav1.ReadRequest{
		StoreId:           req.GetStoreId(),
		TupleKey:          tk,
		PageSize:          req.GetPageSize(),
		ContinuationToken: req.GetContinuationToken(),
	})
}

func (s *Server) Write(ctx context.Context, req *openfgav1.WriteRequest) (*openfgav1.WriteResponse, error) {
	ctx, span := tracer.Start(ctx, "Write")
	defer span.End()

	storeID := req.GetStoreId()

	typesys, err := s.resolveTypesystem(ctx, storeID, req.AuthorizationModelId)
	if err != nil {
		return nil, err
	}

	cmd := commands.NewWriteCommand(s.datastore, s.logger)
	return cmd.Execute(ctx, &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: typesys.GetAuthorizationModelID(), // the resolved model id
		Writes:               req.GetWrites(),
		Deletes:              req.GetDeletes(),
	})
}

func (s *Server) Check(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
	tk := req.GetTupleKey()
	ctx, span := tracer.Start(ctx, "Check", trace.WithAttributes(
		attribute.KeyValue{Key: "object", Value: attribute.StringValue(tk.GetObject())},
		attribute.KeyValue{Key: "relation", Value: attribute.StringValue(tk.GetRelation())},
		attribute.KeyValue{Key: "user", Value: attribute.StringValue(tk.GetUser())},
	))
	defer span.End()

	if tk.GetUser() == "" || tk.GetRelation() == "" || tk.GetObject() == "" {
		return nil, serverErrors.InvalidCheckInput
	}

	storeID := req.GetStoreId()

	typesys, err := s.resolveTypesystem(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}

	if err := validation.ValidateUserObjectRelation(typesys, tk); err != nil {
		return nil, serverErrors.ValidationError(err)
	}

	for _, ctxTuple := range req.GetContextualTuples().GetTupleKeys() {
		if err := validation.ValidateTuple(typesys, ctxTuple); err != nil {
			return nil, serverErrors.HandleTupleValidateError(err)
		}
	}

	ctx = typesystem.ContextWithTypesystem(ctx, typesys)

	checkResolver := graph.NewLocalChecker(
		storagewrappers.NewCombinedTupleReader(s.datastore, req.ContextualTuples.GetTupleKeys()),
		graph.WithResolveNodeBreadthLimit(s.resolveNodeBreadthLimit),
		graph.WithMaxConcurrentReads(s.maxConcurrentReadsForCheck),
	)

	resp, err := checkResolver.ResolveCheck(ctx, &graph.ResolveCheckRequest{
		StoreID:              req.GetStoreId(),
		AuthorizationModelID: typesys.GetAuthorizationModelID(), // the resolved model id
		TupleKey:             req.GetTupleKey(),
		ContextualTuples:     req.ContextualTuples.GetTupleKeys(),
		ResolutionMetadata: &graph.ResolutionMetadata{
			Depth:               s.resolveNodeLimit,
			DatastoreQueryCount: 0,
		},
	})
	if err != nil {
		if errors.Is(err, graph.ErrResolutionDepthExceeded) {
			return nil, serverErrors.AuthorizationModelResolutionTooComplex
		}

		return nil, serverErrors.HandleError("", err)
	}

	queryCount := float64(resp.GetResolutionMetadata().DatastoreQueryCount)

	grpc_ctxtags.Extract(ctx).Set(datastoreQueryCountHistogramName, queryCount)
	span.SetAttributes(attribute.Float64(datastoreQueryCountHistogramName, queryCount))
	datastoreQueryCountHistogram.WithLabelValues(
		openfgav1.OpenFGAService_ServiceDesc.ServiceName,
		"check",
	).Observe(queryCount)

	res := &openfgav1.CheckResponse{
		Allowed: resp.Allowed,
	}

	span.SetAttributes(attribute.KeyValue{Key: "allowed", Value: attribute.BoolValue(res.GetAllowed())})
	return res, nil
}

func (s *Server) Expand(ctx context.Context, req *openfgav1.ExpandRequest) (*openfgav1.ExpandResponse, error) {
	tk := req.GetTupleKey()
	ctx, span := tracer.Start(ctx, "Expand", trace.WithAttributes(
		attribute.KeyValue{Key: "object", Value: attribute.StringValue(tk.GetObject())},
		attribute.KeyValue{Key: "relation", Value: attribute.StringValue(tk.GetRelation())},
		attribute.KeyValue{Key: "user", Value: attribute.StringValue(tk.GetUser())},
	))
	defer span.End()

	storeID := req.GetStoreId()

	typesys, err := s.resolveTypesystem(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}

	q := commands.NewExpandQuery(s.datastore, s.logger)
	return q.Execute(ctx, &openfgav1.ExpandRequest{
		StoreId:              storeID,
		AuthorizationModelId: typesys.GetAuthorizationModelID(), // the resolved model id
		TupleKey:             tk,
	})
}

func (s *Server) ReadAuthorizationModel(ctx context.Context, req *openfgav1.ReadAuthorizationModelRequest) (*openfgav1.ReadAuthorizationModelResponse, error) {
	ctx, span := tracer.Start(ctx, "ReadAuthorizationModel", trace.WithAttributes(
		attribute.KeyValue{Key: authorizationModelIDKey, Value: attribute.StringValue(req.GetId())},
	))
	defer span.End()

	q := commands.NewReadAuthorizationModelQuery(s.datastore, s.logger)
	return q.Execute(ctx, req)
}

func (s *Server) WriteAuthorizationModel(ctx context.Context, req *openfgav1.WriteAuthorizationModelRequest) (*openfgav1.WriteAuthorizationModelResponse, error) {
	ctx, span := tracer.Start(ctx, "WriteAuthorizationModel")
	defer span.End()

	c := commands.NewWriteAuthorizationModelCommand(s.datastore, s.logger)
	res, err := c.Execute(ctx, req)
	if err != nil {
		return nil, err
	}

	s.transport.SetHeader(ctx, httpmiddleware.XHttpCode, strconv.Itoa(http.StatusCreated))

	return res, nil
}

func (s *Server) ReadAuthorizationModels(ctx context.Context, req *openfgav1.ReadAuthorizationModelsRequest) (*openfgav1.ReadAuthorizationModelsResponse, error) {
	ctx, span := tracer.Start(ctx, "ReadAuthorizationModels")
	defer span.End()

	c := commands.NewReadAuthorizationModelsQuery(s.datastore, s.logger, s.encoder)
	return c.Execute(ctx, req)
}

func (s *Server) WriteAssertions(ctx context.Context, req *openfgav1.WriteAssertionsRequest) (*openfgav1.WriteAssertionsResponse, error) {
	ctx, span := tracer.Start(ctx, "WriteAssertions")
	defer span.End()

	storeID := req.GetStoreId()

	typesys, err := s.resolveTypesystem(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}

	c := commands.NewWriteAssertionsCommand(s.datastore, s.logger)
	res, err := c.Execute(ctx, &openfgav1.WriteAssertionsRequest{
		StoreId:              storeID,
		AuthorizationModelId: typesys.GetAuthorizationModelID(), // the resolved model id
		Assertions:           req.GetAssertions(),
	})
	if err != nil {
		return nil, err
	}

	s.transport.SetHeader(ctx, httpmiddleware.XHttpCode, strconv.Itoa(http.StatusNoContent))

	return res, nil
}

func (s *Server) ReadAssertions(ctx context.Context, req *openfgav1.ReadAssertionsRequest) (*openfgav1.ReadAssertionsResponse, error) {
	ctx, span := tracer.Start(ctx, "ReadAssertions")
	defer span.End()

	typesys, err := s.resolveTypesystem(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}

	q := commands.NewReadAssertionsQuery(s.datastore, s.logger)
	return q.Execute(ctx, req.GetStoreId(), typesys.GetAuthorizationModelID())
}

func (s *Server) ReadChanges(ctx context.Context, req *openfgav1.ReadChangesRequest) (*openfgav1.ReadChangesResponse, error) {
	ctx, span := tracer.Start(ctx, "ReadChangesQuery", trace.WithAttributes(
		attribute.KeyValue{Key: "type", Value: attribute.StringValue(req.GetType())},
	))
	defer span.End()

	q := commands.NewReadChangesQuery(s.datastore, s.logger, s.encoder, s.changelogHorizonOffset)
	return q.Execute(ctx, req)
}

func (s *Server) CreateStore(ctx context.Context, req *openfgav1.CreateStoreRequest) (*openfgav1.CreateStoreResponse, error) {
	ctx, span := tracer.Start(ctx, "CreateStore")
	defer span.End()

	c := commands.NewCreateStoreCommand(s.datastore, s.logger)
	res, err := c.Execute(ctx, req)
	if err != nil {
		return nil, err
	}

	s.transport.SetHeader(ctx, httpmiddleware.XHttpCode, strconv.Itoa(http.StatusCreated))

	return res, nil
}

func (s *Server) DeleteStore(ctx context.Context, req *openfgav1.DeleteStoreRequest) (*openfgav1.DeleteStoreResponse, error) {
	ctx, span := tracer.Start(ctx, "DeleteStore")
	defer span.End()

	cmd := commands.NewDeleteStoreCommand(s.datastore, s.logger)
	res, err := cmd.Execute(ctx, req)
	if err != nil {
		return nil, err
	}

	s.transport.SetHeader(ctx, httpmiddleware.XHttpCode, strconv.Itoa(http.StatusNoContent))

	return res, nil
}

func (s *Server) GetStore(ctx context.Context, req *openfgav1.GetStoreRequest) (*openfgav1.GetStoreResponse, error) {
	ctx, span := tracer.Start(ctx, "GetStore")
	defer span.End()

	q := commands.NewGetStoreQuery(s.datastore, s.logger)
	return q.Execute(ctx, req)
}

func (s *Server) ListStores(ctx context.Context, req *openfgav1.ListStoresRequest) (*openfgav1.ListStoresResponse, error) {
	ctx, span := tracer.Start(ctx, "ListStores")
	defer span.End()

	q := commands.NewListStoresQuery(s.datastore, s.logger, s.encoder)
	return q.Execute(ctx, req)
}

// IsReady reports whether this OpenFGA server instance is ready to accept
// traffic.
func (s *Server) IsReady(ctx context.Context) (bool, error) {

	// for now we only depend on the datastore being ready, but in the future
	// server readiness may also depend on other criteria in addition to the
	// datastore being ready.
	return s.datastore.IsReady(ctx)
}

// resolveTypesystem resolves the underlying TypeSystem given the storeID and modelID and
// it sets some response metadata based on the model resolution.
func (s *Server) resolveTypesystem(ctx context.Context, storeID, modelID string) (*typesystem.TypeSystem, error) {
	ctx, span := tracer.Start(ctx, "resolveTypesystem")
	defer span.End()

	typesys, err := s.typesystemResolver(ctx, storeID, modelID)
	if err != nil {
		if errors.Is(err, typesystem.ErrModelNotFound) {
			if modelID == "" {
				return nil, serverErrors.LatestAuthorizationModelNotFound(storeID)
			}

			return nil, serverErrors.AuthorizationModelNotFound(modelID)
		}

		if errors.Is(err, typesystem.ErrInvalidModel) {
			return nil, serverErrors.ValidationError(err)
		}

		return nil, serverErrors.HandleError("", err)
	}

	resolvedModelID := typesys.GetAuthorizationModelID()

	span.SetAttributes(attribute.KeyValue{Key: authorizationModelIDKey, Value: attribute.StringValue(resolvedModelID)})
	grpc_ctxtags.Extract(ctx).Set(authorizationModelIDKey, resolvedModelID)
	_ = grpc.SetHeader(ctx, metadata.Pairs(AuthorizationModelIDHeader, resolvedModelID))

	return typesys, nil
}
