package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/openfga/openfga/internal/gateway"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/middleware"
	httpmiddleware "github.com/openfga/openfga/internal/middleware/http"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type ExperimentalFeatureFlag string

const (
	AuthorizationModelIDHeader   = "openfga-authorization-model-id"
	AuthorizationModelIDTraceTag = "authorization_model_id"

	checkConcurrencyLimit = 100
)

var tracer = otel.Tracer("openfga/pkg/server")

// A Server implements the OpenFGA service backend as both
// a GRPC and HTTP server.
type Server struct {
	openfgapb.UnimplementedOpenFGAServiceServer

	logger    logger.Logger
	datastore storage.OpenFGADatastore
	encoder   encoder.Encoder
	transport gateway.Transport
	config    *Config

	checkResolver graph.CheckResolver
}

type Dependencies struct {
	Datastore    storage.OpenFGADatastore
	Logger       logger.Logger
	Transport    gateway.Transport
	TokenEncoder encoder.Encoder
}

type Config struct {
	ResolveNodeLimit       uint32
	ChangelogHorizonOffset int
	ListObjectsDeadline    time.Duration
	ListObjectsMaxResults  uint32
	Experimentals          []ExperimentalFeatureFlag
}

// New creates a new Server which uses the supplied backends
// for managing data.
func New(dependencies *Dependencies, config *Config) *Server {

	ds := dependencies.Datastore

	return &Server{
		logger:        dependencies.Logger,
		datastore:     dependencies.Datastore,
		encoder:       dependencies.TokenEncoder,
		transport:     dependencies.Transport,
		config:        config,
		checkResolver: graph.NewLocalChecker(storage.NewContextualTupleDatastore(ds), checkConcurrencyLimit),
	}
}

func (s *Server) ListObjects(ctx context.Context, req *openfgapb.ListObjectsRequest) (*openfgapb.ListObjectsResponse, error) {
	storeID := req.GetStoreId()
	targetObjectType := req.GetType()

	ctx, span := tracer.Start(ctx, "ListObjects", trace.WithAttributes(
		attribute.String("object_type", targetObjectType),
		attribute.String("relation", req.GetRelation()),
		attribute.String("user", req.GetUser()),
	))
	defer span.End()

	modelID, ok := middleware.ModelIDFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("no model id")
	}

	model, err := s.datastore.ReadAuthorizationModel(ctx, storeID, modelID)
	if err != nil {
		return nil, serverErrors.AuthorizationModelNotFound(modelID)
	}

	typesys := typesystem.New(model)

	q := &commands.ListObjectsQuery{
		Datastore:             s.datastore,
		Logger:                s.logger,
		ListObjectsDeadline:   s.config.ListObjectsDeadline,
		ListObjectsMaxResults: s.config.ListObjectsMaxResults,
		ResolveNodeLimit:      s.config.ResolveNodeLimit,
		CheckResolver:         s.checkResolver,
	}

	connectObjCmd := &commands.ConnectedObjectsCommand{
		Datastore:        s.datastore,
		Typesystem:       typesys,
		ResolveNodeLimit: s.config.ResolveNodeLimit,
		Limit:            s.config.ListObjectsMaxResults,
	}

	q.ConnectedObjects = connectObjCmd.StreamedConnectedObjects

	return q.Execute(ctx, &openfgapb.ListObjectsRequest{
		StoreId:              storeID,
		ContextualTuples:     req.GetContextualTuples(),
		AuthorizationModelId: modelID,
		Type:                 targetObjectType,
		Relation:             req.Relation,
		User:                 req.User,
	})
}

func (s *Server) StreamedListObjects(req *openfgapb.StreamedListObjectsRequest, srv openfgapb.OpenFGAService_StreamedListObjectsServer) error {
	ctx := srv.Context()
	ctx, span := tracer.Start(ctx, "StreamedListObjects", trace.WithAttributes(
		attribute.String("object_type", req.GetType()),
		attribute.String("relation", req.GetRelation()),
		attribute.String("user", req.GetUser()),
	))
	defer span.End()

	modelID, ok := middleware.ModelIDFromContext(ctx)
	if !ok {
		return fmt.Errorf("no model id")
	}

	model, err := s.datastore.ReadAuthorizationModel(ctx, req.GetStoreId(), modelID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return serverErrors.AuthorizationModelNotFound(req.GetAuthorizationModelId())
		}
		return serverErrors.HandleError("", err)
	}

	typesys := typesystem.New(model)

	connectObjCmd := &commands.ConnectedObjectsCommand{
		Datastore:        s.datastore,
		Typesystem:       typesys,
		ResolveNodeLimit: s.config.ResolveNodeLimit,
		Limit:            s.config.ListObjectsMaxResults,
	}

	q := &commands.ListObjectsQuery{
		Datastore:             s.datastore,
		Logger:                s.logger,
		ListObjectsDeadline:   s.config.ListObjectsDeadline,
		ListObjectsMaxResults: s.config.ListObjectsMaxResults,
		ResolveNodeLimit:      s.config.ResolveNodeLimit,
		ConnectedObjects:      connectObjCmd.StreamedConnectedObjects,
		CheckResolver:         s.checkResolver,
	}

	req.AuthorizationModelId = modelID
	return q.ExecuteStreamed(ctx, req, srv)
}

func (s *Server) Read(ctx context.Context, req *openfgapb.ReadRequest) (*openfgapb.ReadResponse, error) {
	tk := req.GetTupleKey()
	ctx, span := tracer.Start(ctx, "Read", trace.WithAttributes(
		attribute.KeyValue{Key: "object", Value: attribute.StringValue(tk.GetObject())},
		attribute.KeyValue{Key: "relation", Value: attribute.StringValue(tk.GetRelation())},
		attribute.KeyValue{Key: "user", Value: attribute.StringValue(tk.GetUser())},
	))
	defer span.End()

	q := commands.NewReadQuery(s.datastore, s.logger, s.encoder)
	return q.Execute(ctx, &openfgapb.ReadRequest{
		StoreId:           req.GetStoreId(),
		TupleKey:          tk,
		PageSize:          req.GetPageSize(),
		ContinuationToken: req.GetContinuationToken(),
	})
}

func (s *Server) Write(ctx context.Context, req *openfgapb.WriteRequest) (*openfgapb.WriteResponse, error) {
	ctx, span := tracer.Start(ctx, "Write")
	defer span.End()

	modelID, ok := middleware.ModelIDFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("no model id")
	}

	cmd := commands.NewWriteCommand(s.datastore, s.logger)
	return cmd.Execute(ctx, &openfgapb.WriteRequest{
		StoreId:              req.GetStoreId(),
		AuthorizationModelId: modelID,
		Writes:               req.GetWrites(),
		Deletes:              req.GetDeletes(),
	})
}

func (s *Server) Check(ctx context.Context, req *openfgapb.CheckRequest) (*openfgapb.CheckResponse, error) {
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

	modelID, ok := middleware.ModelIDFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("no model id")
	}

	model, err := s.datastore.ReadAuthorizationModel(ctx, storeID, modelID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, serverErrors.AuthorizationModelNotFound(modelID)
		}
		return nil, err
	}

	typesys := typesystem.New(model)

	if err := validation.ValidateUserObjectRelation(typesys, tk); err != nil {
		return nil, serverErrors.ValidationError(err)
	}

	for _, ctxTuple := range req.GetContextualTuples().GetTupleKeys() {
		if err := validation.ValidateTuple(typesys, ctxTuple); err != nil {
			return nil, serverErrors.HandleTupleValidateError(err)
		}
	}

	ctx = typesystem.ContextWithTypesystem(ctx, typesys)
	ctx = storage.ContextWithContextualTuples(ctx, req.ContextualTuples.GetTupleKeys())

	resp, err := s.checkResolver.ResolveCheck(ctx, &graph.ResolveCheckRequest{
		StoreID:              req.GetStoreId(),
		AuthorizationModelID: req.GetAuthorizationModelId(),
		TupleKey:             req.GetTupleKey(),
		ContextualTuples:     req.ContextualTuples.GetTupleKeys(),
		ResolutionMetadata: &graph.ResolutionMetadata{
			Depth: s.config.ResolveNodeLimit,
		},
	})
	if err != nil {
		if errors.Is(err, graph.ErrResolutionDepthExceeded) {
			return nil, serverErrors.AuthorizationModelResolutionTooComplex
		}

		return nil, serverErrors.HandleError("", err)
	}

	res := &openfgapb.CheckResponse{
		Allowed: resp.Allowed,
	}

	span.SetAttributes(attribute.KeyValue{Key: "allowed", Value: attribute.BoolValue(res.GetAllowed())})
	return res, nil
}

func (s *Server) Expand(ctx context.Context, req *openfgapb.ExpandRequest) (*openfgapb.ExpandResponse, error) {
	tk := req.GetTupleKey()
	ctx, span := tracer.Start(ctx, "Expand", trace.WithAttributes(
		attribute.KeyValue{Key: "object", Value: attribute.StringValue(tk.GetObject())},
		attribute.KeyValue{Key: "relation", Value: attribute.StringValue(tk.GetRelation())},
		attribute.KeyValue{Key: "user", Value: attribute.StringValue(tk.GetUser())},
	))
	defer span.End()

	storeID := req.GetStoreId()

	modelID, ok := middleware.ModelIDFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("no model id")
	}

	q := commands.NewExpandQuery(s.datastore, s.logger)
	return q.Execute(ctx, &openfgapb.ExpandRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		TupleKey:             tk,
	})
}

func (s *Server) ReadAuthorizationModel(ctx context.Context, req *openfgapb.ReadAuthorizationModelRequest) (*openfgapb.ReadAuthorizationModelResponse, error) {
	ctx, span := tracer.Start(ctx, "ReadAuthorizationModel", trace.WithAttributes(
		attribute.KeyValue{Key: AuthorizationModelIDTraceTag, Value: attribute.StringValue(req.GetId())},
	))
	defer span.End()

	q := commands.NewReadAuthorizationModelQuery(s.datastore, s.logger)
	return q.Execute(ctx, req)
}

func (s *Server) WriteAuthorizationModel(ctx context.Context, req *openfgapb.WriteAuthorizationModelRequest) (*openfgapb.WriteAuthorizationModelResponse, error) {
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

func (s *Server) ReadAuthorizationModels(ctx context.Context, req *openfgapb.ReadAuthorizationModelsRequest) (*openfgapb.ReadAuthorizationModelsResponse, error) {
	ctx, span := tracer.Start(ctx, "ReadAuthorizationModels")
	defer span.End()

	c := commands.NewReadAuthorizationModelsQuery(s.datastore, s.logger, s.encoder)
	return c.Execute(ctx, req)
}

func (s *Server) WriteAssertions(ctx context.Context, req *openfgapb.WriteAssertionsRequest) (*openfgapb.WriteAssertionsResponse, error) {
	ctx, span := tracer.Start(ctx, "WriteAssertions")
	defer span.End()

	modelID, ok := middleware.ModelIDFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("no model id")
	}

	c := commands.NewWriteAssertionsCommand(s.datastore, s.logger)

	res, err := c.Execute(ctx, &openfgapb.WriteAssertionsRequest{
		StoreId:              req.GetStoreId(),
		AuthorizationModelId: modelID,
		Assertions:           req.GetAssertions(),
	})
	if err != nil {
		return nil, err
	}

	s.transport.SetHeader(ctx, httpmiddleware.XHttpCode, strconv.Itoa(http.StatusNoContent))

	return res, nil
}

func (s *Server) ReadAssertions(ctx context.Context, req *openfgapb.ReadAssertionsRequest) (*openfgapb.ReadAssertionsResponse, error) {
	ctx, span := tracer.Start(ctx, "ReadAssertions")
	defer span.End()

	modelID, ok := middleware.ModelIDFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("no model id")
	}

	q := commands.NewReadAssertionsQuery(s.datastore, s.logger)

	return q.Execute(ctx, req.GetStoreId(), modelID)
}

func (s *Server) ReadChanges(ctx context.Context, req *openfgapb.ReadChangesRequest) (*openfgapb.ReadChangesResponse, error) {
	ctx, span := tracer.Start(ctx, "ReadChangesQuery", trace.WithAttributes(
		attribute.KeyValue{Key: "type", Value: attribute.StringValue(req.GetType())},
	))
	defer span.End()

	q := commands.NewReadChangesQuery(s.datastore, s.logger, s.encoder, s.config.ChangelogHorizonOffset)
	return q.Execute(ctx, req)
}

func (s *Server) CreateStore(ctx context.Context, req *openfgapb.CreateStoreRequest) (*openfgapb.CreateStoreResponse, error) {
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

func (s *Server) DeleteStore(ctx context.Context, req *openfgapb.DeleteStoreRequest) (*openfgapb.DeleteStoreResponse, error) {
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

func (s *Server) GetStore(ctx context.Context, req *openfgapb.GetStoreRequest) (*openfgapb.GetStoreResponse, error) {
	ctx, span := tracer.Start(ctx, "GetStore")
	defer span.End()

	q := commands.NewGetStoreQuery(s.datastore, s.logger)
	return q.Execute(ctx, req)
}

func (s *Server) ListStores(ctx context.Context, req *openfgapb.ListStoresRequest) (*openfgapb.ListStoresResponse, error) {
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
