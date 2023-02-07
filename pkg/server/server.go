package server

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/internal/gateway"
	httpmiddleware "github.com/openfga/openfga/internal/middleware/http"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type ExperimentalFeatureFlag string

const (
	AuthorizationModelIDHeader   = "openfga-authorization-model-id"
	AuthorizationModelIDTraceTag = "authorization_model_id"
)

var tracer = otel.Tracer("openfga/pkg/server")

// A Server implements the OpenFGA service backend as both
// a GRPC and HTTP server.
type Server struct {
	openfgapb.UnimplementedOpenFGAServiceServer

	meter     metric.Meter
	logger    logger.Logger
	datastore storage.OpenFGADatastore
	encoder   encoder.Encoder
	transport gateway.Transport
	config    *Config
}

type Dependencies struct {
	Datastore    storage.OpenFGADatastore
	Meter        metric.Meter
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
	return &Server{
		meter:     dependencies.Meter,
		logger:    dependencies.Logger,
		datastore: dependencies.Datastore,
		encoder:   dependencies.TokenEncoder,
		transport: dependencies.Transport,
		config:    config,
	}
}

func (s *Server) ListObjects(ctx context.Context, req *openfgapb.ListObjectsRequest) (*openfgapb.ListObjectsResponse, error) {
	storeID := req.GetStoreId()
	targetObjectType := req.GetType()

	ctx, span := tracer.Start(ctx, "ListObjects", trace.WithAttributes(
		attribute.KeyValue{Key: "objectType", Value: attribute.StringValue(targetObjectType)},
	))
	defer span.End()

	modelID := req.GetAuthorizationModelId()

	modelID, err := s.resolveAuthorizationModelID(ctx, storeID, modelID)
	if err != nil {
		return nil, err
	}
	model, err := s.datastore.ReadAuthorizationModel(ctx, storeID, modelID)
	if err != nil {
		return nil, serverErrors.AuthorizationModelNotFound(modelID)
	}

	typesys := typesystem.New(model)

	q := &commands.ListObjectsQuery{
		Datastore:             s.datastore,
		Logger:                s.logger,
		Meter:                 s.meter,
		ListObjectsDeadline:   s.config.ListObjectsDeadline,
		ListObjectsMaxResults: s.config.ListObjectsMaxResults,
		ResolveNodeLimit:      s.config.ResolveNodeLimit,
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
		attribute.KeyValue{Key: "objectType", Value: attribute.StringValue(req.GetType())},
	))
	defer span.End()

	storeID := req.GetStoreId()

	modelID, err := s.resolveAuthorizationModelID(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return err
	}

	model, err := s.datastore.ReadAuthorizationModel(ctx, storeID, modelID)
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
		Meter:                 s.meter,
		ListObjectsDeadline:   s.config.ListObjectsDeadline,
		ListObjectsMaxResults: s.config.ListObjectsMaxResults,
		ResolveNodeLimit:      s.config.ResolveNodeLimit,
		ConnectedObjects:      connectObjCmd.StreamedConnectedObjects,
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

	storeID := req.GetStoreId()

	modelID, err := s.resolveAuthorizationModelID(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}

	cmd := commands.NewWriteCommand(s.datastore, s.logger)
	return cmd.Execute(ctx, &openfgapb.WriteRequest{
		StoreId:              storeID,
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

	storeID := req.GetStoreId()

	modelID, err := s.resolveAuthorizationModelID(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}

	q := commands.NewCheckQuery(s.datastore, s.meter, s.logger, s.config.ResolveNodeLimit)

	res, err := q.Execute(ctx, &openfgapb.CheckRequest{
		StoreId:              storeID,
		TupleKey:             tk,
		ContextualTuples:     req.GetContextualTuples(),
		AuthorizationModelId: modelID,
		Trace:                req.GetTrace(),
	})
	if err != nil {
		return nil, err
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

	modelID, err := s.resolveAuthorizationModelID(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
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

	storeID := req.GetStoreId()

	modelID, err := s.resolveAuthorizationModelID(ctx, storeID, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}

	c := commands.NewWriteAssertionsCommand(s.datastore, s.logger)
	res, err := c.Execute(ctx, &openfgapb.WriteAssertionsRequest{
		StoreId:              storeID,
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

	modelID, err := s.resolveAuthorizationModelID(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
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

// resolveAuthorizationModelID takes a modelId. If it is empty, it will find and return the latest authorization model ID.
//
// If is not empty, it will validate it and return it.
//
// This allows caching of types. If the user inserts a new authorization model and doesn't
// provide this field (which should be rate limited more aggressively) the in-flight requests won't be
// affected and newer calls will use the updated authorization model.
func (s *Server) resolveAuthorizationModelID(ctx context.Context, store, modelID string) (string, error) {
	ctx, span := tracer.Start(ctx, "resolveAuthorizationModelID")
	defer span.End()

	var err error
	if modelID != "" {
		if _, err := ulid.Parse(modelID); err != nil {
			return "", serverErrors.AuthorizationModelNotFound(modelID)
		}
	} else {
		if modelID, err = s.datastore.FindLatestAuthorizationModelID(ctx, store); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return "", serverErrors.LatestAuthorizationModelNotFound(store)
			}
			return "", serverErrors.HandleError("", err)
		}
	}

	span.SetAttributes(attribute.KeyValue{Key: AuthorizationModelIDTraceTag, Value: attribute.StringValue(modelID)})
	s.transport.SetHeader(ctx, AuthorizationModelIDHeader, modelID)

	return modelID, nil
}
